require('dotenv').config({ path: '../../.env' });
const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');

const app = express();
app.use(cors());
app.use(express.json());

const kafka = new Kafka({
    clientId: 'simulador',
    brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
    retry: {
        initialRetryTime: 1000,
        retries: 15
    }
});

const producer = kafka.producer();
const admin = kafka.admin();

// GENERADOR PSEUDOALEATORIO CON SEMILLA FIJA (Mulberry32)
let currentSeed = parseInt(process.env.SIMULADOR_SEED) || 2026;
let prngState = currentSeed;

function setSeed(newSeed) {
    currentSeed = newSeed;
    prngState = newSeed;
}

function seededRandom() {
    let t = (prngState += 0x6D2B79F5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}

function deterministicUuid(prefix = 'evt') {
    const r1 = Math.floor(seededRandom() * 0xffffffff).toString(16).padStart(8, '0');
    const r2 = Math.floor(seededRandom() * 0xffffffff).toString(16).padStart(8, '0');
    const r3 = Math.floor(seededRandom() * 0xffffffff).toString(16).padStart(8, '0');
    return `${prefix}-${r1}-${r2}-${r3}`;
}

// PARÁMETROS DEL SIMULADOR
const config = {
    enviosPorMinuto: parseInt(process.env.SIMULADOR_ENVIOS_POR_MINUTO) || 20,
    aceleracion: parseInt(process.env.SIMULADOR_ACELERACION) || 120, // 1h simulada = 30s reales (120x)
    flujoContinuoActivo: process.env.SIMULADOR_FLUJO_CONTINUO !== 'false',
    seed: currentSeed,
    tasasFalloPorZona: {
        'ZONA-1': 0.30, // 30% de fallo (mayor en franja MAÑANA para caso de mejora CU4)
        'ZONA-2': 0.22,
        'ZONA-3': 0.18,
        'ZONA-4': 0.15,
        'ZONA-5': 0.10,
        'ZONA-6': 0.05
    },
    distribucionMotivos: [
        { motivo: 'AUSENTE', peso: 0.60 },
        { motivo: 'DIRECCION_INCORRECTA', peso: 0.20 },
        { motivo: 'RECHAZADO', peso: 0.15 },
        { motivo: 'ZONA_INACCESIBLE', peso: 0.05 }
    ],
    probabilidadDemoraHub: 0.10,
    probabilidadDuplicado: 0.02,
    probabilidadFueraDeOrden: 0.02,
    probabilidadMalformado: 0.01
};

const HUBS = ['HUB-NORTE', 'HUB-SUR', 'HUB-ESTE'];
const ZONAS = Object.keys(config.tasasFalloPorZona);
const FRANJAS = ['MAÑANA', 'TARDE'];

// Reloj simulado global
const realStartMs = Date.now();
const simBaseMs = Date.now();

function getSimTimeDate(minutosSimuladosExtra = 0) {
    const elapsedRealMs = Date.now() - realStartMs;
    const elapsedSimMs = elapsedRealMs * config.aceleracion;
    return new Date(simBaseMs + elapsedSimMs + minutosSimuladosExtra * 60 * 1000);
}

function getOcurridoEn(minutosSimuladosExtra = 0) {
    return getSimTimeDate(minutosSimuladosExtra).toISOString();
}

function elegirMotivoFallo() {
    const r = seededRandom();
    let acumulado = 0;
    for (const item of config.distribucionMotivos) {
        acumulado += item.peso;
        if (r <= acumulado) return item.motivo;
    }
    return 'AUSENTE';
}

let contadorEnvios = 1000;
function proximoShipmentId(prefijo = 'SHIP') {
    contadorEnvios += 1;
    return `${prefijo}-${contadorEnvios}`;
}

// PUBLICACIÓN DE EVENTOS EN KAFKA CON REGLAS DE CAOS OPCIONALES
async function publicarEvento(topic, payload, key, opciones = { aplicarCaos: false }) {
    const copiaPayload = { ...payload };

    // 1% Probabilidad de evento malformado (para probar DLQ)
    if (opciones.aplicarCaos && topic === 'envios.estados' && seededRandom() < config.probabilidadMalformado) {
        const malformado = { ...copiaPayload };
        delete malformado.shipmentId;
        await producer.send({
            topic,
            messages: [{ key: key || 'malformed', value: JSON.stringify(malformado) }]
        });
        console.log(`[Simulador][Caos] Evento malformado inyectado en ${topic}`);
        return;
    }

    await producer.send({
        topic,
        messages: [{ key, value: JSON.stringify(copiaPayload) }]
    });

    // 2% Probabilidad de evento duplicado (mismo eventId)
    if (opciones.aplicarCaos && topic === 'envios.estados' && seededRandom() < config.probabilidadDuplicado) {
        await producer.send({
            topic,
            messages: [{ key, value: JSON.stringify(copiaPayload) }]
        });
        console.log(`[Simulador][Caos] Evento duplicado inyectado: ${copiaPayload.eventId} (${copiaPayload.shipmentId})`);
    }
}

// GENERADOR DE ENVÍO PARAMETRIZADO (Flujo continuo y escenarios)
async function simularCicloEnvio(opciones = {}) {
    const {
        prefijo = 'SHIP',
        modo = 'aleatorio', // 'feliz' | 'ausente' | 'demorado' | 'tercer-fallo' | 'aleatorio'
        aplicarCaos = false,
        delayBaseMs = 1500
    } = opciones;

    const shipmentId = opciones.shipmentId || proximoShipmentId(prefijo);
    const hubId = opciones.hubId || HUBS[Math.floor(seededRandom() * HUBS.length)];
    const zonaId = opciones.zonaId || ZONAS[Math.floor(seededRandom() * ZONAS.length)];
    const repartidorId = opciones.repartidorId || `REP-${Math.floor(seededRandom() * 120) + 1}`;
    // En ZONA-1 forzar mayor proporción en MAÑANA para evidenciar el caso de negocio de CU4
    const franja = opciones.franja || (zonaId === 'ZONA-1' && seededRandom() < 0.75 ? 'MAÑANA' : FRANJAS[Math.floor(seededRandom() * FRANJAS.length)]);

    // Determinar si hay demora en hub (10% en modo aleatorio, 100% en modo demorado)
    const esDemorado = modo === 'demorado' || (modo === 'aleatorio' && seededRandom() < config.probabilidadDemoraHub);

    // Determinar horas hasta el vencimiento de la promesa
    // Si es demorado, vence en 3 horas simuladas para disparar alerta "Sin salida" (< 4h)
    const horasPromesa = modo === 'demorado' ? 3 : 24;
    const venceEn = getOcurridoEn(horasPromesa * 60);

    // Publicar Promesa de Entrega en envios.promesas
    await publicarEvento('envios.promesas', {
        eventId: deterministicUuid('prom'),
        shipmentId,
        ocurridoEn: getOcurridoEn(0),
        venceEn,
        hubId,
        zonaId,
        franja,
        version: 1
    }, shipmentId, { aplicarCaos: false });

    let secuencia = 1;
    const pasos = [];

    if (modo === 'demorado') {
        // Queda detenido en el hub para que el Monitor de SLA dispare la alerta
        pasos.push({ tipo: 'CREADO', minSimulados: 0 });
        pasos.push({ tipo: 'INGRESADO_HUB', minSimulados: 30 });
    } else if (modo === 'feliz') {
        pasos.push({ tipo: 'CREADO', minSimulados: 0 });
        pasos.push({ tipo: 'INGRESADO_HUB', minSimulados: 60 });
        pasos.push({ tipo: 'CLASIFICADO', minSimulados: 120 });
        pasos.push({ tipo: 'EN_REPARTO', minSimulados: 180 });
        pasos.push({ tipo: 'ENTREGADO', minSimulados: 300, intentos: 1 });
    } else if (modo === 'ausente') {
        // Intento 1 falla por AUSENTE -> Reprogramación emite REPROGRAMADO -> Vuelve al circuito y se entrega en intento 2
        pasos.push({ tipo: 'CREADO', minSimulados: 0 });
        pasos.push({ tipo: 'INGRESADO_HUB', minSimulados: 60 });
        pasos.push({ tipo: 'CLASIFICADO', minSimulados: 120 });
        pasos.push({ tipo: 'EN_REPARTO', minSimulados: 180 });
        pasos.push({ tipo: 'ENTREGA_FALLIDA', minSimulados: 240, motivo: 'AUSENTE', intentos: 1 });
        // Nuevo intento al día siguiente en franja opuesta
        const franjaOpuesta = franja === 'MAÑANA' ? 'TARDE' : 'MAÑANA';
        pasos.push({ tipo: 'CLASIFICADO', minSimulados: 1440, franjaOverride: franjaOpuesta, intentos: 2, extraDelayMs: 2500 });
        pasos.push({ tipo: 'EN_REPARTO', minSimulados: 1500, franjaOverride: franjaOpuesta, intentos: 2, extraDelayMs: 2500 });
        pasos.push({ tipo: 'ENTREGADO', minSimulados: 1620, franjaOverride: franjaOpuesta, intentos: 2, extraDelayMs: 2500 });
    } else if (modo === 'tercer-fallo') {
        // Agota 3 intentos y pasa a DEVOLUCION_COMERCIO
        pasos.push({ tipo: 'CREADO', minSimulados: 0 });
        pasos.push({ tipo: 'INGRESADO_HUB', minSimulados: 60 });
        pasos.push({ tipo: 'CLASIFICADO', minSimulados: 120 });
        pasos.push({ tipo: 'EN_REPARTO', minSimulados: 180 });
        pasos.push({ tipo: 'ENTREGA_FALLIDA', minSimulados: 240, motivo: 'AUSENTE', intentos: 1 });
        pasos.push({ tipo: 'CLASIFICADO', minSimulados: 1440, intentos: 2 });
        pasos.push({ tipo: 'EN_REPARTO', minSimulados: 1500, intentos: 2 });
        pasos.push({ tipo: 'ENTREGA_FALLIDA', minSimulados: 1560, motivo: 'AUSENTE', intentos: 2 });
        pasos.push({ tipo: 'CLASIFICADO', minSimulados: 2880, intentos: 3 });
        pasos.push({ tipo: 'EN_REPARTO', minSimulados: 2940, intentos: 3 });
        pasos.push({ tipo: 'ENTREGA_FALLIDA', minSimulados: 3000, motivo: 'AUSENTE', intentos: 3 });
    } else {
        // Modo aleatorio (Flujo continuo)
        const tasaBase = config.tasasFalloPorZona[zonaId] || 0.15;
        // En ZONA-1 por la mañana la tasa de fallo es mayor (30%), por la tarde baja (8%)
        const tasaEfectiva = (zonaId === 'ZONA-1' && franja === 'TARDE') ? 0.08 : tasaBase;
        const fallaIntento1 = seededRandom() < tasaEfectiva;
        const demoraHubMin = esDemorado ? 480 : Math.floor(seededRandom() * 90) + 30;
        const salidaRepartoMin = demoraHubMin + (hubId === 'HUB-SUR' ? 240 : 90);

        pasos.push({ tipo: 'CREADO', minSimulados: 0 });
        pasos.push({ tipo: 'INGRESADO_HUB', minSimulados: 45 });
        pasos.push({ tipo: 'CLASIFICADO', minSimulados: 90 });
        pasos.push({ tipo: 'EN_REPARTO', minSimulados: salidaRepartoMin });

        if (!fallaIntento1) {
            // Si es de HUB-SUR y esDemorado, entregar después de las 24h para reflejar incumplimiento de SLA en ese Hub
            const entregaMin = (hubId === 'HUB-SUR' && esDemorado) ? 1600 : salidaRepartoMin + 120;
            pasos.push({ tipo: 'ENTREGADO', minSimulados: entregaMin, intentos: 1 });
        } else {
            const motivo = elegirMotivoFallo();
            pasos.push({ tipo: 'ENTREGA_FALLIDA', minSimulados: salidaRepartoMin + 120, motivo, intentos: 1 });
            if (motivo === 'AUSENTE') {
                const franjaOpuesta = franja === 'MAÑANA' ? 'TARDE' : 'MAÑANA';
                pasos.push({ tipo: 'CLASIFICADO', minSimulados: 1440, franjaOverride: franjaOpuesta, intentos: 2, extraDelayMs: 1500 });
                pasos.push({ tipo: 'EN_REPARTO', minSimulados: 1500, franjaOverride: franjaOpuesta, intentos: 2, extraDelayMs: 1500 });
                pasos.push({ tipo: 'ENTREGADO', minSimulados: 1620, franjaOverride: franjaOpuesta, intentos: 2, extraDelayMs: 1500 });
            }
        }
    }

    pasos.forEach((paso, index) => {
        const seqActual = secuencia++;
        const delayMs = index * delayBaseMs + (paso.extraDelayMs || 0);

        setTimeout(async () => {
            try {
                const evento = {
                    eventId: deterministicUuid('evt'),
                    shipmentId,
                    tipo: paso.tipo,
                    ocurridoEn: getOcurridoEn(paso.minSimulados),
                    venceEn,
                    secuencia: seqActual,
                    hubId,
                    zonaId,
                    franja: paso.franjaOverride || franja,
                    repartidorId,
                    intentos: paso.intentos || 1,
                    version: 1
                };
                if (paso.motivo) {
                    evento.motivo = paso.motivo;
                }

                await publicarEvento('envios.estados', evento, shipmentId, { aplicarCaos });

                // 2% Probabilidad de evento fuera de orden (envía un evento con secuencia menor)
                if (aplicarCaos && seqActual >= 3 && seededRandom() < config.probabilidadFueraDeOrden) {
                    const eventoViejo = {
                        ...evento,
                        eventId: deterministicUuid('ooo'),
                        tipo: 'CREADO',
                        secuencia: 1
                    };
                    await publicarEvento('envios.estados', eventoViejo, shipmentId, { aplicarCaos: false });
                    console.log(`[Simulador][Caos] Evento fuera de orden (secuencia 1 después de ${seqActual}) enviado para ${shipmentId}`);
                }
            } catch (err) {
                console.error(`[Simulador] Error emitiendo paso para ${shipmentId}:`, err.message);
            }
        }, delayMs);
    });

    return { shipmentId, hubId, zonaId, franja, venceEn };
}

// FLUJO CONTINUO (20 envíos por minuto por defecto)
let intervaloFlujo = null;

function iniciarFlujoContinuo() {
    if (intervaloFlujo) clearInterval(intervaloFlujo);
    if (!config.flujoContinuoActivo) return;

    const intervaloMs = Math.max(500, Math.floor((60 * 1000) / config.enviosPorMinuto));
    intervaloFlujo = setInterval(() => {
        if (config.flujoContinuoActivo) {
            simularCicloEnvio({
                prefijo: 'SHIP',
                modo: 'aleatorio',
                aplicarCaos: true,
                delayBaseMs: 800
            }).catch(console.error);
        }
    }, intervaloMs);
    console.log(`[Simulador] Flujo continuo activo (${config.enviosPorMinuto} envíos/min, cada ${intervaloMs}ms)`);
}

// ENDPOINTS REST PARA EL PANEL DE DEMO Y OBSERVABILIDAD
// Escenario Feliz
app.post('/api/escenarios/feliz', async (req, res) => {
    const info = await simularCicloEnvio({ prefijo: 'SHIP-FELIZ', modo: 'feliz', aplicarCaos: false, delayBaseMs: 1200 });
    res.json({ message: 'Escenario Feliz disparado', ...info });
});

// Escenario Ausente (Falla 1er intento -> Notificación + Reprogramación + Analítica -> 2do intento entregado)
app.post('/api/escenarios/ausente', async (req, res) => {
    const info = await simularCicloEnvio({ prefijo: 'SHIP-AUSENTE', modo: 'ausente', aplicarCaos: false, delayBaseMs: 1200 });
    res.json({ message: 'Escenario Ausente disparado (con reprogramación y reintento)', ...info });
});

// Escenario Demorado (Detenido en hub -> Alerta SLA prioridad Alta)
app.post('/api/escenarios/demorado', async (req, res) => {
    const info = await simularCicloEnvio({ prefijo: 'SHIP-DEMORA', modo: 'demorado', aplicarCaos: false, delayBaseMs: 800 });
    res.json({ message: 'Escenario Demorado disparado (alerta SLA Sin salida en instantes)', ...info });
});

// Escenario Tercer Fallo (Agota 3 intentos -> Devolución al comercio)
app.post('/api/escenarios/tercer-fallo', async (req, res) => {
    const info = await simularCicloEnvio({ prefijo: 'SHIP-3FALLOS', modo: 'tercer-fallo', aplicarCaos: false, delayBaseMs: 900 });
    res.json({ message: 'Escenario Tercer Fallo disparado (pasa a DEVOLUCION_COMERCIO)', ...info });
});

// Escenario Ráfaga (500 envíos en pocos segundos)
app.post('/api/escenarios/rafaga', async (req, res) => {
    const cantidad = parseInt(req.body?.cantidad) || 500;
    res.json({ message: `Ráfaga de ${cantidad} envíos iniciada en segundo plano`, cantidad });

    // Emitir los 500 envíos en lotes rápidos para no bloquear el event loop HTTP
    setImmediate(async () => {
        console.log(`[Simulador] Iniciando ráfaga de ${cantidad} envíos...`);
        for (let i = 0; i < cantidad; i++) {
            const shipmentId = proximoShipmentId('SHIP-RAF');
            const hubId = HUBS[i % HUBS.length];
            const zonaId = ZONAS[i % ZONAS.length];
            const franja = FRANJAS[i % FRANJAS.length];
            const venceEn = getOcurridoEn(24 * 60);
            const ocurridoCreacion = getOcurridoEn(0);
            const ocurridoHub = getOcurridoEn(30);
            const ocurridoReparto = getOcurridoEn(90 + (i % 3) * 30);
            const ocurridoFinal = getOcurridoEn(180);
            const esExito = (i % 5) !== 0; // 80% éxito, 20% fallo

            try {
                await producer.send({
                    topic: 'envios.promesas',
                    messages: [{
                        key: shipmentId,
                        value: JSON.stringify({
                            eventId: deterministicUuid('prom'),
                            shipmentId,
                            ocurridoEn: ocurridoCreacion,
                            venceEn,
                            hubId,
                            zonaId,
                            franja,
                            version: 1
                        })
                    }]
                });

                const eventosBatch = [
                    {
                        eventId: deterministicUuid('evt'),
                        shipmentId,
                        tipo: 'CREADO',
                        ocurridoEn: ocurridoCreacion,
                        venceEn,
                        secuencia: 1,
                        hubId,
                        zonaId,
                        franja,
                        repartidorId: `REP-${(i % 50) + 1}`,
                        intentos: 1,
                        version: 1
                    },
                    {
                        eventId: deterministicUuid('evt'),
                        shipmentId,
                        tipo: 'INGRESADO_HUB',
                        ocurridoEn: ocurridoHub,
                        venceEn,
                        secuencia: 2,
                        hubId,
                        zonaId,
                        franja,
                        repartidorId: `REP-${(i % 50) + 1}`,
                        intentos: 1,
                        version: 1
                    },
                    {
                        eventId: deterministicUuid('evt'),
                        shipmentId,
                        tipo: 'EN_REPARTO',
                        ocurridoEn: ocurridoReparto,
                        venceEn,
                        secuencia: 3,
                        hubId,
                        zonaId,
                        franja,
                        repartidorId: `REP-${(i % 50) + 1}`,
                        intentos: 1,
                        version: 1
                    },
                    {
                        eventId: deterministicUuid('evt'),
                        shipmentId,
                        tipo: esExito ? 'ENTREGADO' : 'ENTREGA_FALLIDA',
                        motivo: esExito ? undefined : 'AUSENTE',
                        ocurridoEn: ocurridoFinal,
                        venceEn,
                        secuencia: 4,
                        hubId,
                        zonaId,
                        franja,
                        repartidorId: `REP-${(i % 50) + 1}`,
                        intentos: 1,
                        version: 1
                    }
                ];

                await producer.send({
                    topic: 'envios.estados',
                    messages: eventosBatch.map((ev) => ({ key: shipmentId, value: JSON.stringify(ev) }))
                });
            } catch (e) {
                console.error('[Simulador] Error en ráfaga:', e.message);
            }
        }
        console.log(`[Simulador] Ráfaga de ${cantidad} envíos completada.`);
    });
});

// Escenario Evento Malformado (va a la DLQ y el flujo continúa)
app.post('/api/escenarios/malformado', async (req, res) => {
    const payloadRoto = {
        eventId: deterministicUuid('bad'),
        // Sin shipmentId a propósito para que falle la validación de esquema
        tipo: 'CREADO',
        ocurridoEn: getOcurridoEn(0),
        detalle: 'Evento inyectado sin shipmentId para validar envios.estados.dlq'
    };
    await producer.send({
        topic: 'envios.estados',
        messages: [{ key: 'SIN-CLAVE', value: JSON.stringify(payloadRoto) }]
    });
    res.json({ message: 'Evento malformado enviado a envios.estados (verificar en envios.estados.dlq)', eventId: payloadRoto.eventId });
});

// Escenario Duplicado (Prueba de idempotencia sobre un envío)
app.post('/api/escenarios/duplicado', async (req, res) => {
    const shipmentId = req.body?.shipmentId || proximoShipmentId('SHIP-DUP');
    const eventId = deterministicUuid('dup');
    const evento = {
        eventId,
        shipmentId,
        tipo: 'CREADO',
        ocurridoEn: getOcurridoEn(0),
        venceEn: getOcurridoEn(24 * 60),
        secuencia: 1,
        hubId: 'HUB-NORTE',
        zonaId: 'ZONA-1',
        franja: 'MAÑANA',
        repartidorId: 'REP-10',
        version: 1
    };
    // Enviar dos veces exactamente el mismo evento con igual eventId
    await producer.send({
        topic: 'envios.estados',
        messages: [
            { key: shipmentId, value: JSON.stringify(evento) },
            { key: shipmentId, value: JSON.stringify(evento) }
        ]
    });
    res.json({
        message: `Evento enviado 2 veces con el mismo eventId (${eventId}) para ${shipmentId}. El historial solo debe registrarlo una vez.`,
        shipmentId,
        eventId
    });
});

// Escenario Fuera de Orden (Envía secuencia 3 y luego secuencia 1)
app.post('/api/escenarios/fuera-de-orden', async (req, res) => {
    const shipmentId = proximoShipmentId('SHIP-ORDEN');
    const evAvanzado = {
        eventId: deterministicUuid('ord'),
        shipmentId,
        tipo: 'EN_REPARTO',
        ocurridoEn: getOcurridoEn(120),
        venceEn: getOcurridoEn(24 * 60),
        secuencia: 3,
        hubId: 'HUB-NORTE',
        zonaId: 'ZONA-2',
        franja: 'MAÑANA',
        repartidorId: 'REP-20',
        version: 1
    };
    const evAtrasado = {
        eventId: deterministicUuid('ord'),
        shipmentId,
        tipo: 'CREADO',
        ocurridoEn: getOcurridoEn(0),
        venceEn: getOcurridoEn(24 * 60),
        secuencia: 1,
        hubId: 'HUB-NORTE',
        zonaId: 'ZONA-2',
        franja: 'MAÑANA',
        repartidorId: 'REP-20',
        version: 1
    };
    await producer.send({
        topic: 'envios.estados',
        messages: [
            { key: shipmentId, value: JSON.stringify(evAvanzado) },
            { key: shipmentId, value: JSON.stringify(evAtrasado) }
        ]
    });
    res.json({
        message: `Enviados EN_REPARTO (secuencia 3) y luego CREADO (secuencia 1) para ${shipmentId}. El envío debe mantenerse en EN_REPARTO.`,
        shipmentId
    });
});

// Corrida determinística con semilla fija para verificación de Analítica (Criterio CU4)
app.post('/api/escenarios/corrida-semilla', async (req, res) => {
    const seed = parseInt(req.body?.seed) || 2026;
    const cantidad = parseInt(req.body?.cantidad) || 15;
    setSeed(seed);

    const recuentoManual = {
        seed,
        cantidadEnvios: cantidad,
        primerIntentoExitosos: 0,
        primerIntentoFallidos: 0,
        motivosFallo: {
            AUSENTE: 0,
            DIRECCION_INCORRECTA: 0,
            RECHAZADO: 0,
            ZONA_INACCESIBLE: 0
        },
        slaPorHub: {}
    };

    for (let i = 0; i < cantidad; i++) {
        const shipmentId = `SEED-${seed}-${i + 1}`;
        const hubId = HUBS[Math.floor(seededRandom() * HUBS.length)];
        const zonaId = ZONAS[Math.floor(seededRandom() * ZONAS.length)];
        const franja = (zonaId === 'ZONA-1' && seededRandom() < 0.75) ? 'MAÑANA' : FRANJAS[Math.floor(seededRandom() * FRANJAS.length)];
        const tasa = config.tasasFalloPorZona[zonaId] || 0.15;
        const falla = seededRandom() < tasa;
        const llegaTarde = !falla && (hubId === 'HUB-SUR' && seededRandom() < 0.35);

        const tCreacion = getOcurridoEn(0);
        const tHub = getOcurridoEn(30);
        const tReparto = getOcurridoEn(hubId === 'HUB-SUR' ? 180 : 90);
        const tVence = getOcurridoEn(24 * 60);
        const tFinal = llegaTarde ? getOcurridoEn(26 * 60) : getOcurridoEn(12 * 60);

        await producer.send({
            topic: 'envios.promesas',
            messages: [{
                key: shipmentId,
                value: JSON.stringify({
                    eventId: deterministicUuid(`seed-${seed}-prom`),
                    shipmentId,
                    ocurridoEn: tCreacion,
                    venceEn: tVence,
                    hubId,
                    zonaId,
                    franja,
                    version: 1
                })
            }]
        });

        const motivo = falla ? elegirMotivoFallo() : undefined;

        if (!recuentoManual.slaPorHub[hubId]) {
            recuentoManual.slaPorHub[hubId] = { enSla: 0, fueraSla: 0 };
        }

        if (!falla) {
            recuentoManual.primerIntentoExitosos += 1;
            if (llegaTarde) {
                recuentoManual.slaPorHub[hubId].fueraSla += 1;
            } else {
                recuentoManual.slaPorHub[hubId].enSla += 1;
            }
        } else {
            recuentoManual.primerIntentoFallidos += 1;
            recuentoManual.motivosFallo[motivo] += 1;
            recuentoManual.slaPorHub[hubId].fueraSla += 1;
        }

        const eventos = [
            {
                eventId: deterministicUuid(`seed-${seed}-ev1`),
                shipmentId,
                tipo: 'CREADO',
                ocurridoEn: tCreacion,
                venceEn: tVence,
                secuencia: 1,
                hubId,
                zonaId,
                franja,
                repartidorId: 'REP-SEED',
                intentos: 1,
                version: 1
            },
            {
                eventId: deterministicUuid(`seed-${seed}-ev2`),
                shipmentId,
                tipo: 'INGRESADO_HUB',
                ocurridoEn: tHub,
                venceEn: tVence,
                secuencia: 2,
                hubId,
                zonaId,
                franja,
                repartidorId: 'REP-SEED',
                intentos: 1,
                version: 1
            },
            {
                eventId: deterministicUuid(`seed-${seed}-ev3`),
                shipmentId,
                tipo: 'EN_REPARTO',
                ocurridoEn: tReparto,
                venceEn: tVence,
                secuencia: 3,
                hubId,
                zonaId,
                franja,
                repartidorId: 'REP-SEED',
                intentos: 1,
                version: 1
            },
            {
                eventId: deterministicUuid(`seed-${seed}-ev4`),
                shipmentId,
                tipo: falla ? 'ENTREGA_FALLIDA' : 'ENTREGADO',
                motivo,
                ocurridoEn: tFinal,
                venceEn: tVence,
                secuencia: 4,
                hubId,
                zonaId,
                franja,
                repartidorId: 'REP-SEED',
                intentos: 1,
                version: 1
            }
        ];

        await producer.send({
            topic: 'envios.estados',
            messages: eventos.map((e) => ({ key: shipmentId, value: JSON.stringify(e) }))
        });
    }

    res.json({
        message: `Corrida con semilla fija (${seed}) ejecutada con ${cantidad} envíos determinísticos.`,
        recuentoManual
    });
});

// Control del flujo continuo y consulta de parámetros + lag de grupos (Observabilidad)
app.get('/api/estado', async (req, res) => {
    let lagPorGrupo = {};
    try {
        const topicOffsets = await admin.fetchTopicOffsets('envios.estados');
        const grupos = ['proyector', 'notificaciones', 'reprogramacion', 'monitor-sla', 'analitica'];
        for (const groupId of grupos) {
            try {
                const groupOffsets = await admin.fetchOffsets({ groupId, topics: ['envios.estados'] });
                const topicData = groupOffsets.find((g) => g.topic === 'envios.estados');
                let totalLag = 0;
                if (topicData && topicData.partitions) {
                    for (const p of topicData.partitions) {
                        const high = topicOffsets.find((t) => t.partition === p.partition);
                        const highWatermark = high ? parseInt(high.high || high.offset || 0) : 0;
                        const currentOffset = p.offset && parseInt(p.offset) >= 0 ? parseInt(p.offset) : 0;
                        totalLag += Math.max(0, highWatermark - currentOffset);
                    }
                }
                lagPorGrupo[groupId] = totalLag;
            } catch {
                lagPorGrupo[groupId] = 0;
            }
        }
    } catch (e) {
        console.warn('[Simulador] No se pudo calcular el lag:', e.message);
    }

    res.json({
        config,
        horaSimuladaActual: getOcurridoEn(0),
        lagPorGrupo
    });
});

app.post('/api/flujo', (req, res) => {
    if (typeof req.body?.activo === 'boolean') {
        config.flujoContinuoActivo = req.body.activo;
    }
    if (req.body?.enviosPorMinuto) {
        config.enviosPorMinuto = Math.max(1, Math.min(200, parseInt(req.body.enviosPorMinuto)));
    }
    iniciarFlujoContinuo();
    res.json({
        message: `Flujo continuo ${config.flujoContinuoActivo ? 'activado' : 'pausado'}`,
        config
    });
});

// ARRANQUE DEL SERVIDOR Y CREACIÓN DEL CATÁLOGO DE TOPICS
async function start() {
    await admin.connect();
    console.log('[Simulador] Verificando y creando los 6 topics de Kafka con sus políticas de retención...');

    const MS_7_DIAS = String(7 * 24 * 60 * 60 * 1000);
    const MS_30_DIAS = String(30 * 24 * 60 * 60 * 1000);

    await admin.createTopics({
        topics: [
            {
                topic: 'envios.estados',
                numPartitions: 3,
                configEntries: [{ name: 'retention.ms', value: MS_30_DIAS }]
            },
            {
                topic: 'envios.promesas',
                numPartitions: 1,
                configEntries: [{ name: 'cleanup.policy', value: 'compact' }]
            },
            {
                topic: 'envios.reprogramaciones',
                numPartitions: 1,
                configEntries: [{ name: 'retention.ms', value: MS_30_DIAS }]
            },
            {
                topic: 'alertas.sla',
                numPartitions: 1,
                configEntries: [{ name: 'retention.ms', value: MS_7_DIAS }]
            },
            {
                topic: 'notificaciones.enviadas',
                numPartitions: 1,
                configEntries: [{ name: 'retention.ms', value: MS_7_DIAS }]
            },
            {
                topic: 'envios.estados.dlq',
                numPartitions: 1,
                configEntries: [{ name: 'retention.ms', value: MS_30_DIAS }]
            }
        ]
    });

    await producer.connect();
    console.log('[Simulador] Conectado a Kafka como productor');

    iniciarFlujoContinuo();

    const port = parseInt(process.env.PORT_SIMULADOR) || 4000;
    app.listen(port, () => {
        console.log(`[Simulador] API escuchando en el puerto ${port}`);
    });
}

start().catch(console.error);
