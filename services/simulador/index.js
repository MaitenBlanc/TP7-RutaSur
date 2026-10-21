// services/simulador/index.js
require('dotenv').config({ path: '../../.env' });
const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(express.json());

// Configuración de Kafka con variables de entorno
const kafka = new Kafka({
    clientId: 'simulador',
    brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
    retry: {
        initialRetryTime: 1000,
        retries: 10 // Intenta conectarse 10 veces antes de rendirse
    }
});
const producer = kafka.producer();

// Parámetros de aceleración y volumen del negocio
const ACELERACION = parseInt(process.env.SIMULADOR_ACELERACION) || 120; 
const PROBABILIDAD_MALFORMADO = 0.01; // 1% de error forzado

// Utilidad para simular tiempo avanzado
function getOcurridoEn(minutosAgregados = 0) {
    const date = new Date();
    // Acelera el tiempo sumando minutos ficticios
    date.setMinutes(date.getMinutes() + (minutosAgregados * ACELERACION));
    return date.toISOString();
}

// Lógica para emitir eventos
async function publicarEvento(topic, payload, key) {
    // Probabilidad de evento inválido (DLQ Test)
    if (Math.random() < PROBABILIDAD_MALFORMADO) {
        delete payload.shipmentId; // Rompemos el esquema a propósito
    }

    await producer.send({
        topic,
        messages: [{ key, value: JSON.stringify(payload) }]
    });
    console.log(`[Kafka] Publicado en ${topic}:`, payload.tipo || 'Promesa');
}

// Generador de un ciclo de vida completo (Camino Feliz)
async function simularEnvioFeliz() {
    const shipmentId = `SHIP-${Date.now().toString().slice(-6)}`;
    const hubId = ['HUB-NORTE', 'HUB-SUR', 'HUB-ESTE'][Math.floor(Math.random() * 3)];
    const zonaId = `ZONA-${Math.floor(Math.random() * 10) + 1}`;
    
    // Evento de Promesa de Entrega
    await publicarEvento('envios.promesas', {
        eventId: uuidv4(),
        shipmentId,
        ocurridoEn: getOcurridoEn(0),
        venceEn: getOcurridoEn(24 * 60), // Vence en 24h simuladas
        version: 1
    }, shipmentId);

    // Secuencia de estados
    const estados = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO'];
    let secuencia = 1;

    for (const [index, estado] of estados.entries()) {
        const retrasoReal = index * 2000; // 2 segundos reales entre eventos
        setTimeout(async () => {
            const evento = {
                eventId: uuidv4(),
                shipmentId,
                tipo: estado,
                ocurridoEn: getOcurridoEn(index * 60), // Cada estado pasa con horas de diferencia simulada
                secuencia: secuencia++,
                hubId,
                zonaId,
                repartidorId: `REP-${Math.floor(Math.random() * 100)}`,
                version: 1
            };
            await publicarEvento('envios.estados', evento, shipmentId);
        }, retrasoReal);
    }
    return shipmentId;
}

// Generador de un ciclo de vida con entrega fallida
async function simularEnvioAusente() {
    const shipmentId = `SHIP-${Date.now().toString().slice(-6)}`;
    const hubId = ['HUB-NORTE', 'HUB-SUR', 'HUB-ESTE'][Math.floor(Math.random() * 3)];
    const zonaId = `ZONA-${Math.floor(Math.random() * 10) + 1}`;
    
    // Promesa de Entrega
    await publicarEvento('envios.promesas', {
        eventId: uuidv4(),
        shipmentId,
        ocurridoEn: getOcurridoEn(0),
        venceEn: getOcurridoEn(24 * 60),
        version: 1
    }, shipmentId);

    // Secuencia que termina en fallo
    const estados = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGA_FALLIDA'];
    let secuencia = 1;

    for (const [index, estado] of estados.entries()) {
        const retrasoReal = index * 2000;
        setTimeout(async () => {
            const evento = {
                eventId: uuidv4(),
                shipmentId,
                tipo: estado,
                ocurridoEn: getOcurridoEn(index * 60),
                secuencia: secuencia++,
                hubId,
                zonaId,
                repartidorId: `REP-${Math.floor(Math.random() * 100)}`,
                version: 1
            };

            // Regla de negocio: inyectamos el motivo del fallo y el número de intento
            if (estado === 'ENTREGA_FALLIDA') {
                evento.motivo = 'AUSENTE';
                evento.intentos = 1;
            }

            await publicarEvento('envios.estados', evento, shipmentId);
        }, retrasoReal);
    }
    return shipmentId;
}

// API REST para los botones del Panel de Demo en Next.js
app.post('/api/escenarios/feliz', async (req, res) => {
    const id = await simularEnvioFeliz();
    res.json({ message: 'Escenario Feliz disparado', shipmentId: id });
});

app.post('/api/escenarios/ausente', async (req, res) => {
    const id = await simularEnvioAusente();
    res.json({ message: 'Escenario Ausente disparado', shipmentId: id });
});

app.post('/api/escenarios/rafaga', async (req, res) => {
    // Lógica para el bucle de 500 envíos
    res.json({ message: 'Escenario Ráfaga disparado (Mock)' });
});

// Arranque del servidor
async function start() {
    const admin = kafka.admin();
    await admin.connect();
    console.log('[Simulador] Verificando y creando topics de Kafka...');
    await admin.createTopics({
        topics: [
            { topic: 'envios.estados', numPartitions: 3 },
            { topic: 'envios.promesas', numPartitions: 1 },
            { topic: 'envios.reprogramaciones', numPartitions: 1 },
            { topic: 'notificaciones.enviadas', numPartitions: 1 },
            { topic: 'envios.estados.dlq', numPartitions: 1 }
        ]
    });
    await admin.disconnect();

    await producer.connect();
    console.log('[Simulador] Conectado a Kafka como productor');
    app.listen(4000, () => {
        console.log('[Simulador] API escuchando en el puerto 4000 para el Panel de Demo');
    });
}

start().catch(console.error);