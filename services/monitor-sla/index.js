require('dotenv').config({ path: '../../.env' });
const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');
const cron = require('node-cron');
const { v4: uuidv4 } = require('uuid');
const { Vencimiento } = require('./models');

const kafka = new Kafka({
    clientId: 'monitor-sla',
    brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
    retry: { initialRetryTime: 1000, retries: 10 }
});

const consumer = kafka.consumer({ groupId: 'monitor-sla' });
const producer = kafka.producer();

// Función auxiliar para entender el tiempo acelerado del simulador
function getTiempoSimulado() {
    return new Date();
}

async function procesarEvento(message, topic) {
    const evento = JSON.parse(message.value.toString());
    const { shipmentId, tipo, venceEn } = evento;

    let doc = await Vencimiento.findOne({ shipmentId });

    if (topic === 'envios.promesas') {
        // Guardar el vencimiento inicial
        if (!doc) {
            await Vencimiento.create({ shipmentId, ultimoEstado: 'CREADO', venceEn: new Date(venceEn) });
            console.log(`[Monitor SLA] Promesa registrada para ${shipmentId}`);
        }
    } else if (topic === 'envios.estados') {
        if (doc) {
            doc.ultimoEstado = tipo;
            await doc.save();

            // Si se entregó, cerrar y borrar del monitor para no seguir evaluando
            if (tipo === 'ENTREGADO') {
                await Vencimiento.deleteOne({ shipmentId });
                // Emitir cierre de alerta
                await producer.send({
                    topic: 'alertas.sla',
                    messages: [{ key: shipmentId, value: JSON.stringify({ shipmentId, tipo: 'ALERTA_CERRADA' }) }]
                });
                console.log(`[Monitor SLA] Cierre de alertas para ${shipmentId} (Entregado)`);
            }
        }
    }
}

async function evaluarRiesgosCron() {
    // Evalúa cada minuto la tabla temporal
    const enviosAbiertos = await Vencimiento.find();
    const ahora = getTiempoSimulado();

    for (const envio of enviosAbiertos) {
        const horasRestantes = (new Date(envio.venceEn) - ahora) / (1000 * 60 * 60);
        let nuevaAlerta = null;

        // Reglas de Alerta
        if (horasRestantes <= 0 && !envio.alertasEmitidas.includes('Vencido')) {
            nuevaAlerta = { regla: 'Vencido', prioridad: 'Crítica' };
        } else if (horasRestantes < 4 && envio.ultimoEstado !== 'EN_REPARTO' && !envio.alertasEmitidas.includes('Sin salida')) {
            nuevaAlerta = { regla: 'Sin salida', prioridad: 'Alta' };
        } else if (horasRestantes < 10 && envio.ultimoEstado === 'CREADO' && !envio.alertasEmitidas.includes('Sin ingreso')) {
            nuevaAlerta = { regla: 'Sin ingreso', prioridad: 'Media' };
        }

        if (nuevaAlerta) {
            envio.alertasEmitidas.push(nuevaAlerta.regla);
            await envio.save();

            const alertaMsg = {
                eventId: uuidv4(),
                shipmentId: envio.shipmentId,
                tipo: 'ALERTA_GENERADA',
                causa: nuevaAlerta.regla,
                prioridad: nuevaAlerta.prioridad,
                ocurridoEn: new Date().toISOString()
            };

            await producer.send({
                topic: 'alertas.sla',
                messages: [{ key: envio.shipmentId, value: JSON.stringify(alertaMsg) }]
            });
            console.log(`[Monitor SLA] ALERTA EMITIDA: ${envio.shipmentId} - ${nuevaAlerta.regla} (${nuevaAlerta.prioridad})`);
        }
    }
}

async function start() {
    await mongoose.connect(process.env.MONGO_URI);
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });
    await consumer.subscribe({ topic: 'envios.promesas', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            await procesarEvento(message, topic);
        },
    });
    
    // Ejecuta la evaluación de tiempo cada minuto real
    cron.schedule('* * * * *', evaluarRiesgosCron);

    console.log('[Monitor SLA] Evaluando tiempos y promesas...');
}

start().catch(console.error);