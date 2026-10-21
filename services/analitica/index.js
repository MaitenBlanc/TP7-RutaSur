require('dotenv').config({ path: '../../.env' });
const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');
const { MetricaDiaria } = require('./models');

const kafka = new Kafka({
    clientId: 'analitica',
    brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
    retry: { initialRetryTime: 1000, retries: 10 }
});

const consumer = kafka.consumer({ groupId: 'analitica' });

async function procesarEvento(message) {
    const evento = JSON.parse(message.value.toString());
    const { tipo, ocurridoEn, zonaId, hubId, motivo } = evento;

    // Solo interesan los finales para la métrica de éxito/fallo
    if (tipo !== 'ENTREGADO' && tipo !== 'ENTREGA_FALLIDA') return;

    // Extraer la fecha (YYYY-MM-DD)
    const fecha = new Date(ocurridoEn).toISOString().split('T')[0];

    // Preparar el incremento atómico en base al tipo de evento
    const incQuery = {};
    if (tipo === 'ENTREGADO') {
        incQuery['entregasExitosas'] = 1;
    } else if (tipo === 'ENTREGA_FALLIDA') {
        incQuery['entregasFallidas'] = 1;
        if (motivo) {
            incQuery[`motivosFallo.${motivo}`] = 1; // Incrementa el motivo específico
        }
    }

    // Actualización atómica (Upsert: si no existe el día/zona, lo crea)
    await MetricaDiaria.findOneAndUpdate(
        { fecha, zonaId: zonaId || 'ZONA-DESC', hubId: hubId || 'HUB-DESC' },
        { $inc: incQuery },
        { upsert: true, returnDocument: 'after' }
    );

    console.log(`[Analítica] Métricas actualizadas para ${zonaId} el día ${fecha}`);
}

async function start() {
    await mongoose.connect(process.env.MONGO_URI);
    await consumer.connect();
    
    // Lee desde el inicio para procesar el histórico automáticamente
    await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                await procesarEvento(message);
            } catch (error) {
                console.error('[Analítica] Error procesando evento:', error.message);
            }
        },
    });

    console.log('[Analítica] Escuchando eventos para agregar métricas...');
}

start().catch(console.error);