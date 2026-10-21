require('dotenv').config({ path: '../../.env' });
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const kafka = new Kafka({
    clientId: 'notificaciones',
    brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
    retry: {
        initialRetryTime: 1000,
        retries: 10 // Intenta conectarse 10 veces antes de rendirse
    }
});

const consumer = kafka.consumer({ groupId: 'notificaciones' });
const producer = kafka.producer();

async function start() {
    await producer.connect();
    await consumer.connect();
    // Escucha fallos (CU2) y alertas (CU3)
    await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const evento = JSON.parse(message.value.toString());
            
            if (evento.tipo === 'ENTREGA_FALLIDA') {
                const aviso = {
                    eventId: uuidv4(),
                    shipmentId: evento.shipmentId,
                    tipoAviso: 'FALLO_ENTREGA',
                    mensaje: `No pudimos entregar tu paquete. Motivo: ${evento.motivo || 'No especificado'}.`,
                    ocurridoEn: new Date().toISOString()
                };

                // Deja un registro auditable del aviso emitido
                await producer.send({
                    topic: 'notificaciones.enviadas',
                    messages: [{ key: evento.shipmentId, value: JSON.stringify(aviso) }]
                });

                console.log(`[Notificaciones] Aviso enviado a destinatario del envío: ${evento.shipmentId}`);
            }
        },
    });
    console.log('[Notificaciones] Escuchando eventos para notificar...');
}

start().catch(console.error);