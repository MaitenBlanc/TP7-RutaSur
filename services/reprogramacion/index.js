require('dotenv').config({ path: '../../.env' });
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const kafka = new Kafka({
    clientId: 'reprogramacion',
    brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
    retry: {
        initialRetryTime: 1000,
        retries: 10 // Intenta conectarse 10 veces antes de rendirse
    }
});
const consumer = kafka.consumer({ groupId: 'reprogramacion' });
const producer = kafka.producer();

async function procesarFallo(evento) {
    const { shipmentId, motivo, intentos = 1 } = evento;
    let nuevoEstado = '';
    
    // Reglas de negocio para reprogramación
    if (intentos >= 3 || motivo === 'RECHAZADO') {
        nuevoEstado = 'DEVOLUCION_COMERCIO';
    } else if (motivo === 'DIRECCION_INCORRECTA') {
        nuevoEstado = 'RETENIDO';
    } else if (motivo === 'AUSENTE') {
        nuevoEstado = 'REPROGRAMADO';
    } else {
        nuevoEstado = 'REPROGRAMADO'; // Fallback por defecto
    }

    const eventoReprogramacion = {
        eventId: uuidv4(),
        shipmentId,
        tipo: nuevoEstado,
        motivoOriginal: motivo,
        ocurridoEn: new Date().toISOString(),
        version: 1
    };

    // Publica la decisión en su propio topic
    await producer.send({
        topic: 'envios.reprogramaciones',
        messages: [{ key: shipmentId, value: JSON.stringify(eventoReprogramacion) }]
    });

    console.log(`[Reprogramación] Decisión tomada para ${shipmentId}: ${nuevoEstado}`);
}

async function start() {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const evento = JSON.parse(message.value.toString());
            // Solo interesa reaccionar si el envío falló
            if (evento.tipo === 'ENTREGA_FALLIDA') {
                await procesarFallo(evento);
            }
        },
    });
    console.log('[Reprogramación] Escuchando fallos de entrega...');
}

start().catch(console.error);