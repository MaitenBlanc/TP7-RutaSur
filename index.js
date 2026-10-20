require("dotenv").config({ path: "../../.env" });
const { Kafka } = require("kafkajs");
const mongoose = require("mongoose");
const { Envio, EventoProcesado } = require("./models");

const kafka = new Kafka({
  clientId: "proyector",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "proyector" });
const producer = kafka.producer(); // Usado para mandar errores a la DLQ

async function conectarDB() {
  await mongoose.connect(process.env.MONGO_URI);
  console.log("[Proyector] Conectado a MongoDB");
}

async function procesarEvento(message, topic) {
  let evento;

  // Validar formato JSON y existencia de campos clave
  try {
    evento = JSON.parse(message.value.toString());
    if (!evento.shipmentId || !evento.eventId)
      throw new Error("Evento malformado (faltan campos clave)");
  } catch (error) {
    console.error(
      "[Proyector] Evento inválido detectado. Enviando a DLQ...",
      error.message,
    );
    await producer.send({
      topic: `${topic}.dlq`,
      messages: [{ value: message.value, headers: { error: error.message } }],
    });
    return; // Sale sin fallar, proceso completo
  }

  const { eventId, shipmentId, tipo, ocurridoEn, secuencia } = evento;

  // Control de Idempotencia (Duplicados)
  try {
    await EventoProcesado.create({ consumidor: "proyector", eventId });
  } catch (err) {
    if (err.code === 11000) {
      console.warn(`[Proyector] Ignorando evento duplicado: ${eventId}`);
      return;
    }
    throw err;
  }

  // Obtener o inicializar el documento del envío en MongoDB
  let envio = await Envio.findOne({ shipmentId });
  if (!envio) {
    envio = new Envio({ shipmentId, estado: "PENDIENTE", historial: [] });
  }

  // Regla de Secuencia: Descartar si el evento llegó fuera de orden
  if (secuencia && secuencia <= envio.ultimaSecuencia) {
    console.warn(
      `[Proyector] Evento fuera de orden descartado para ${shipmentId} (Llegó: ${secuencia}, Actual: ${envio.ultimaSecuencia})`,
    );
    return;
  }

  // Aplicar cambios según el estado (Embeber el historial)
  envio.estado = tipo;
  if (secuencia) envio.ultimaSecuencia = secuencia;
  if (evento.zonaId) envio.zonaId = evento.zonaId;
  if (evento.hubId) envio.hubId = evento.hubId;

  envio.historial.push({ tipo, ocurridoEn, eventId });
  await envio.save();

  console.log(`[Proyector] Envío ${shipmentId} actualizado a estado: ${tipo}`);
}

async function start() {
  await conectarDB();
  await producer.connect();
  await consumer.connect();

  // El proyector escucha varios topics, empezando por los estados
  await consumer.subscribe({ topic: "envios.estados", fromBeginning: true });

  await consumer.run({
    autoCommit: false, // Confirmación manual (decisión técnica)
    eachMessage: async ({ topic, partition, message }) => {
      await procesarEvento(message, topic);

      // Confirmar offset en Kafka, si se guardó correctamente en Mongo
      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (parseInt(message.offset) + 1).toString(),
        },
      ]);
    },
  });
  console.log("[Proyector] Escuchando eventos...");
}

start().catch(console.error);
