require("dotenv").config({ path: "../../.env" });
const { Kafka } = require("kafkajs");
const mongoose = require("mongoose");
const { v4: uuidv4 } = require("uuid");

// Esquema de idempotencia compartido en la colección eventos_procesados
const eventoProcesadoSchema = new mongoose.Schema(
  {
    consumidor: { type: String, required: true },
    eventId: { type: String, required: true },
    createdAt: { type: Date, expires: "7d", default: Date.now },
  },
  { collection: "eventos_procesados" }
);
eventoProcesadoSchema.index({ consumidor: 1, eventId: 1 }, { unique: true });
const EventoProcesado = mongoose.model("EventoProcesado", eventoProcesadoSchema);

const kafka = new Kafka({
  clientId: "reprogramacion",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
  retry: {
    initialRetryTime: 1000,
    retries: 15,
  },
});

const consumer = kafka.consumer({ groupId: "reprogramacion" });
const producer = kafka.producer();

// Calcula el día hábil siguiente (salta sábado=6 y domingo=0)
function calcularDiaHabilSiguiente(fechaIso) {
  const fecha = fechaIso ? new Date(fechaIso) : new Date();
  fecha.setDate(fecha.getDate() + 1);
  while (fecha.getDay() === 0 || fecha.getDay() === 6) {
    fecha.setDate(fecha.getDate() + 1);
  }
  return fecha.toISOString().split("T")[0];
}

async function procesarFallo(evento, partition, offset) {
  const { eventId, shipmentId, motivo = "AUSENTE", intentos = 1, franja = "MAÑANA", ocurridoEn } = evento;

  // Control de idempotencia
  try {
    await EventoProcesado.create({ consumidor: "reprogramacion", eventId });
  } catch (err) {
    if (err.code === 11000) {
      console.warn(
        `[Reprogramación] Duplicado descartado | eventId=${eventId} | shipmentId=${shipmentId} | partición=${partition} | offset=${offset}`
      );
      return;
    }
    throw err;
  }

  let nuevoEstado = "";
  let proximoIntentoFecha = null;
  let proximaFranja = null;

  // Reglas de negocio de reprogramación
  // 1. Tercer intento fallido, cualquiera sea el motivo -> DEVOLUCION_COMERCIO
  // 2. RECHAZADO -> DEVOLUCION_COMERCIO
  // 3. DIRECCION_INCORRECTA (o ZONA_INACCESIBLE) -> RETENIDO (no se reprograma solo)
  // 4. AUSENTE (con < 3 intentos) -> REPROGRAMADO al día hábil siguiente en la franja opuesta
  if (intentos >= 3 || motivo === "RECHAZADO") {
    nuevoEstado = "DEVOLUCION_COMERCIO";
  } else if (motivo === "DIRECCION_INCORRECTA" || motivo === "ZONA_INACCESIBLE") {
    nuevoEstado = "RETENIDO";
  } else if (motivo === "AUSENTE") {
    nuevoEstado = "REPROGRAMADO";
    proximoIntentoFecha = calcularDiaHabilSiguiente(ocurridoEn);
    proximaFranja = franja === "MAÑANA" ? "TARDE" : "MAÑANA";
  } else {
    nuevoEstado = "RETENIDO";
  }

  const eventoReprogramacion = {
    eventId: uuidv4(),
    shipmentId,
    tipo: nuevoEstado,
    motivoOriginal: motivo,
    intentos,
    proximoIntentoFecha,
    proximaFranja,
    ocurridoEn: ocurridoEn || new Date().toISOString(),
    version: 1,
  };

  await producer.send({
    topic: "envios.reprogramaciones",
    messages: [{ key: shipmentId, value: JSON.stringify(eventoReprogramacion) }],
  });

  console.log(
    `[Reprogramación] Decisión=${nuevoEstado} (intento=${intentos}, motivo=${motivo}, proxFecha=${proximoIntentoFecha || "-"}, proxFranja=${proximaFranja || "-"}) | shipmentId=${shipmentId} | eventId=${eventId} | partición=${partition} | offset=${offset}`
  );
}

async function start() {
  await mongoose.connect(process.env.MONGO_URI);
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "envios.estados", fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      let evento;
      try {
        evento = JSON.parse(message.value.toString());
        if (!evento || !evento.shipmentId || !evento.eventId) {
          throw new Error("Evento malformado: faltan shipmentId o eventId");
        }
      } catch (err) {
        console.error(
          `[Reprogramación] Evento inválido [partición=${partition}, offset=${message.offset}]: ${err.message}`
        );
        await producer.send({
          topic: "envios.estados.dlq",
          messages: [
            {
              key: message.key ? message.key.toString() : "sin-clave",
              value: JSON.stringify({
                motivo: err.message,
                consumidor: "reprogramacion",
                topicOrigen: topic,
                payloadOriginal: message.value ? message.value.toString() : "",
              }),
              headers: { error: err.message },
            },
          ],
        });
        await consumer.commitOffsets([
          { topic, partition, offset: (parseInt(message.offset) + 1).toString() },
        ]);
        return;
      }

      if (evento.tipo === "ENTREGA_FALLIDA") {
        await procesarFallo(evento, partition, message.offset);
      }

      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (parseInt(message.offset) + 1).toString(),
        },
      ]);
    },
  });

  console.log("[Reprogramación] Escuchando fallos de entrega con idempotencia y confirmación manual...");
}

start().catch(console.error);
