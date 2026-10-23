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
  clientId: "notificaciones",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
  retry: {
    initialRetryTime: 1000,
    retries: 15,
  },
});

const consumer = kafka.consumer({ groupId: "notificaciones" });
const producer = kafka.producer();

function obtenerOpcionesPorMotivo(motivo) {
  switch (motivo) {
    case "AUSENTE":
      return ["Cambiar a franja TARDE", "Cambiar a franja MAÑANA", "Retirar en Hub"];
    case "DIRECCION_INCORRECTA":
      return ["Actualizar calle y altura", "Agregar referencias del domicilio"];
    case "ZONA_INACCESIBLE":
      return ["Coordinar punto de encuentro", "Retirar en sucursal RutaSur"];
    case "RECHAZADO":
      return ["Confirmar devolución al comercio"];
    default:
      return ["Reprogramar entrega"];
  }
}

async function procesarMensaje({ topic, partition, message }) {
  let evento;
  try {
    evento = JSON.parse(message.value.toString());
    if (!evento || !evento.shipmentId || !evento.eventId) {
      throw new Error("Evento malformado: faltan shipmentId o eventId");
    }
  } catch (err) {
    console.error(
      `[Notificaciones] Evento inválido en ${topic} [partición=${partition}, offset=${message.offset}]: ${err.message}`
    );
    await producer.send({
      topic: "envios.estados.dlq",
      messages: [
        {
          key: message.key ? message.key.toString() : "sin-clave",
          value: JSON.stringify({
            motivo: err.message,
            consumidor: "notificaciones",
            topicOrigen: topic,
            payloadOriginal: message.value ? message.value.toString() : "",
          }),
          headers: { error: err.message },
        },
      ],
    });
    return;
  }

  const { eventId, shipmentId, tipo } = evento;

  // Solo nos interesan ENTREGA_FALLIDA (en envios.estados) y ALERTA_GENERADA (en alertas.sla)
  const esFallo = topic === "envios.estados" && tipo === "ENTREGA_FALLIDA";
  const esAlerta = topic === "alertas.sla" && tipo === "ALERTA_GENERADA";
  if (!esFallo && !esAlerta) {
    return;
  }

  // Control de idempotencia: no volver a notificar un eventId ya procesado
  try {
    await EventoProcesado.create({ consumidor: "notificaciones", eventId });
  } catch (err) {
    if (err.code === 11000) {
      console.warn(
        `[Notificaciones] Duplicado descartado | eventId=${eventId} | shipmentId=${shipmentId} | partición=${partition} | offset=${message.offset}`
      );
      return;
    }
    throw err;
  }

  if (esFallo) {
    const motivo = evento.motivo || "AUSENTE";
    const aviso = {
      eventId: uuidv4(),
      shipmentId,
      tipoAviso: "FALLO_ENTREGA",
      motivo,
      mensaje: `Intento #${evento.intentos || 1} fallido (${motivo}). Opciones disponibles para gestionar tu envío.`,
      opcionesReprogramacion: obtenerOpcionesPorMotivo(motivo),
      ocurridoEn: evento.ocurridoEn || new Date().toISOString(),
      version: 1,
    };

    await producer.send({
      topic: "notificaciones.enviadas",
      messages: [{ key: shipmentId, value: JSON.stringify(aviso) }],
    });

    console.log(
      `[Notificaciones] Aviso de fallo emitido | shipmentId=${shipmentId} | motivo=${motivo} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
    );
  } else if (esAlerta) {
    const avisoAlerta = {
      eventId: uuidv4(),
      shipmentId,
      tipoAviso: `ALERTA_SLA_${evento.prioridad?.toUpperCase() || "ALTA"}`,
      mensaje: `Aviso preventivo SLA (${evento.causa}): estamos priorizando tu envío.`,
      opcionesReprogramacion: [],
      ocurridoEn: evento.ocurridoEn || new Date().toISOString(),
      version: 1,
    };

    await producer.send({
      topic: "notificaciones.enviadas",
      messages: [{ key: shipmentId, value: JSON.stringify(avisoAlerta) }],
    });

    console.log(
      `[Notificaciones] Aviso de alerta SLA emitido | shipmentId=${shipmentId} | causa=${evento.causa} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
    );
  }
}

async function start() {
  await mongoose.connect(process.env.MONGO_URI);
  await producer.connect();
  await consumer.connect();

  // Escucha fallos (CU2) y alertas de SLA (CU3)
  await consumer.subscribe({ topic: "envios.estados", fromBeginning: true });
  await consumer.subscribe({ topic: "alertas.sla", fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      await procesarMensaje({ topic, partition, message });
      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (parseInt(message.offset) + 1).toString(),
        },
      ]);
    },
  });

  console.log("[Notificaciones] Escuchando eventos con idempotencia y confirmación manual...");
}

start().catch(console.error);
