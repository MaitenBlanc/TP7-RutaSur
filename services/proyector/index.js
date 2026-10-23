require("dotenv").config({ path: "../../.env" });
const { Kafka } = require("kafkajs");
const mongoose = require("mongoose");
const { Envio, Alerta, Notificacion, EventoProcesado } = require("./models");

const kafka = new Kafka({
  clientId: "proyector",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
  retry: {
    initialRetryTime: 1000,
    retries: 15,
  },
});

const consumer = kafka.consumer({ groupId: "proyector" });
const producer = kafka.producer(); // envía errores a la DLQ

const PRIORIDAD_ORDEN = {
  Crítica: 1,
  Alta: 2,
  Media: 3,
};

let reconstruyendo = false;
let eventosAplicadosTotal = 0;

async function conectarDB() {
  await mongoose.connect(process.env.MONGO_URI);
  console.log("[Proyector] Conectado a MongoDB");
}

async function enviarADlq(message, motivoError, topicOrigen) {
  let rawStr = "";
  try {
    rawStr = message.value ? message.value.toString() : "";
  } catch {
    rawStr = "unparseable";
  }

  const dlqPayload = {
    motivo: motivoError,
    topicOrigen,
    recibidoEn: new Date().toISOString(),
    payloadOriginal: rawStr,
  };

  await producer.send({
    topic: "envios.estados.dlq",
    messages: [
      {
        key: message.key ? message.key.toString() : "sin-clave",
        value: JSON.stringify(dlqPayload),
        headers: { error: motivoError, topicOrigen },
      },
    ],
  });
}

async function procesarEvento(message, topic, partition) {
  let evento;

  // Validar formato JSON y existencia de campos clave de esquema
  try {
    evento = JSON.parse(message.value.toString());
    if (!evento || typeof evento !== "object") {
      throw new Error("El payload no es un objeto JSON válido");
    }
    if (!evento.shipmentId || !evento.eventId) {
      throw new Error("Evento malformado: faltan campos obligatorios (shipmentId o eventId)");
    }
    if (topic === "envios.estados" && !evento.tipo) {
      throw new Error("Evento malformado en envios.estados: falta el campo tipo");
    }
  } catch (error) {
    console.error(
      `[Proyector] Evento inválido en ${topic} [partición=${partition}, offset=${message.offset}]. Enviando a envios.estados.dlq -> Motivo: ${error.message}`
    );
    await enviarADlq(message, error.message, topic);
    return;
  }

  const { eventId, shipmentId, tipo, ocurridoEn, secuencia } = evento;

  // Control de Idempotencia (Regla de duplicados: un eventId ya registrado no se vuelve a aplicar)
  try {
    await EventoProcesado.create({ consumidor: "proyector", eventId });
  } catch (err) {
    if (err.code === 11000) {
      console.warn(
        `[Proyector] Ignorando evento duplicado: eventId=${eventId} (shipmentId=${shipmentId}, partición=${partition}, offset=${message.offset})`
      );
      return;
    }
    throw err;
  }

  // Calcular demora de sincronización (diferencia entre timestamp de Kafka y ocurridoEn)
  const kafkaTs = message.timestamp ? parseInt(message.timestamp) : Date.now();
  const origenTs = ocurridoEn ? new Date(ocurridoEn).getTime() : kafkaTs;
  const demoraSincronizacionMs = Math.max(0, kafkaTs - origenTs);

  // Obtener o inicializar el documento del envío en MongoDB
  let envio = await Envio.findOne({ shipmentId });
  if (!envio) {
    envio = new Envio({
      shipmentId,
      estado: "CREADO",
      ultimaSecuencia: 0,
      intentos: evento.intentos || 1,
      historial: [],
    });
  }

  // Procesamiento según el topic
  if (topic === "envios.promesas") {
    envio.promesa = {
      venceEn: evento.venceEn ? new Date(evento.venceEn) : null,
      ocurridoEn: evento.ocurridoEn ? new Date(evento.ocurridoEn) : new Date(),
    };
    if (evento.zonaId && !envio.zonaId) envio.zonaId = evento.zonaId;
    if (evento.hubId && !envio.hubId) envio.hubId = evento.hubId;
    if (evento.franja && !envio.franja) envio.franja = evento.franja;

    await envio.save();
    eventosAplicadosTotal += 1;
    console.log(
      `[Proyector] Promesa aplicada | shipmentId=${shipmentId} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
    );
    return;
  }

  if (topic === "notificaciones.enviadas") {
    await Notificacion.create({
      eventId,
      shipmentId,
      tipoAviso: evento.tipoAviso || "AVISO",
      mensaje: evento.mensaje || "",
      opcionesReprogramacion: evento.opcionesReprogramacion || [],
      ocurridoEn: evento.ocurridoEn ? new Date(evento.ocurridoEn) : new Date(),
    });

    envio.historial.push({
      tipo: "NOTIFICACION_ENVIADA",
      detalle: `${evento.tipoAviso}: ${evento.mensaje || ""}`,
      ocurridoEn: evento.ocurridoEn || new Date().toISOString(),
      kafkaTimestamp: new Date(kafkaTs).toISOString(),
      eventId,
    });

    await envio.save();
    eventosAplicadosTotal += 1;
    console.log(
      `[Proyector] Notificación registrada | shipmentId=${shipmentId} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
    );
    return;
  }

  if (topic === "alertas.sla") {
    if (evento.tipo === "ALERTA_GENERADA") {
      await Alerta.findOneAndUpdate(
        { shipmentId, causa: evento.causa },
        {
          $set: {
            eventId,
            shipmentId,
            causa: evento.causa,
            prioridad: evento.prioridad || "Media",
            PrioridadOrden: PRIORIDAD_ORDEN[evento.prioridad] || 3,
            estado: "ABIERTA",
            ocurridoEn: evento.ocurridoEn ? new Date(evento.ocurridoEn) : new Date(),
          },
        },
        { upsert: true, returnDocument: "after" }
      );

      envio.historial.push({
        tipo: "ALERTA_SLA",
        detalle: `Alerta ${evento.prioridad}: ${evento.causa}`,
        prioridad: evento.prioridad,
        ocurridoEn: evento.ocurridoEn || new Date().toISOString(),
        eventId,
      });
      await envio.save();
    } else if (evento.tipo === "ALERTA_CERRADA") {
      await Alerta.updateMany(
        { shipmentId, estado: "ABIERTA" },
        {
          $set: {
            estado: "CERRADA",
            cerradaEn: evento.ocurridoEn ? new Date(evento.ocurridoEn) : new Date(),
          },
        }
      );

      envio.historial.push({
        tipo: "ALERTA_CERRADA",
        detalle: "Alertas de SLA cerradas por entrega completada",
        ocurridoEn: evento.ocurridoEn || new Date().toISOString(),
        eventId,
      });
      await envio.save();
    }

    eventosAplicadosTotal += 1;
    console.log(
      `[Proyector] Evento de alerta (${evento.tipo}) | shipmentId=${shipmentId} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
    );
    return;
  }

  if (topic === "envios.reprogramaciones") {
    // No sobreescribir si el envío ya llegó a ENTREGADO
    if (envio.estado !== "ENTREGADO") {
      envio.estado = tipo; // REPROGRAMADO | RETENIDO | DEVOLUCION_COMERCIO
    }
    envio.reprogramacion = {
      proximoIntentoFecha: evento.proximoIntentoFecha || null,
      proximaFranja: evento.proximaFranja || null,
      motivo: evento.motivoOriginal || null,
    };
    if (evento.intentos) {
      envio.intentos = Math.max(envio.intentos || 1, evento.intentos);
    }

    envio.historial.push({
      tipo,
      detalle:
        tipo === "REPROGRAMADO"
          ? `Reprogramado para ${evento.proximoIntentoFecha || "día hábil siguiente"} (${evento.proximaFranja || "franja opuesta"})`
          : `Decisión tras fallo (${evento.motivoOriginal || "N/A"}): ${tipo}`,
      ocurridoEn: evento.ocurridoEn || new Date().toISOString(),
      eventId,
    });

    await envio.save();
    eventosAplicadosTotal += 1;
    console.log(
      `[Proyector] Reprogramación (${tipo}) aplicada | shipmentId=${shipmentId} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
    );
    return;
  }

  // Topic envios.estados: Regla de orden por secuencia
  if (secuencia && secuencia <= envio.ultimaSecuencia) {
    console.warn(
      `[Proyector] Evento fuera de orden descartado | shipmentId=${shipmentId} | eventId=${eventId} | secuenciaLlegada=${secuencia} <= ultimaSecuencia=${envio.ultimaSecuencia}`
    );
    return;
  }

  // No retroceder de un estado terminal final (DEVOLUCION_COMERCIO / RETENIDO / ENTREGADO) salvo secuencia superior válida
  envio.estado = tipo;
  if (secuencia) envio.ultimaSecuencia = secuencia;
  if (evento.zonaId) envio.zonaId = evento.zonaId;
  if (evento.hubId) envio.hubId = evento.hubId;
  if (evento.repartidorId) envio.repartidorId = evento.repartidorId;
  if (evento.franja) envio.franja = evento.franja;
  if (evento.intentos) envio.intentos = evento.intentos;
  if (evento.venceEn && (!envio.promesa || !envio.promesa.venceEn)) {
    envio.promesa = {
      venceEn: new Date(evento.venceEn),
      ocurridoEn: ocurridoEn ? new Date(ocurridoEn) : new Date(),
    };
  }

  envio.historial.push({
    tipo,
    motivo: evento.motivo || undefined,
    intentos: evento.intentos || 1,
    secuencia: secuencia || undefined,
    ocurridoEn: ocurridoEn || new Date().toISOString(),
    kafkaTimestamp: new Date(kafkaTs).toISOString(),
    demoraSincronizacionMs,
    eventId,
  });

  await envio.save();

  // Si el envío se entregó, también asegurar el cierre de alertas abiertas en la vista
  if (tipo === "ENTREGADO") {
    await Alerta.updateMany(
      { shipmentId, estado: "ABIERTA" },
      { $set: { estado: "CERRADA", cerradaEn: new Date() } }
    );
  }

  eventosAplicadosTotal += 1;
  console.log(
    `[Proyector] Estado ${tipo} aplicado | shipmentId=${shipmentId} | seq=${secuencia} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
  );
}

// Vigilante de reconstrucción: si se borra la colección 'envios', rebobina los offsets a 0 para reconstruirla desde Kafka
function iniciarMonitorReconstruccion() {
  setInterval(async () => {
    try {
      if (reconstruyendo || eventosAplicadosTotal === 0) return;
      const countEnvios = await Envio.countDocuments();
      if (countEnvios === 0) {
        reconstruyendo = true;
        console.log(
          "[Proyector] Se detectó el vaciado de la colección 'envios'. Limpiando idempotencia del proyector y rebobinando offsets a 0 para reconstruir desde Kafka..."
        );
        await EventoProcesado.deleteMany({ consumidor: "proyector" });

        // Seek al inicio en todas las particiones de los topics suscriptos
        for (const p of [0, 1, 2]) {
          consumer.seek({ topic: "envios.estados", partition: p, offset: "0" });
        }
        consumer.seek({ topic: "envios.promesas", partition: 0, offset: "0" });
        consumer.seek({ topic: "envios.reprogramaciones", partition: 0, offset: "0" });
        consumer.seek({ topic: "notificaciones.enviadas", partition: 0, offset: "0" });
        consumer.seek({ topic: "alertas.sla", partition: 0, offset: "0" });

        setTimeout(() => {
          reconstruyendo = false;
        }, 3000);
      }
    } catch (err) {
      console.error("[Proyector] Error en monitor de reconstrucción:", err.message);
      reconstruyendo = false;
    }
  }, 2000);
}

async function start() {
  await conectarDB();
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: "envios.estados", fromBeginning: true });
  await consumer.subscribe({ topic: "envios.promesas", fromBeginning: true });
  await consumer.subscribe({ topic: "envios.reprogramaciones", fromBeginning: true });
  await consumer.subscribe({ topic: "notificaciones.enviadas", fromBeginning: true });
  await consumer.subscribe({ topic: "alertas.sla", fromBeginning: true });

  await consumer.run({
    autoCommit: false, // Confirmación manual de offsets después de escribir en MongoDB
    eachMessage: async ({ topic, partition, message }) => {
      await procesarEvento(message, topic, partition);

      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (parseInt(message.offset) + 1).toString(),
        },
      ]);
    },
  });

  iniciarMonitorReconstruccion();
  console.log("[Proyector] Escuchando los 5 topics con confirmación manual de offsets...");
}

start().catch(console.error);
