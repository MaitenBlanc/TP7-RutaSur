require("dotenv").config({ path: "../../.env" });
const { Kafka } = require("kafkajs");
const mongoose = require("mongoose");
const cron = require("node-cron");
const { v4: uuidv4 } = require("uuid");
const { Vencimiento, EventoProcesado } = require("./models");

const kafka = new Kafka({
  clientId: "monitor-sla",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
  retry: { initialRetryTime: 1000, retries: 15 },
});

const consumer = kafka.consumer({ groupId: "monitor-sla" });
const producer = kafka.producer();

const ACELERACION = parseInt(process.env.SIMULADOR_ACELERACION) || 120;

// Estado en memoria sincronizado con MongoDB
const estadoEnMemoria = new Map();

// Reloj simulado sincronizado con el campo ocurridoEn que viaja en los eventos
let ultimoTiempoSimuladoMs = Date.now();
let ultimoTiempoRealMs = Date.now();

function actualizarRelojSimulado(ocurridoEnIso) {
  if (!ocurridoEnIso) return;
  const tsEvento = new Date(ocurridoEnIso).getTime();
  if (!isNaN(tsEvento) && tsEvento > ultimoTiempoSimuladoMs) {
    ultimoTiempoSimuladoMs = tsEvento;
    ultimoTiempoRealMs = Date.now();
  }
}

function getTiempoSimulado(referenciaIso = null) {
  if (referenciaIso) {
    const refMs = new Date(referenciaIso).getTime();
    if (!isNaN(refMs)) {
      actualizarRelojSimulado(referenciaIso);
      return new Date(Math.max(refMs, ultimoTiempoSimuladoMs));
    }
  }
  const deltaRealMs = Date.now() - ultimoTiempoRealMs;
  const simMs = ultimoTiempoSimuladoMs + deltaRealMs * ACELERACION;
  return new Date(simMs);
}

// Evalúa las 4 reglas de SLA sobre un envío concreto
async function evaluarEnvio(registro, ahoraSimulado) {
  if (!registro || !registro.venceEn) return;
  if (registro.ultimoEstado === "ENTREGADO") return;

  const venceDate = new Date(registro.venceEn);
  const horasRestantes = (venceDate.getTime() - ahoraSimulado.getTime()) / (1000 * 60 * 60);
  const diaVence = venceDate.toISOString().split("T")[0];
  const diaAhora = ahoraSimulado.toISOString().split("T")[0];
  const venceHoy = diaVence === diaAhora || (horasRestantes > 0 && horasRestantes <= 24);

  const candidatos = [];

  // Regla 4: Vencido -> Pasó el vencimiento sin ENTREGADO (Prioridad: Crítica)
  if (horasRestantes <= 0 && !registro.alertasEmitidas.includes("Vencido")) {
    candidatos.push({ regla: "Vencido", prioridad: "Crítica" });
  }

  // Regla 3: Fallo en el día límite -> ENTREGA_FALLIDA y la promesa vence hoy (Prioridad: Alta)
  if (
    registro.ultimoEstado === "ENTREGA_FALLIDA" &&
    venceHoy &&
    !registro.alertasEmitidas.includes("Fallo en el día límite")
  ) {
    candidatos.push({ regla: "Fallo en el día límite", prioridad: "Alta" });
  }

  // Regla 2: Sin salida -> Faltan menos de 4 h y no está en reparto (Prioridad: Alta)
  if (
    horasRestantes > 0 &&
    horasRestantes < 4 &&
    registro.ultimoEstado !== "EN_REPARTO" &&
    registro.ultimoEstado !== "ENTREGADO" &&
    !registro.alertasEmitidas.includes("Sin salida")
  ) {
    candidatos.push({ regla: "Sin salida", prioridad: "Alta" });
  }

  // Regla 1: Sin ingreso -> Faltan menos de 10 h para el vencimiento y no ingresó al hub (Prioridad: Media)
  if (
    horasRestantes > 0 &&
    horasRestantes < 10 &&
    registro.ultimoEstado === "CREADO" &&
    !registro.alertasEmitidas.includes("Sin ingreso")
  ) {
    candidatos.push({ regla: "Sin ingreso", prioridad: "Media" });
  }

  // Paquetes dormidos en hub
  // Detecta la ausencia del evento esperado (EN_REPARTO) tras permanecer >= 6h simuladas en el hub
  if (
    (registro.ultimoEstado === "INGRESADO_HUB" ||
      registro.ultimoEstado === "CLASIFICADO") &&
    registro.ingresadoHubEn &&
    !registro.alertasEmitidas.includes("Paquete dormido en hub")
  ) {
    const horasEnHub =
      (ahoraSimulado.getTime() - new Date(registro.ingresadoHubEn).getTime()) /
      (1000 * 60 * 60);
    if (horasEnHub >= 6) {
      candidatos.push({ regla: "Paquete dormido en hub", prioridad: "Media" });
    }
  }

  if (candidatos.length === 0) return;

  for (const alerta of candidatos) {
    registro.alertasEmitidas.push(alerta.regla);
  }

  // Persistir en MongoDB y actualizar memoria
  await Vencimiento.findOneAndUpdate(
    { shipmentId: registro.shipmentId },
    {
      $set: {
        ultimoEstado: registro.ultimoEstado,
        venceEn: registro.venceEn,
        ingresadoHubEn: registro.ingresadoHubEn || null,
        ultimoOcurridoEn: ahoraSimulado,
        alertasEmitidas: registro.alertasEmitidas,
      },
    },
    { upsert: true }
  );
  estadoEnMemoria.set(registro.shipmentId, registro);

  for (const alerta of candidatos) {
    const alertaMsg = {
      eventId: uuidv4(),
      shipmentId: registro.shipmentId,
      tipo: "ALERTA_GENERADA",
      causa: alerta.regla,
      prioridad: alerta.prioridad,
      ocurridoEn: ahoraSimulado.toISOString(),
      version: 1,
    };

    await producer.send({
      topic: "alertas.sla",
      messages: [{ key: registro.shipmentId, value: JSON.stringify(alertaMsg) }],
    });

    console.log(
      `[Monitor SLA] ALERTA EMITIDA | shipmentId=${registro.shipmentId} | regla="${alerta.regla}" | prioridad=${alerta.prioridad} | horasRestantes=${horasRestantes.toFixed(1)}h`
    );
  }
}

async function procesarEvento(message, topic, partition) {
  let evento;
  try {
    evento = JSON.parse(message.value.toString());
    if (!evento || !evento.shipmentId || !evento.eventId) {
      throw new Error("Evento malformado: faltan shipmentId o eventId");
    }
  } catch (err) {
    console.error(
      `[Monitor SLA] Evento inválido en ${topic} [partición=${partition}, offset=${message.offset}]: ${err.message}`
    );
    await producer.send({
      topic: "envios.estados.dlq",
      messages: [
        {
          key: message.key ? message.key.toString() : "sin-clave",
          value: JSON.stringify({
            motivo: err.message,
            consumidor: "monitor-sla",
            topicOrigen: topic,
            payloadOriginal: message.value ? message.value.toString() : "",
          }),
          headers: { error: err.message },
        },
      ],
    });
    return;
  }

  const { eventId, shipmentId, tipo, venceEn, ocurridoEn } = evento;

  // Idempotencia con eventos_procesados
  try {
    await EventoProcesado.create({ consumidor: "monitor-sla", eventId });
  } catch (err) {
    if (err.code === 11000) {
      return;
    }
    throw err;
  }

  const ahoraSimulado = getTiempoSimulado(ocurridoEn);

  let registro = estadoEnMemoria.get(shipmentId);
  if (!registro) {
    const docDb = await Vencimiento.findOne({ shipmentId });
    if (docDb) {
      registro = {
        shipmentId: docDb.shipmentId,
        ultimoEstado: docDb.ultimoEstado,
        venceEn: docDb.venceEn,
        ingresadoHubEn: docDb.ingresadoHubEn || null,
        ultimoOcurridoEn: docDb.ultimoOcurridoEn,
        alertasEmitidas: docDb.alertasEmitidas || [],
      };
    } else {
      registro = {
        shipmentId,
        ultimoEstado: tipo || "CREADO",
        venceEn: venceEn ? new Date(venceEn) : null,
        ingresadoHubEn: null,
        ultimoOcurridoEn: ahoraSimulado,
        alertasEmitidas: [],
      };
    }
  }

  if (topic === "envios.promesas") {
    if (venceEn) registro.venceEn = new Date(venceEn);
    registro.ultimoOcurridoEn = ahoraSimulado;
    estadoEnMemoria.set(shipmentId, registro);

    await Vencimiento.findOneAndUpdate(
      { shipmentId },
      {
        $set: {
          ultimoEstado: registro.ultimoEstado,
          venceEn: registro.venceEn,
          ingresadoHubEn: registro.ingresadoHubEn || null,
          ultimoOcurridoEn: registro.ultimoOcurridoEn,
          alertasEmitidas: registro.alertasEmitidas,
        },
      },
      { upsert: true }
    );

    console.log(
      `[Monitor SLA] Promesa registrada | shipmentId=${shipmentId} | venceEn=${venceEn} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
    );

    // Evaluar inmediatamente al recibir el evento
    await evaluarEnvio(registro, ahoraSimulado);
    return;
  }

  if (topic === "envios.estados") {
    registro.ultimoEstado = tipo;
    if (venceEn && !registro.venceEn) {
      registro.venceEn = new Date(venceEn);
    }
    if (tipo === "INGRESADO_HUB" && !registro.ingresadoHubEn) {
      registro.ingresadoHubEn = ocurridoEn ? new Date(ocurridoEn) : ahoraSimulado;
    } else if (tipo === "EN_REPARTO") {
      registro.ingresadoHubEn = null;
    }
    registro.ultimoOcurridoEn = ahoraSimulado;

    // Cierre: cuando llega ENTREGADO, el monitor emite el cierre de la alerta y quita de abiertos
    if (tipo === "ENTREGADO") {
      estadoEnMemoria.delete(shipmentId);
      await Vencimiento.deleteOne({ shipmentId });

      const cierreMsg = {
        eventId: uuidv4(),
        shipmentId,
        tipo: "ALERTA_CERRADA",
        ocurridoEn: ahoraSimulado.toISOString(),
        version: 1,
      };

      await producer.send({
        topic: "alertas.sla",
        messages: [{ key: shipmentId, value: JSON.stringify(cierreMsg) }],
      });

      console.log(
        `[Monitor SLA] Cierre de alerta emitido (ENTREGADO) | shipmentId=${shipmentId} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
      );
      return;
    }

    estadoEnMemoria.set(shipmentId, registro);
    await Vencimiento.findOneAndUpdate(
      { shipmentId },
      {
        $set: {
          ultimoEstado: registro.ultimoEstado,
          venceEn: registro.venceEn,
          ingresadoHubEn: registro.ingresadoHubEn || null,
          ultimoOcurridoEn: registro.ultimoOcurridoEn,
          alertasEmitidas: registro.alertasEmitidas,
        },
      },
      { upsert: true }
    );

    // Evaluar inmediatamente al recibir el evento
    await evaluarEnvio(registro, ahoraSimulado);
  }
}

async function evaluarTodosLosEnviosAbiertos() {
  const ahoraSimulado = getTiempoSimulado();
  for (const registro of estadoEnMemoria.values()) {
    try {
      await evaluarEnvio(registro, ahoraSimulado);
    } catch (err) {
      console.error(`[Monitor SLA] Error evaluando ${registro.shipmentId}:`, err.message);
    }
  }
}

async function restaurarEstadoDesdeMongo() {
  const abiertos = await Vencimiento.find();
  for (const doc of abiertos) {
    estadoEnMemoria.set(doc.shipmentId, {
      shipmentId: doc.shipmentId,
      ultimoEstado: doc.ultimoEstado,
      venceEn: doc.venceEn,
      ingresadoHubEn: doc.ingresadoHubEn || null,
      ultimoOcurridoEn: doc.ultimoOcurridoEn,
      alertasEmitidas: doc.alertasEmitidas || [],
    });
    if (doc.ultimoOcurridoEn) {
      actualizarRelojSimulado(doc.ultimoOcurridoEn.toISOString());
    }
  }
  console.log(`[Monitor SLA] Estado restaurado desde MongoDB: ${abiertos.length} envíos abiertos`);
}

async function start() {
  await mongoose.connect(process.env.MONGO_URI);
  await restaurarEstadoDesdeMongo();

  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: "envios.promesas", fromBeginning: true });
  await consumer.subscribe({ topic: "envios.estados", fromBeginning: true });

  await consumer.run({
    autoCommit: false,
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

  // Evaluación periódica cada minuto y cada 5 segundos reales (10 min simulados con aceleración 120x)
  cron.schedule("* * * * *", evaluarTodosLosEnviosAbiertos);
  setInterval(evaluarTodosLosEnviosAbiertos, 5000);

  console.log("[Monitor SLA] Escuchando estados/promesas y evaluando las 4 reglas con reloj simulado...");
}

start().catch(console.error);
