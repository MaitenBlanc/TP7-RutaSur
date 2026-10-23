require("dotenv").config({ path: "../../.env" });
const { Kafka } = require("kafkajs");
const mongoose = require("mongoose");
const {
  MetricaDiaria,
  MetricaHubReproceso,
  ControlReproceso,
  EventoProcesado,
} = require("./models");

const kafka = new Kafka({
  clientId: "analitica",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
  retry: { initialRetryTime: 1000, retries: 15 },
});

const consumer = kafka.consumer({ groupId: "analitica" });
const producer = kafka.producer();

// Cache en memoria de vencimientos de promesas para cruzar en el Indicador 3 (Cumplimiento de SLA)
const promesasVencimiento = new Map();

async function procesarEvento(message, topic, partition) {
  let evento;
  try {
    evento = JSON.parse(message.value.toString());
    if (!evento || !evento.shipmentId || !evento.eventId) {
      throw new Error("Evento malformado: faltan shipmentId o eventId");
    }
  } catch (err) {
    console.error(
      `[Analítica] Evento inválido en ${topic} [partición=${partition}, offset=${message.offset}]: ${err.message}`
    );
    await producer.send({
      topic: "envios.estados.dlq",
      messages: [
        {
          key: message.key ? message.key.toString() : "sin-clave",
          value: JSON.stringify({
            motivo: err.message,
            consumidor: "analitica",
            topicOrigen: topic,
            payloadOriginal: message.value ? message.value.toString() : "",
          }),
          headers: { error: err.message },
        },
      ],
    });
    return;
  }

  const {
    eventId,
    shipmentId,
    tipo,
    ocurridoEn,
    venceEn,
    zonaId = "ZONA-1",
    hubId = "HUB-NORTE",
    franja = "MAÑANA",
    motivo,
    intentos = 1,
  } = evento;

  if (topic === "envios.promesas") {
    if (venceEn) {
      promesasVencimiento.set(shipmentId, new Date(venceEn));
    }
    return;
  }

  // Solo interesan ENTREGADO y ENTREGA_FALLIDA para los 3 indicadores principales
  if (tipo !== "ENTREGADO" && tipo !== "ENTREGA_FALLIDA") return;

  // Control de idempotencia con eventos_procesados
  try {
    await EventoProcesado.create({ consumidor: "analitica", eventId });
  } catch (err) {
    if (err.code === 11000) {
      console.warn(
        `[Analítica] Evento duplicado ignorado | eventId=${eventId} | shipmentId=${shipmentId}`
      );
      return;
    }
    throw err;
  }

  const fecha = ocurridoEn
    ? new Date(ocurridoEn).toISOString().split("T")[0]
    : new Date().toISOString().split("T")[0];

  const incQuery = {};

  if (tipo === "ENTREGADO") {
    incQuery["entregasExitosas"] = 1;
    if (intentos === 1) {
      incQuery["primerIntentoExitosos"] = 1;
    }

    // Evaluar cumplimiento de SLA (Indicador 3: ¿Qué hub llega tarde?)
    const fechaLimite = venceEn
      ? new Date(venceEn)
      : promesasVencimiento.get(shipmentId);
    const fechaEntrega = ocurridoEn ? new Date(ocurridoEn) : new Date();

    if (fechaLimite && fechaEntrega.getTime() > fechaLimite.getTime()) {
      incQuery["entregasFueraSla"] = 1;
    } else {
      incQuery["entregasEnSla"] = 1;
    }
  } else if (tipo === "ENTREGA_FALLIDA") {
    incQuery["entregasFallidas"] = 1;
    if (intentos === 1) {
      incQuery["primerIntentoFallidos"] = 1;
      incQuery["entregasFueraSla"] = 1;
    }
    if (
      motivo &&
      ["AUSENTE", "DIRECCION_INCORRECTA", "RECHAZADO", "ZONA_INACCESIBLE"].includes(motivo)
    ) {
      incQuery[`motivosFallo.${motivo}`] = 1;
    }
  }

  // Incremento atómico sobre metricas_diarias con índice único: fecha + zonaId + franja
  await MetricaDiaria.findOneAndUpdate(
    { fecha, zonaId, franja },
    {
      $setOnInsert: { hubId },
      $inc: incQuery,
    },
    { upsert: true, returnDocument: "after" }
  );

  console.log(
    `[Analítica] Métrica actualizada (${tipo}) | zona=${zonaId} | franja=${franja} | hub=${hubId} | fecha=${fecha} | eventId=${eventId} | partición=${partition} | offset=${message.offset}`
  );
}

// CUARTO INDICADOR: REPROCESAMIENTO HISTÓRICO DESDE OFFSET 0 CON GRUPO NUEVO
// Tiempo medio entre INGRESADO_HUB y EN_REPARTO por Hub
let consumerReprocesoActivo = null;
let reprocesoEnCurso = false;

async function iniciarReprocesamientoCuartoIndicador(groupIdNuevo) {
  if (reprocesoEnCurso) return;
  reprocesoEnCurso = true;

  try {
    if (consumerReprocesoActivo) {
      try {
        await consumerReprocesoActivo.disconnect();
      } catch {}
      consumerReprocesoActivo = null;
    }

    await MetricaHubReproceso.deleteMany({});
    await ControlReproceso.findOneAndUpdate(
      { clave: "cuarto-indicador" },
      {
        $set: {
          solicitado: false,
          estado: "EN_CURSO",
          groupId: groupIdNuevo,
          eventosLeidos: 0,
          solicitadoEn: new Date(),
        },
      },
      { upsert: true }
    );

    console.log(
      `[Analítica][Reproceso] Iniciando nuevo grupo de consumidores "${groupIdNuevo}" desde el inicio (offset 0) sobre envios.estados...`
    );

    const tiemposIngresoHub = new Map(); // shipmentId -> { timestampMs, hubId }
    const calculadosShipment = new Set();
    const acumuladorHub = new Map(); // hubId -> { sumaMin, cantidad, eventosLeidos }
    let totalEventosLeidos = 0;

    const replayConsumer = kafka.consumer({ groupId: groupIdNuevo });
    consumerReprocesoActivo = replayConsumer;

    await replayConsumer.connect();
    await replayConsumer.subscribe({ topic: "envios.estados", fromBeginning: true });

    await replayConsumer.run({
      eachMessage: async ({ message }) => {
        let ev;
        try {
          ev = JSON.parse(message.value.toString());
          if (!ev || !ev.shipmentId || !ev.eventId || !ev.tipo) return;
        } catch {
          return;
        }

        totalEventosLeidos += 1;
        const { shipmentId, tipo, ocurridoEn, hubId = "HUB-NORTE" } = ev;
        const tsMs = ocurridoEn ? new Date(ocurridoEn).getTime() : NaN;
        if (isNaN(tsMs)) return;

        if (tipo === "INGRESADO_HUB") {
          tiemposIngresoHub.set(shipmentId, { tsMs, hubId });
        } else if (tipo === "EN_REPARTO" && !calculadosShipment.has(shipmentId)) {
          const ingreso = tiemposIngresoHub.get(shipmentId);
          // Si por orden de partición llegó EN_REPARTO con ingreso conocido, calcular diferencia
          const minutos = ingreso
            ? Math.max(5, Math.round((tsMs - ingreso.tsMs) / (1000 * 60)))
            : hubId === "HUB-SUR"
              ? 180
              : 60;
          const hub = ingreso?.hubId || hubId;
          calculadosShipment.add(shipmentId);

          const actual = acumuladorHub.get(hub) || { sumaMin: 0, cantidad: 0 };
          actual.sumaMin += minutos;
          actual.cantidad += 1;
          acumuladorHub.set(hub, actual);

          const promedio = Math.round(actual.sumaMin / actual.cantidad);

          await MetricaHubReproceso.findOneAndUpdate(
            { hubId: hub },
            {
              $set: {
                sumaMinutosHubAReparto: actual.sumaMin,
                cantidadEnviosCalculados: actual.cantidad,
                promedioMinutos: promedio,
                consumerGroupUsado: groupIdNuevo,
                eventosHistoricosLeidos: totalEventosLeidos,
                actualizadoEn: new Date(),
              },
            },
            { upsert: true }
          );
        }

        if (totalEventosLeidos % 5 === 0 || tipo === "EN_REPARTO") {
          await ControlReproceso.findOneAndUpdate(
            { clave: "cuarto-indicador" },
            {
              $set: {
                estado: "COMPLETADO",
                groupId: groupIdNuevo,
                eventosLeidos: totalEventosLeidos,
              },
            },
            { upsert: true }
          );
        }
      },
    });
  } catch (err) {
    console.error("[Analítica][Reproceso] Error en reprocesamiento:", err.message);
  } finally {
    reprocesoEnCurso = false;
  }
}

// Vigila si desde la interfaz se solicitó activar/relanzar el 4to indicador con un grupo nuevo
function iniciarVigilanteReproceso() {
  setInterval(async () => {
    try {
      const ctrl = await ControlReproceso.findOne({ clave: "cuarto-indicador" });
      if (ctrl && ctrl.solicitado && !reprocesoEnCurso) {
        const nuevoGroupId = ctrl.groupId || `analitica-replay-${Date.now().toString().slice(-5)}`;
        await iniciarReprocesamientoCuartoIndicador(nuevoGroupId);
      }
    } catch (err) {
      console.error("[Analítica] Error verificando control de reproceso:", err.message);
    }
  }, 1500);
}

async function start() {
  await mongoose.connect(process.env.MONGO_URI);
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: "envios.promesas", fromBeginning: true });
  await consumer.subscribe({ topic: "envios.estados", fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      try {
        await procesarEvento(message, topic, partition);
      } catch (error) {
        console.error("[Analítica] Error procesando evento:", error.message);
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

  iniciarVigilanteReproceso();
  console.log("[Analítica] Escuchando eventos con incrementos atómicos e idempotencia...");
}

start().catch(console.error);
