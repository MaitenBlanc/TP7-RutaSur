const mongoose = require("mongoose");

// Colección metricas_diarias: Agregados por día, zona, hub y franja horaria
const metricaDiariaSchema = new mongoose.Schema(
  {
    fecha: { type: String, required: true }, // Formato YYYY-MM-DD
    zonaId: { type: String, required: true },
    franja: { type: String, required: true, default: "MAÑANA" }, // MAÑANA | TARDE
    hubId: { type: String, required: true },
    // Indicador 1: Tasa de entrega en primer intento (Zona, franja horaria, día)
    primerIntentoExitosos: { type: Number, default: 0 },
    primerIntentoFallidos: { type: Number, default: 0 },
    entregasExitosas: { type: Number, default: 0 },
    entregasFallidas: { type: Number, default: 0 },
    // Indicador 2: Distribución de motivos de fallo (Zona, motivo)
    motivosFallo: {
      AUSENTE: { type: Number, default: 0 },
      DIRECCION_INCORRECTA: { type: Number, default: 0 },
      RECHAZADO: { type: Number, default: 0 },
      ZONA_INACCESIBLE: { type: Number, default: 0 },
    },
    // Indicador 3: Cumplimiento de SLA (Hub, día)
    entregasEnSla: { type: Number, default: 0 },
    entregasFueraSla: { type: Number, default: 0 },
  },
  { timestamps: true, collection: "metricas_diarias" }
);

// Índice único: fecha + zonaId + franja único
metricaDiariaSchema.index({ fecha: 1, zonaId: 1, franja: 1 }, { unique: true });

const MetricaDiaria = mongoose.model("MetricaDiaria", metricaDiariaSchema);

// Colección para el 4to indicador (Reprocesamiento histórico: tiempo medio entre INGRESADO_HUB y EN_REPARTO por hub)
const metricaHubReprocesoSchema = new mongoose.Schema(
  {
    hubId: { type: String, required: true, unique: true },
    sumaMinutosHubAReparto: { type: Number, default: 0 },
    cantidadEnviosCalculados: { type: Number, default: 0 },
    promedioMinutos: { type: Number, default: 0 },
    consumerGroupUsado: { type: String, default: "analitica-reproceso-hub" },
    eventosHistoricosLeidos: { type: Number, default: 0 },
    actualizadoEn: { type: Date, default: Date.now },
  },
  { timestamps: true, collection: "metricas_reproceso_hub" }
);

const MetricaHubReproceso = mongoose.model("MetricaHubReproceso", metricaHubReprocesoSchema);

// Estado de control para disparar reprocesamiento de nuevo grupo sin reiniciar contenedores
const controlReprocesoSchema = new mongoose.Schema(
  {
    clave: { type: String, required: true, unique: true },
    solicitado: { type: Boolean, default: false },
    estado: { type: String, default: "LISTO" }, // EN_CURSO | COMPLETADO | LISTO
    groupId: { type: String, default: "" },
    eventosLeidos: { type: Number, default: 0 },
    solicitadoEn: { type: Date, default: Date.now },
  },
  { collection: "control_reproceso" }
);

const ControlReproceso = mongoose.model("ControlReproceso", controlReprocesoSchema);

// Colección eventos_procesados
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

module.exports = {
  MetricaDiaria,
  MetricaHubReproceso,
  ControlReproceso,
  EventoProcesado,
};
