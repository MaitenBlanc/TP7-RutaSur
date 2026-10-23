const mongoose = require("mongoose");

// Colección envios: Un documento por envío
const envioSchema = new mongoose.Schema(
  {
    shipmentId: { type: String, required: true, unique: true },
    estado: { type: String, required: true },
    zonaId: { type: String, default: null },
    hubId: { type: String, default: null },
    repartidorId: { type: String, default: null },
    franja: { type: String, default: "MAÑANA" },
    intentos: { type: Number, default: 1 },
    promesa: {
      venceEn: { type: Date, default: null },
      ocurridoEn: { type: Date, default: null },
    },
    reprogramacion: {
      proximoIntentoFecha: { type: String, default: null },
      proximaFranja: { type: String, default: null },
      motivo: { type: String, default: null },
    },
    ultimaSecuencia: { type: Number, default: 0 },
    historial: { type: Array, default: [] }, // Máximo 15 eventos esperados
  },
  { timestamps: true, collection: "envios" }
);

// Índices requeridos por la especificación:
// shipmentId único; estado + zonaId; promesa.venceEn
envioSchema.index({ estado: 1, zonaId: 1 });
envioSchema.index({ "promesa.venceEn": 1 });

const Envio = mongoose.model("Envio", envioSchema);

// Colección alertas: Alertas de SLA abiertas y cerradas, con causa y prioridad
const alertaSchema = new mongoose.Schema(
  {
    eventId: { type: String, required: true },
    shipmentId: { type: String, required: true },
    causa: { type: String, required: true }, // Sin ingreso | Sin salida | Fallo en el día límite | Vencido
    prioridad: { type: String, required: true }, // Media | Alta | Crítica
    PrioridadOrden: { type: Number, default: 2 }, // 1: Crítica, 2: Alta, 3: Media
    estado: { type: String, enum: ["ABIERTA", "CERRADA"], default: "ABIERTA" },
    ocurridoEn: { type: Date, default: Date.now },
    cerradaEn: { type: Date, default: null },
  },
  { timestamps: true, collection: "alertas" }
);

// Índices requeridos: estado + prioridad; shipmentId
alertaSchema.index({ estado: 1, prioridad: 1 });
alertaSchema.index({ shipmentId: 1 });

const Alerta = mongoose.model("Alerta", alertaSchema);

// Colección notificaciones: Registro de cada notificación simulada
const notificacionSchema = new mongoose.Schema(
  {
    eventId: { type: String, required: true },
    shipmentId: { type: String, required: true },
    tipoAviso: { type: String, required: true },
    mensaje: { type: String, required: true },
    opcionesReprogramacion: { type: Array, default: [] },
    ocurridoEn: { type: Date, default: Date.now },
  },
  { timestamps: true, collection: "notificaciones" }
);

// Índice requerido: shipmentId
notificacionSchema.index({ shipmentId: 1 });

const Notificacion = mongoose.model("Notificacion", notificacionSchema);

// Colección eventos_procesados: eventId ya aplicados por cada consumidor con TTL 7 días
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

module.exports = { Envio, Alerta, Notificacion, EventoProcesado };
