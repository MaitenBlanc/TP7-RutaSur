const mongoose = require("mongoose");

// Modelo del Envío: historial embebido y última secuencia aplicada
const envioSchema = new mongoose.Schema(
  {
    shipmentId: { type: String, required: true, unique: true },
    estado: { type: String, required: true },
    zonaId: String,
    hubId: String,
    promesa: { type: Object, default: null },
    ultimaSecuencia: { type: Number, default: 0 },
    historial: { type: Array, default: [] }, // Máximo 15 eventos esperados
  },
  { timestamps: true },
);

const Envio = mongoose.model("Envio", envioSchema);

// Registro de Idempotencia: consumidor + eventId con TTL de 7 días
const eventoProcesadoSchema = new mongoose.Schema({
  consumidor: { type: String, required: true },
  eventId: { type: String, required: true },
  createdAt: { type: Date, expires: "7d", default: Date.now },
});
// Índice compuesto único para garantizar idempotencia a nivel de BD
eventoProcesadoSchema.index({ consumidor: 1, eventId: 1 }, { unique: true });

const EventoProcesado = mongoose.model(
  "EventoProcesado",
  eventoProcesadoSchema,
);

module.exports = { Envio, EventoProcesado };
