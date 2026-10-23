const mongoose = require("mongoose");

// Estado interno respaldado en MongoDB del Monitor de SLA
const vencimientoSchema = new mongoose.Schema(
  {
    shipmentId: { type: String, required: true, unique: true },
    ultimoEstado: { type: String, default: "CREADO" },
    venceEn: { type: Date, default: null },
    ingresadoHubEn: { type: Date, default: null }, // Extensión: Paquetes dormidos en hub
    ultimoOcurridoEn: { type: Date, default: Date.now },
    alertasEmitidas: { type: [String], default: [] }, // Evita repetir la misma regla sobre el mismo envío
  },
  { timestamps: true, collection: "vencimientos_sla" }
);

const Vencimiento = mongoose.model("Vencimiento", vencimientoSchema);

// Registro de idempotencia compartido en eventos_procesados
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

module.exports = { Vencimiento, EventoProcesado };
