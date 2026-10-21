const mongoose = require('mongoose');

// Agregados diarios por zona y hub
const metricaDiariaSchema = new mongoose.Schema({
    fecha: { type: String, required: true }, // Formato YYYY-MM-DD
    zonaId: { type: String, required: true },
    hubId: { type: String, required: true },
    entregasExitosas: { type: Number, default: 0 },
    entregasFallidas: { type: Number, default: 0 },
    // Guardar los motivos de fallo para responder "¿Por qué falló?"
    motivosFallo: {
        AUSENTE: { type: Number, default: 0 },
        DIRECCION_INCORRECTA: { type: Number, default: 0 },
        RECHAZADO: { type: Number, default: 0 },
        ZONA_INACCESIBLE: { type: Number, default: 0 }
    }
});

// Índice único compuesto para evitar duplicados en la misma franja/día
metricaDiariaSchema.index({ fecha: 1, zonaId: 1, hubId: 1 }, { unique: true });

const MetricaDiaria = mongoose.model('MetricaDiaria', metricaDiariaSchema);

module.exports = { MetricaDiaria };