const mongoose = require('mongoose'); // usado para que el monitor guarde un respaldo de vencimientos en la DB

// Estado interno del Monitor (Tabla temporal)
const vencimientoSchema = new mongoose.Schema({
    shipmentId: { type: String, required: true, unique: true },
    ultimoEstado: { type: String, required: true },
    venceEn: { type: Date, required: true },
    alertasEmitidas: { type: Array, default: [] } // Evita repetir la misma alerta
});

const Vencimiento = mongoose.model('Vencimiento', vencimientoSchema);

module.exports = { Vencimiento };