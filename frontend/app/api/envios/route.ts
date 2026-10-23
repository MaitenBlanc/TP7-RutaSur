import { NextResponse } from "next/server";
import mongoose from "mongoose";
import { connectDB } from "../../../lib/mongodb";

export const dynamic = "force-dynamic";

export async function GET() {
  await connectDB();
  const db = mongoose.connection;

  const [envios, conteoAgregado, alertasAbiertas, totalEnvios] = await Promise.all([
    db
      .collection("envios")
      .find()
      .sort({ updatedAt: -1 })
      .limit(25)
      .toArray(),
    db
      .collection("envios")
      .aggregate([{ $group: { _id: "$estado", cantidad: { $sum: 1 } } }])
      .toArray(),
    db
      .collection("alertas")
      .find({ estado: "ABIERTA" })
      .sort({ PrioridadOrden: 1, ocurridoEn: -1 })
      .limit(30)
      .toArray(),
    db.collection("envios").countDocuments(),
  ]);

  const contadoresPorEstado: Record<string, number> = {
    CREADO: 0,
    INGRESADO_HUB: 0,
    CLASIFICADO: 0,
    EN_REPARTO: 0,
    ENTREGADO: 0,
    ENTREGA_FALLIDA: 0,
    REPROGRAMADO: 0,
    RETENIDO: 0,
    DEVOLUCION_COMERCIO: 0,
  };

  for (const item of conteoAgregado) {
    if (item._id) {
      contadoresPorEstado[item._id] = item.cantidad;
    }
  }

  // Extraer los últimos eventos de los envíos recientes para el feed de la Torre de Control
  const ultimosEventos = envios
    .flatMap((env: any) =>
      (env.historial || []).map((ev: any) => ({
        shipmentId: env.shipmentId,
        zonaId: env.zonaId,
        hubId: env.hubId,
        ...ev,
      }))
    )
    .sort(
      (a: any, b: any) =>
        new Date(b.ocurridoEn || 0).getTime() - new Date(a.ocurridoEn || 0).getTime()
    )
    .slice(0, 15);

  return NextResponse.json({
    totalEnvios,
    contadoresPorEstado,
    alertas: alertasAbiertas,
    ultimosEventos,
    envios,
  });
}

// Endpoint para probar la reconstrucción de la vista: borra la colección envios
// El proyector detecta la colección vacía, limpia su idempotencia y rebobina Kafka al offset 0
export async function DELETE() {
  await connectDB();
  const db = mongoose.connection;
  const previos = await db.collection("envios").countDocuments();
  await db.collection("envios").deleteMany({});
  return NextResponse.json({
    message: `Se eliminaron ${previos} documentos de 'envios'. El Proyector está reconstruyendo la colección leyendo Kafka desde el inicio.`,
    eliminados: previos,
  });
}
