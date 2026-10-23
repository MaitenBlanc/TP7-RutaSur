import { NextRequest, NextResponse } from "next/server";
import mongoose from "mongoose";
import { connectDB } from "../../../../lib/mongodb";

export const dynamic = "force-dynamic";

export async function GET(
  req: NextRequest,
  context: { params: Promise<{ shipmentId: string }> }
) {
  await connectDB();
  const { shipmentId } = await context.params;
  const modo = req.nextUrl.searchParams.get("modo");
  const db = mongoose.connection;

  const envio = await db.collection("envios").findOne({ shipmentId });
  if (!envio) {
    return NextResponse.json({ error: "Envío no encontrado" }, { status: 404 });
  }

  const [alertas, notificaciones] = await Promise.all([
    db
      .collection("alertas")
      .find({ shipmentId })
      .sort({ ocurridoEn: -1 })
      .toArray(),
    db
      .collection("notificaciones")
      .find({ shipmentId })
      .sort({ ocurridoEn: -1 })
      .toArray(),
  ]);

  // Vista pública de Tracking (Destinatario): solo estado actual y fecha estimada, sin datos internos
  if (modo === "publico") {
    const pasosPublicos = (envio.historial || [])
      .filter((h: any) =>
        [
          "CREADO",
          "INGRESADO_HUB",
          "CLASIFICADO",
          "EN_REPARTO",
          "ENTREGADO",
          "ENTREGA_FALLIDA",
          "REPROGRAMADO",
          "RETENIDO",
          "DEVOLUCION_COMERCIO",
        ].includes(h.tipo)
      )
      .map((h: any) => ({
        estado: h.tipo,
        fecha: h.ocurridoEn,
        motivo: h.motivo || undefined,
      }));

    return NextResponse.json({
      shipmentId: envio.shipmentId,
      estado: envio.estado,
      fechaEstimada:
        envio.reprogramacion?.proximoIntentoFecha ||
        envio.promesa?.venceEn ||
        null,
      proximaFranja: envio.reprogramacion?.proximaFranja || envio.franja || null,
      avisosDestinatario: notificaciones
        .filter((n: any) => n.tipoAviso === "FALLO_ENTREGA")
        .map((n: any) => ({
          mensaje: n.mensaje,
          opciones: n.opcionesReprogramacion || [],
          fecha: n.ocurridoEn,
        })),
      seguimiento: pasosPublicos,
      updatedAt: envio.updatedAt,
    });
  }

  return NextResponse.json({
    envio,
    alertas,
    notificaciones,
  });
}

