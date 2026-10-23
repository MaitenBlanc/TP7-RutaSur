import { NextRequest, NextResponse } from "next/server";
import mongoose from "mongoose";
import { connectDB } from "../../../lib/mongodb";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  await connectDB();
  const db = mongoose.connection;

  const zonaId = req.nextUrl.searchParams.get("zonaId");
  const hubId = req.nextUrl.searchParams.get("hubId");
  const fecha = req.nextUrl.searchParams.get("fecha");

  const filtro: Record<string, string> = {};
  if (zonaId && zonaId !== "TODAS") filtro.zonaId = zonaId;
  if (hubId && hubId !== "TODOS") filtro.hubId = hubId;
  if (fecha && fecha !== "TODAS") filtro.fecha = fecha;

  const [todasLasMetricas, metricasFiltradas, cuartoIndicador, controlReproceso] =
    await Promise.all([
      db.collection("metricas_diarias").find().toArray(),
      db
        .collection("metricas_diarias")
        .find(filtro)
        .sort({ fecha: -1, zonaId: 1, franja: 1 })
        .toArray(),
      db
        .collection("metricas_reproceso_hub")
        .find()
        .sort({ hubId: 1 })
        .toArray(),
      db.collection("control_reproceso").findOne({ clave: "cuarto-indicador" }),
    ]);

  const zonasDisponibles = Array.from(
    new Set(todasLasMetricas.map((m: any) => m.zonaId).filter(Boolean))
  ).sort();
  const hubsDisponibles = Array.from(
    new Set(todasLasMetricas.map((m: any) => m.hubId).filter(Boolean))
  ).sort();
  const fechasDisponibles = Array.from(
    new Set(todasLasMetricas.map((m: any) => m.fecha).filter(Boolean))
  )
    .sort()
    .reverse();

  return NextResponse.json({
    metricas: metricasFiltradas,
    filtrosDisponibles: {
      zonas: zonasDisponibles,
      hubs: hubsDisponibles,
      fechas: fechasDisponibles,
    },
    cuartoIndicador: {
      datosPorHub: cuartoIndicador,
      control: controlReproceso || {
        estado: "NO_ACTIVADO",
        groupId: null,
        eventosLeidos: 0,
      },
    },
  });
}

// POST /api/analitica:
// - accion = "activar-cuarto-indicador": dispara el reprocesamiento histórico en el servicio de analítica
// - accion = "limpiar-metricas": limpia metricas_diarias antes de una corrida con semilla fija
export async function POST(req: NextRequest) {
  await connectDB();
  const db = mongoose.connection;
  const body = await req.json().catch(() => ({}));
  const accion = body.accion || "activar-cuarto-indicador";

  if (accion === "limpiar-metricas") {
    await db.collection("metricas_diarias").deleteMany({});
    await db.collection("eventos_procesados").deleteMany({ consumidor: "analitica" });
    return NextResponse.json({
      message: "Métricas diarias reiniciadas para verificación limpia con semilla fija.",
    });
  }

  const nuevoGroupId = `analitica-replay-${Date.now().toString().slice(-5)}`;
  await db.collection("control_reproceso").updateOne(
    { clave: "cuarto-indicador" },
    {
      $set: {
        solicitado: true,
        estado: "SOLICITADO",
        groupId: nuevoGroupId,
        solicitadoEn: new Date(),
      },
    },
    { upsert: true }
  );

  return NextResponse.json({
    message: `Reprocesamiento histórico activado con nuevo grupo de consumidores: ${nuevoGroupId}`,
    groupId: nuevoGroupId,
  });
}
