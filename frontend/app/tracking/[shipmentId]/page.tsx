"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { ThemeToggle } from "@/app/components/ThemeToggle";

const PASOS_TRACKING = [
  { codigo: "CREADO", etiqueta: "Orden registrada por el comercio" },
  { codigo: "INGRESADO_HUB", etiqueta: "En centro de procesamiento" },
  { codigo: "CLASIFICADO", etiqueta: "Preparado para despacho" },
  { codigo: "EN_REPARTO", etiqueta: "En camino a tu domicilio" },
  { codigo: "ENTREGADO", etiqueta: "Entregado" },
];

function etiquetaAmigableEstado(estado: string) {
  switch (estado) {
    case "CREADO":
      return "Paquete registrado";
    case "INGRESADO_HUB":
    case "CLASIFICADO":
      return "En centro de distribución";
    case "EN_REPARTO":
      return "En camino a tu domicilio";
    case "ENTREGADO":
      return "¡Entregado con éxito!";
    case "ENTREGA_FALLIDA":
      return "No pudimos concretar la visita";
    case "REPROGRAMADO":
      return "Visita reprogramada para el próximo día hábil";
    case "RETENIDO":
      return "Retenido: requiere confirmación de domicilio";
    case "DEVOLUCION_COMERCIO":
      return "En devolución al remitente";
    default:
      return estado;
  }
}

export default function TrackingPublicoDetalle() {
  const params = useParams<{ shipmentId: string }>();
  const shipmentId = params?.shipmentId;

  const [info, setInfo] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  const fetchTracking = useCallback(async () => {
    if (!shipmentId) return;
    try {
      const res = await fetch(
        `/api/envios/${encodeURIComponent(shipmentId)}?modo=publico`
      );
      if (res.ok) {
        const json = await res.json();
        setInfo(json);
      }
    } catch (e) {
      console.error("Error cargando tracking público:", e);
    } finally {
      setLoading(false);
    }
  }, [shipmentId]);

  useEffect(() => {
    fetchTracking();

    const eventSource = new EventSource("/api/stream");
    eventSource.onmessage = () => {
      fetchTracking();
    };
    return () => eventSource.close();
  }, [fetchTracking]);

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-8 text-gray-800 dark:text-gray-100">
        <div className="max-w-3xl mx-auto">Consultando seguimiento de {shipmentId}...</div>
      </div>
    );
  }

  if (!info) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-8 text-gray-800 dark:text-gray-100">
        <div className="max-w-3xl mx-auto space-y-4">
          <Link href="/tracking" className="text-emerald-600 hover:underline text-sm">
            ← Buscar otro código
          </Link>
          <div className="bg-white dark:bg-gray-800 p-8 rounded-xl border dark:border-gray-700">
            No encontramos un envío con el código <strong>{shipmentId}</strong>.
          </div>
        </div>
      </div>
    );
  }

  const estadosAlcanzados = new Set(
    (info.seguimiento || []).map((s: any) => s.estado)
  );
  estadosAlcanzados.add(info.estado);

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-6 md:p-8 font-sans text-gray-800 dark:text-gray-100">
      <div className="max-w-3xl mx-auto space-y-6">
        <header className="flex justify-between items-center border-b dark:border-gray-800 pb-4">
          <div>
            <span className="text-xs font-bold uppercase tracking-wider text-emerald-600 dark:text-emerald-400">
              Tracking Público · Destinatario
            </span>
            <h1 className="text-2xl font-bold font-mono mt-0.5">{info.shipmentId}</h1>
          </div>
          <div className="flex items-center gap-3">
            <Link
              href="/tracking"
              className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-white dark:bg-gray-800 border dark:border-gray-700"
            >
              Buscar otro
            </Link>
            <Link
              href="/"
              className="px-3 py-1.5 rounded-lg text-xs font-semibold text-blue-600 dark:text-blue-400 hover:underline"
            >
              Torre de Control
            </Link>
            <ThemeToggle />
          </div>
        </header>

        {/* ESTADO ACTUAL Y FECHA ESTIMADA (Sin datos internos) */}
        <section className="bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border dark:border-gray-700 space-y-5">
          <div className="flex flex-col sm:flex-row justify-between sm:items-center gap-4 border-b dark:border-gray-700 pb-4">
            <div>
              <div className="text-xs text-gray-500">Estado actual de tu envío</div>
              <div className="text-2xl font-bold text-emerald-700 dark:text-emerald-400 mt-1">
                {etiquetaAmigableEstado(info.estado)}
              </div>
              <div className="text-xs font-mono text-gray-400 mt-0.5">
                Código de estado: {info.estado}
              </div>
            </div>

            <div className="bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-800 p-3.5 rounded-xl text-right">
              <div className="text-xs text-emerald-800 dark:text-emerald-300">
                Fecha estimada de entrega
              </div>
              <div className="text-sm font-bold text-emerald-900 dark:text-emerald-200 mt-0.5">
                {info.fechaEstimada
                  ? new Date(info.fechaEstimada).toLocaleString()
                  : "En cálculo"}
              </div>
              {info.proximaFranja && (
                <div className="text-xs text-emerald-700 dark:text-emerald-400">
                  Franja horaria: {info.proximaFranja}
                </div>
              )}
            </div>
          </div>

          {/* Barra de progreso */}
          <div className="grid grid-cols-1 sm:grid-cols-5 gap-2 pt-2">
            {PASOS_TRACKING.map((paso, index) => {
              const completado = estadosAlcanzados.has(paso.codigo);
              const esActual = info.estado === paso.codigo;
              return (
                <div
                  key={paso.codigo}
                  className={`p-3 rounded-lg border text-xs transition ${
                    esActual
                      ? "border-emerald-500 bg-emerald-50 dark:bg-emerald-950/50 font-bold"
                      : completado
                        ? "border-emerald-200 dark:border-emerald-800 bg-white dark:bg-gray-800"
                        : "border-gray-200 dark:border-gray-700 opacity-50"
                  }`}
                >
                  <div className="text-[11px] text-gray-400">Paso {index + 1}</div>
                  <div className="mt-1">{paso.etiqueta}</div>
                </div>
              );
            })}
          </div>
        </section>

        {/* AVISOS AL DESTINATARIO (Si hubo intento fallido) */}
        {Array.isArray(info.avisosDestinatario) && info.avisosDestinatario.length > 0 && (
          <section className="bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 p-5 rounded-xl space-y-3">
            <h2 className="text-sm font-bold text-amber-900 dark:text-amber-300">
              Avisos sobre tu entrega
            </h2>
            {info.avisosDestinatario.map((av: any, idx: number) => (
              <div key={idx} className="text-xs space-y-2">
                <p className="font-medium text-amber-900 dark:text-amber-200">
                  {av.mensaje}
                </p>
                {Array.isArray(av.opciones) && av.opciones.length > 0 && (
                  <div className="flex flex-wrap gap-2">
                    {av.opciones.map((op: string) => (
                      <span
                        key={op}
                        className="px-2.5 py-1 rounded bg-white dark:bg-gray-800 border border-amber-300 dark:border-amber-700 font-semibold text-amber-800 dark:text-amber-300"
                      >
                        {op}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </section>
        )}

        {/* DETALLE DE MOVIMIENTOS PÚBLICOS */}
        <section className="bg-white dark:bg-gray-800 rounded-xl border dark:border-gray-700 shadow-sm overflow-hidden">
          <div className="p-4 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/60">
            <h2 className="text-sm font-bold">Historial de movimientos</h2>
          </div>
          <div className="p-4 divide-y dark:divide-gray-700">
            {(info.seguimiento || []).map((mov: any, idx: number) => (
              <div key={idx} className="py-2.5 flex justify-between items-center text-sm">
                <div>
                  <span className="font-semibold">
                    {etiquetaAmigableEstado(mov.estado)}
                  </span>
                  {mov.motivo && (
                    <span className="ml-2 text-xs text-red-600 dark:text-red-400">
                      ({mov.motivo.replace("_", " ")})
                    </span>
                  )}
                </div>
                <span className="text-xs text-gray-500">
                  {mov.fecha ? new Date(mov.fecha).toLocaleString() : ""}
                </span>
              </div>
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}

