"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { ThemeToggle } from "@/app/components/ThemeToggle";

export default function DetalleEnvioSupervisor() {
  const params = useParams<{ shipmentId: string }>();
  const shipmentId = params?.shipmentId;

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  const fetchDetalle = useCallback(async () => {
    if (!shipmentId) return;
    try {
      const res = await fetch(`/api/envios/${encodeURIComponent(shipmentId)}`);
      if (res.ok) {
        const json = await res.json();
        setData(json);
      }
    } catch (e) {
      console.error("Error cargando detalle del envío:", e);
    } finally {
      setLoading(false);
    }
  }, [shipmentId]);

  useEffect(() => {
    fetchDetalle();

    const eventSource = new EventSource("/api/stream");
    eventSource.onmessage = () => {
      fetchDetalle();
    };
    return () => eventSource.close();
  }, [fetchDetalle]);

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-8 text-gray-800 dark:text-gray-100">
        <div className="max-w-5xl mx-auto">Cargando detalle del envío {shipmentId}...</div>
      </div>
    );
  }

  if (!data || !data.envio) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-8 text-gray-800 dark:text-gray-100">
        <div className="max-w-5xl mx-auto space-y-4">
          <Link href="/" className="text-blue-600 hover:underline text-sm">
            ← Volver a la Torre de Control
          </Link>
          <div className="bg-white dark:bg-gray-800 p-8 rounded-xl border dark:border-gray-700">
            No se encontró el envío <strong>{shipmentId}</strong>.
          </div>
        </div>
      </div>
    );
  }

  const { envio, alertas = [], notificaciones = [] } = data;

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-6 md:p-8 font-sans text-gray-800 dark:text-gray-100">
      <div className="max-w-6xl mx-auto space-y-6">
        <header className="flex flex-col md:flex-row justify-between md:items-center gap-4 border-b dark:border-gray-800 pb-4">
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-2xl font-bold font-mono text-blue-900 dark:text-blue-400">
                {envio.shipmentId}
              </h1>
              <span className="px-3 py-1 rounded-full text-xs font-bold bg-blue-100 dark:bg-blue-900/50 text-blue-800 dark:text-blue-300">
                {envio.estado}
              </span>
            </div>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Vista de Supervisor — Línea de tiempo, intentos, alertas de SLA y notificaciones en vivo
            </p>
          </div>

          <div className="flex items-center gap-3">
            <Link
              href="/"
              className="px-3 py-1.5 rounded-lg text-sm bg-white dark:bg-gray-800 border dark:border-gray-700 hover:bg-gray-100 dark:hover:bg-gray-700"
            >
              ← Torre de Control
            </Link>
            <Link
              href={`/tracking/${envio.shipmentId}`}
              className="px-3 py-1.5 rounded-lg text-sm bg-emerald-600 text-white hover:bg-emerald-700"
            >
              Ver en Tracking Público
            </Link>
            <ThemeToggle />
          </div>
        </header>

        {/* METADATOS OPERATIVOS DEL ENVÍO */}
        <section className="grid grid-cols-2 md:grid-cols-6 gap-4">
          <div className="bg-white dark:bg-gray-800 p-4 rounded-xl border dark:border-gray-700">
            <div className="text-xs text-gray-500">Hub Asignado</div>
            <div className="font-bold text-sm mt-1">{envio.hubId || "-"}</div>
          </div>
          <div className="bg-white dark:bg-gray-800 p-4 rounded-xl border dark:border-gray-700">
            <div className="text-xs text-gray-500">Zona / Franja</div>
            <div className="font-bold text-sm mt-1">
              {envio.zonaId || "-"} ({envio.franja || "MAÑANA"})
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 p-4 rounded-xl border dark:border-gray-700">
            <div className="text-xs text-gray-500">Repartidor</div>
            <div className="font-bold text-sm mt-1">{envio.repartidorId || "-"}</div>
          </div>
          <div className="bg-white dark:bg-gray-800 p-4 rounded-xl border dark:border-gray-700">
            <div className="text-xs text-gray-500">Intentos / Últ. Secuencia</div>
            <div className="font-bold text-sm mt-1">
              Intento #{envio.intentos || 1} (Seq {envio.ultimaSecuencia || 0})
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 p-4 rounded-xl border dark:border-gray-700">
            <div className="text-xs text-gray-500">Promesa Vence</div>
            <div className="font-bold text-xs mt-1">
              {envio.promesa?.venceEn
                ? new Date(envio.promesa.venceEn).toLocaleString()
                : "Sin promesa"}
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 p-4 rounded-xl border dark:border-gray-700">
            <div className="text-xs text-gray-500">Reprogramación</div>
            <div className="font-bold text-xs mt-1">
              {envio.reprogramacion?.proximoIntentoFecha
                ? `${envio.reprogramacion.proximoIntentoFecha} (${envio.reprogramacion.proximaFranja})`
                : "-"}
            </div>
          </div>
        </section>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* LÍNEA DE TIEMPO COMPLETA (HISTORIAL EMBEBIDO) */}
          <section className="bg-white dark:bg-gray-800 rounded-xl border dark:border-gray-700 shadow-sm overflow-hidden">
            <div className="p-4 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/60">
              <h2 className="font-bold text-base">
                Línea de Tiempo del Envío ({envio.historial?.length || 0} eventos)
              </h2>
              <p className="text-xs text-gray-500">
                Historial embebido con control de secuencia e idempotencia
              </p>
            </div>
            <div className="p-5 space-y-4 max-h-[500px] overflow-y-auto">
              {(envio.historial || []).map((item: any, idx: number) => (
                <div
                  key={`${item.eventId || idx}-${idx}`}
                  className="border-l-2 border-blue-500 pl-4 py-1 space-y-1 text-sm"
                >
                  <div className="flex justify-between items-center">
                    <span className="font-bold">{item.tipo}</span>
                    {item.secuencia && (
                      <span className="text-xs font-mono bg-gray-100 dark:bg-gray-700 px-2 py-0.5 rounded">
                        Secuencia #{item.secuencia}
                      </span>
                    )}
                  </div>
                  {item.motivo && (
                    <div className="text-xs text-red-600 dark:text-red-400 font-semibold">
                      Motivo de fallo: {item.motivo} (Intento #{item.intentos || 1})
                    </div>
                  )}
                  {item.detalle && (
                    <div className="text-xs text-gray-600 dark:text-gray-300">
                      {item.detalle}
                    </div>
                  )}
                  <div className="text-[11px] text-gray-400 flex flex-wrap gap-3">
                    <span>
                      ocurridoEn:{" "}
                      {item.ocurridoEn
                        ? new Date(item.ocurridoEn).toLocaleString()
                        : "-"}
                    </span>
                    {typeof item.demoraSincronizacionMs === "number" && (
                      <span>Demora sync: {item.demoraSincronizacionMs} ms</span>
                    )}
                    {item.eventId && <span className="font-mono">id: {item.eventId}</span>}
                  </div>
                </div>
              ))}
            </div>
          </section>

          {/* ALERTAS DE SLA Y NOTIFICACIONES SIMULADAS */}
          <div className="space-y-6">
            <section className="bg-white dark:bg-gray-800 rounded-xl border dark:border-gray-700 shadow-sm overflow-hidden">
              <div className="p-4 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/60">
                <h2 className="font-bold text-base">
                  Alertas de SLA del Envío ({alertas.length})
                </h2>
              </div>
              <div className="p-4 divide-y dark:divide-gray-700">
                {alertas.length === 0 ? (
                  <p className="text-sm text-gray-500 py-4 text-center">
                    Este envío no registra alertas de SLA.
                  </p>
                ) : (
                  alertas.map((a: any) => (
                    <div key={a._id} className="py-3 flex justify-between items-center text-sm">
                      <div>
                        <div className="font-bold">
                          {a.causa}{" "}
                          <span className="text-xs font-normal text-gray-500">
                            (Prioridad: {a.prioridad})
                          </span>
                        </div>
                        <div className="text-xs text-gray-500">
                          Abierta: {a.ocurridoEn ? new Date(a.ocurridoEn).toLocaleString() : "-"}
                          {a.cerradaEn &&
                            ` · Cerrada: ${new Date(a.cerradaEn).toLocaleString()}`}
                        </div>
                      </div>
                      <span
                        className={`px-2.5 py-0.5 rounded-full text-xs font-bold ${
                          a.estado === "ABIERTA"
                            ? "bg-red-100 text-red-800 dark:bg-red-900/50 dark:text-red-200"
                            : "bg-green-100 text-green-800 dark:bg-green-900/50 dark:text-green-200"
                        }`}
                      >
                        {a.estado}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </section>

            <section className="bg-white dark:bg-gray-800 rounded-xl border dark:border-gray-700 shadow-sm overflow-hidden">
              <div className="p-4 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/60">
                <h2 className="font-bold text-base">
                  Notificaciones al Destinatario ({notificaciones.length})
                </h2>
              </div>
              <div className="p-4 divide-y dark:divide-gray-700">
                {notificaciones.length === 0 ? (
                  <p className="text-sm text-gray-500 py-4 text-center">
                    No se han emitido notificaciones para este envío.
                  </p>
                ) : (
                  notificaciones.map((n: any) => (
                    <div key={n._id} className="py-3 space-y-1.5 text-sm">
                      <div className="flex justify-between">
                        <span className="font-bold text-amber-600 dark:text-amber-400">
                          {n.tipoAviso}
                        </span>
                        <span className="text-xs text-gray-400">
                          {n.ocurridoEn ? new Date(n.ocurridoEn).toLocaleString() : ""}
                        </span>
                      </div>
                      <p className="text-xs text-gray-700 dark:text-gray-300">{n.mensaje}</p>
                      {Array.isArray(n.opcionesReprogramacion) &&
                        n.opcionesReprogramacion.length > 0 && (
                          <div className="flex flex-wrap gap-1.5 pt-1">
                            {n.opcionesReprogramacion.map((op: string) => (
                              <span
                                key={op}
                                className="text-[11px] bg-blue-50 dark:bg-blue-950/60 text-blue-700 dark:text-blue-300 border border-blue-200 dark:border-blue-800 px-2 py-0.5 rounded"
                              >
                                {op}
                              </span>
                            ))}
                          </div>
                        )}
                    </div>
                  ))
                )}
              </div>
            </section>
          </div>
        </div>
      </div>
    </div>
  );
}

