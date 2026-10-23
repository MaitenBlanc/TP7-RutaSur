"use client";

import {
  faBolt,
  faCircleCheck,
  faCircleExclamation,
  faCircleXmark,
  faClock,
  faPlay,
  faRotate,
  faShuffle,
  faStop,
  faTriangleExclamation,
} from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { ThemeToggle } from "./components/ThemeToggle";

const SIMULADOR_BASE =
  process.env.NEXT_PUBLIC_SIMULADOR_URL || "http://localhost:4000";

const ESTADOS_ORDEN = [
  "CREADO",
  "INGRESADO_HUB",
  "CLASIFICADO",
  "EN_REPARTO",
  "ENTREGADO",
  "ENTREGA_FALLIDA",
  "REPROGRAMADO",
  "RETENIDO",
  "DEVOLUCION_COMERCIO",
];

function badgeColorEstado(estado: string) {
  switch (estado) {
    case "ENTREGADO":
      return "bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-300 border-green-300 dark:border-green-800";
    case "ENTREGA_FALLIDA":
    case "DEVOLUCION_COMERCIO":
      return "bg-red-100 dark:bg-red-900/40 text-red-800 dark:text-red-300 border-red-300 dark:border-red-800";
    case "REPROGRAMADO":
    case "RETENIDO":
      return "bg-amber-100 dark:bg-amber-900/40 text-amber-800 dark:text-amber-300 border-amber-300 dark:border-amber-800";
    case "EN_REPARTO":
      return "bg-indigo-100 dark:bg-indigo-900/40 text-indigo-800 dark:text-indigo-300 border-indigo-300 dark:border-indigo-800";
    default:
      return "bg-blue-100 dark:bg-blue-900/40 text-blue-800 dark:text-blue-300 border-blue-300 dark:border-blue-800";
  }
}

function badgePrioridad(prioridad: string) {
  switch (prioridad) {
    case "Crítica":
      return "bg-red-600 text-white";
    case "Alta":
      return "bg-orange-500 text-white";
    default:
      return "bg-yellow-500 text-gray-900";
  }
}

export default function TorreDeControl() {
  const [envios, setEnvios] = useState<any[]>([]);
  const [contadores, setContadores] = useState<Record<string, number>>({});
  const [alertas, setAlertas] = useState<any[]>([]);
  const [ultimosEventos, setUltimosEventos] = useState<any[]>([]);
  const [totalEnvios, setTotalEnvios] = useState<number>(0);

  const [estadoSimulador, setEstadoSimulador] = useState<any>(null);
  const [feedbackDemo, setFeedbackDemo] = useState<string | null>(null);
  const [ultimoShipmentCreado, setUltimoShipmentCreado] = useState<string | null>(null);

  const fetchTorreData = useCallback(async () => {
    try {
      const res = await fetch("/api/envios");
      const data = await res.json();
      if (Array.isArray(data)) {
        setEnvios(data);
      } else {
        setEnvios(data.envios || []);
        setContadores(data.contadoresPorEstado || {});
        setAlertas(data.alertas || []);
        setUltimosEventos(data.ultimosEventos || []);
        setTotalEnvios(data.totalEnvios || 0);
      }
    } catch (error) {
      console.error("Error cargando datos de Torre de Control:", error);
    }
  }, []);

  const fetchEstadoSimulador = useCallback(async () => {
    try {
      const res = await fetch(`${SIMULADOR_BASE}/api/estado`);
      if (res.ok) {
        const data = await res.json();
        setEstadoSimulador(data);
      }
    } catch {
      // El simulador puede tardar unos segundos al iniciar
    }
  }, []);

  useEffect(() => {
    fetchTorreData();
    fetchEstadoSimulador();

    const eventSource = new EventSource("/api/stream");
    eventSource.onmessage = () => {
      fetchTorreData();
    };

    const intervalLag = setInterval(fetchEstadoSimulador, 3000);

    return () => {
      eventSource.close();
      clearInterval(intervalLag);
    };
  }, [fetchTorreData, fetchEstadoSimulador]);

  const dispararEscenario = async (escenario: string) => {
    try {
      setFeedbackDemo(`Disparando escenario: ${escenario}...`);
      const res = await fetch(`${SIMULADOR_BASE}/api/escenarios/${escenario}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      const data = await res.json();
      setFeedbackDemo(data.message || `Escenario ${escenario} ejecutado`);
      if (data.shipmentId) {
        setUltimoShipmentCreado(data.shipmentId);
      }
      setTimeout(fetchTorreData, 400);
      setTimeout(fetchEstadoSimulador, 600);
    } catch {
      setFeedbackDemo("Error conectando al simulador en el puerto 4000.");
    }
  };

  const toggleFlujoContinuo = async () => {
    const nuevoEstado = !(estadoSimulador?.config?.flujoContinuoActivo ?? true);
    try {
      const res = await fetch(`${SIMULADOR_BASE}/api/flujo`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ activo: nuevoEstado }),
      });
      const data = await res.json();
      setFeedbackDemo(data.message);
      fetchEstadoSimulador();
    } catch {
      setFeedbackDemo("No se pudo cambiar el flujo continuo.");
    }
  };

  const reconstruirEnvios = async () => {
    try {
      setFeedbackDemo("Borrando colección 'envios' para reconstruir desde Kafka...");
      const res = await fetch("/api/envios", { method: "DELETE" });
      const data = await res.json();
      setFeedbackDemo(data.message);
      setUltimoShipmentCreado(null);
      fetchTorreData();
    } catch {
      setFeedbackDemo("Error al solicitar la reconstrucción de envíos.");
    }
  };

  const flujoActivo = estadoSimulador?.config?.flujoContinuoActivo ?? true;

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-6 md:p-8 font-sans text-gray-800 dark:text-gray-100 transition-colors duration-300">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* ENCABEZADO Y NAVEGACIÓN ENTRE LAS 4 PANTALLAS DE PRODUCTO */}
        <header className="flex flex-col md:flex-row justify-between md:items-center gap-4 border-b dark:border-gray-800 pb-4">
          <div>
            <h1 className="text-3xl font-bold text-blue-900 dark:text-blue-400">
              RutaSur Logística
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Torre de Control del Supervisor — Seguimiento Orientado a Eventos (Kafka + MongoDB + SSE)
            </p>
          </div>

          <div className="flex flex-wrap gap-3 items-center">
            <Link
              href="/"
              className="px-3 py-1.5 rounded-lg text-sm font-semibold bg-blue-600 text-white shadow-sm"
            >
              Torre de Control
            </Link>
            <Link
              href="/tracking"
              className="px-3 py-1.5 rounded-lg text-sm font-medium bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 hover:bg-gray-100 dark:hover:bg-gray-700 transition"
            >
              Tracking Público
            </Link>
            <Link
              href="/analitica"
              className="px-3 py-1.5 rounded-lg text-sm font-medium bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 hover:bg-gray-100 dark:hover:bg-gray-700 transition"
            >
              Panel Analítico
            </Link>

            <ThemeToggle />

            <span className="flex items-center text-xs font-medium text-green-700 dark:text-green-400 bg-green-100 dark:bg-green-900/30 px-3 py-1.5 rounded-full border border-green-200 dark:border-green-800">
              <span className="w-2 h-2 bg-green-500 dark:bg-green-400 rounded-full animate-pulse mr-2"></span>
              SSE + Change Streams
            </span>
          </div>
        </header>

        {/* PANEL DE DEMO */}
        <section className="bg-white dark:bg-gray-800 p-5 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 space-y-4">
          <div className="flex flex-col md:flex-row justify-between md:items-center gap-2 border-b dark:border-gray-700 pb-3">
            <div>
              <h2 className="text-base font-bold uppercase tracking-wider text-gray-700 dark:text-gray-200">
                Panel de Demo (Simulador de Eventos y Resiliencia)
              </h2>
              <p className="text-xs text-gray-500 dark:text-gray-400">
                Dispara los escenarios del guion de demo o controla el flujo continuo ({estadoSimulador?.config?.enviosPorMinuto || 20} envíos/min, aceleración 1h = 30s).
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              {estadoSimulador?.horaSimuladaActual && (
                <span className="text-xs font-mono bg-gray-100 dark:bg-gray-900 px-3 py-1.5 rounded border dark:border-gray-700">
                  Reloj Simulado:{" "}
                  <strong>
                    {new Date(estadoSimulador.horaSimuladaActual).toLocaleString()}
                  </strong>
                </span>
              )}

              <button
                onClick={toggleFlujoContinuo}
                className={`px-3 py-1.5 rounded-lg text-xs font-semibold flex items-center gap-2 transition ${
                  flujoActivo
                    ? "bg-amber-500 hover:bg-amber-600 text-white"
                    : "bg-emerald-600 hover:bg-emerald-700 text-white"
                }`}
              >
                <FontAwesomeIcon icon={flujoActivo ? faStop : faPlay} />
                {flujoActivo ? "Pausar Flujo Continuo" : "Reanudar Flujo Continuo"}
              </button>
            </div>
          </div>

          {/* Botones principales de escenarios */}
          <div className="flex flex-wrap gap-2.5">
            <button
              onClick={() => dispararEscenario("feliz")}
              className="bg-blue-600 text-white text-sm px-3.5 py-2 rounded-lg shadow-sm hover:bg-blue-700 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faCircleCheck} />
              1. Envío Feliz
            </button>

            <button
              onClick={() => dispararEscenario("ausente")}
              className="bg-orange-500 text-white text-sm px-3.5 py-2 rounded-lg shadow-sm hover:bg-orange-600 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faCircleXmark} />
              2. Ausente (3 Reacciones + Reintento)
            </button>

            <button
              onClick={() => dispararEscenario("demorado")}
              className="bg-red-600 text-white text-sm px-3.5 py-2 rounded-lg shadow-sm hover:bg-red-700 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faClock} />
              3. Demorado (Alerta SLA)
            </button>

            <button
              onClick={() => dispararEscenario("tercer-fallo")}
              className="bg-rose-700 text-white text-sm px-3.5 py-2 rounded-lg shadow-sm hover:bg-rose-800 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faCircleExclamation} />
              4. Tercer Fallo (Devolución)
            </button>

            <button
              onClick={() => dispararEscenario("rafaga")}
              className="bg-purple-600 text-white text-sm px-3.5 py-2 rounded-lg shadow-sm hover:bg-purple-700 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faBolt} />
              5. Ráfaga (500 envíos)
            </button>

            <button
              onClick={() => dispararEscenario("malformado")}
              className="bg-gray-700 text-white text-sm px-3.5 py-2 rounded-lg shadow-sm hover:bg-gray-800 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faTriangleExclamation} />
              6. Inyectar Malformado (DLQ)
            </button>

            <button
              onClick={() => dispararEscenario("duplicado")}
              className="bg-teal-600 text-white text-sm px-3.5 py-2 rounded-lg shadow-sm hover:bg-teal-700 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faRotate} />
              7. Inyectar Duplicado
            </button>

            <button
              onClick={() => dispararEscenario("fuera-de-orden")}
              className="bg-cyan-700 text-white text-sm px-3.5 py-2 rounded-lg shadow-sm hover:bg-cyan-800 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faShuffle} />
              8. Fuera de Orden
            </button>

            <button
              onClick={reconstruirEnvios}
              className="bg-indigo-600 text-white text-sm px-3.5 py-2 rounded-lg shadow-sm hover:bg-indigo-700 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faRotate} />
              9. Borrar envíos y Reprocesar
            </button>
          </div>

          {/* Feedback del escenario disparado y Lag por grupo de consumidores (Observabilidad) */}
          <div className="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-3 pt-2 text-xs">
            <div className="flex flex-wrap items-center gap-2">
              {feedbackDemo && (
                <span className="bg-blue-50 dark:bg-blue-950/60 text-blue-800 dark:text-blue-300 border border-blue-200 dark:border-blue-800 px-3 py-1.5 rounded-lg font-medium">
                  {feedbackDemo}
                </span>
              )}
              {ultimoShipmentCreado && (
                <div className="flex items-center gap-2">
                  <Link
                    href={`/envios/${ultimoShipmentCreado}`}
                    className="underline font-semibold text-blue-600 dark:text-blue-400"
                  >
                    Ver Detalle de {ultimoShipmentCreado} →
                  </Link>
                  <Link
                    href={`/tracking/${ultimoShipmentCreado}`}
                    className="underline font-semibold text-emerald-600 dark:text-emerald-400"
                  >
                    Ver en Tracking Público →
                  </Link>
                </div>
              )}
            </div>

            {estadoSimulador?.lagPorGrupo && (
              <div className="flex flex-wrap items-center gap-2 bg-gray-50 dark:bg-gray-900/70 px-3 py-1.5 rounded-lg border dark:border-gray-700">
                <span className="font-semibold text-gray-600 dark:text-gray-400">
                  Lag Kafka por Grupo:
                </span>
                {Object.entries(estadoSimulador.lagPorGrupo).map(([grupo, lag]) => (
                  <span
                    key={grupo}
                    className={`px-2 py-0.5 rounded font-mono ${
                      Number(lag) > 0
                        ? "bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-300 font-bold"
                        : "bg-gray-200 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                    }`}
                  >
                    {grupo}: {String(lag)}
                  </span>
                ))}
              </div>
            )}
          </div>
        </section>

        {/* CONTADORES POR ESTADO EN VIVO (Torre de Control) */}
        <section className="space-y-2">
          <div className="flex justify-between items-center">
            <h2 className="text-sm font-bold uppercase tracking-wider text-gray-600 dark:text-gray-400">
              Contadores por Estado ({totalEnvios} envíos totales proyectados)
            </h2>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-9 gap-3">
            {ESTADOS_ORDEN.map((est) => (
              <div
                key={est}
                className="bg-white dark:bg-gray-800 p-3.5 rounded-xl border border-gray-200 dark:border-gray-700 shadow-sm flex flex-col justify-between"
              >
                <span className="text-[11px] font-semibold text-gray-500 dark:text-gray-400 truncate">
                  {est.replace("_", " ")}
                </span>
                <span className="text-2xl font-bold mt-1">
                  {contadores[est] || 0}
                </span>
              </div>
            ))}
          </div>
        </section>

        {/* COLA DE ALERTAS PRIORIZADA + ÚLTIMOS EVENTOS EN VIVO */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* COLA DE ALERTAS DE SLA PRIORIZADA (Crítica > Alta > Media) */}
          <section className="lg:col-span-2 bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden">
            <div className="p-4 border-b dark:border-gray-700 bg-red-50/60 dark:bg-red-950/20 flex justify-between items-center">
              <div>
                <h2 className="text-base font-bold text-red-900 dark:text-red-300">
                  Cola de Alertas de SLA Abiertas (Priorizada)
                </h2>
                <p className="text-xs text-gray-500 dark:text-gray-400">
                  Se cierran automáticamente cuando el envío pasa a ENTREGADO.
                </p>
              </div>
              <span className="px-2.5 py-1 rounded-full text-xs font-bold bg-red-100 dark:bg-red-900/50 text-red-800 dark:text-red-200">
                {alertas.length} activas
              </span>
            </div>

            <div className="max-h-72 overflow-y-auto divide-y dark:divide-gray-700">
              {alertas.length === 0 ? (
                <div className="p-8 text-center text-sm text-gray-500 dark:text-gray-400">
                  No hay alertas de SLA abiertas en este momento.
                </div>
              ) : (
                alertas.map((alerta) => (
                  <div
                    key={alerta._id}
                    className="p-3.5 flex items-center justify-between hover:bg-gray-50 dark:hover:bg-gray-700/40 transition"
                  >
                    <div className="flex items-center gap-3">
                      <span
                        className={`px-2.5 py-0.5 rounded text-xs font-bold ${badgePrioridad(
                          alerta.prioridad
                        )}`}
                      >
                        {alerta.prioridad}
                      </span>
                      <div>
                        <div className="font-mono text-sm font-bold">
                          {alerta.shipmentId} —{" "}
                          <span className="font-sans font-semibold">
                            {alerta.causa}
                          </span>
                        </div>
                        <div className="text-xs text-gray-500 dark:text-gray-400">
                          Emitida (reloj simulado):{" "}
                          {alerta.ocurridoEn
                            ? new Date(alerta.ocurridoEn).toLocaleString()
                            : "-"}
                        </div>
                      </div>
                    </div>

                    <Link
                      href={`/envios/${alerta.shipmentId}`}
                      className="text-xs font-semibold text-blue-600 dark:text-blue-400 hover:underline"
                    >
                      Inspeccionar →
                    </Link>
                  </div>
                ))
              )}
            </div>
          </section>

          {/* ÚLTIMOS EVENTOS EN VIVO */}
          <section className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden">
            <div className="p-4 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/60">
              <h2 className="text-base font-bold">Últimos Eventos Aplicados</h2>
              <p className="text-xs text-gray-500 dark:text-gray-400">
                Flujo de eventos proyectados en tiempo real
              </p>
            </div>
            <div className="max-h-72 overflow-y-auto divide-y dark:divide-gray-700">
              {ultimosEventos.length === 0 ? (
                <div className="p-8 text-center text-sm text-gray-500">
                  Sin eventos recientes.
                </div>
              ) : (
                ultimosEventos.map((ev, idx) => (
                  <div key={`${ev.eventId || idx}-${idx}`} className="p-3 text-xs space-y-1">
                    <div className="flex justify-between items-center">
                      <Link
                        href={`/envios/${ev.shipmentId}`}
                        className="font-mono font-bold text-blue-600 dark:text-blue-400 hover:underline"
                      >
                        {ev.shipmentId}
                      </Link>
                      <span className="text-gray-400">
                        {ev.ocurridoEn
                          ? new Date(ev.ocurridoEn).toLocaleTimeString()
                          : ""}
                      </span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="font-semibold">{ev.tipo}</span>
                      {ev.detalle && (
                        <span className="text-gray-500 dark:text-gray-400 truncate max-w-[180px]">
                          {ev.detalle}
                        </span>
                      )}
                    </div>
                  </div>
                ))
              )}
            </div>
          </section>
        </div>

        {/* TABLA DE ENVÍOS EN VIVO */}
        <section className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden">
          <div className="p-5 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 flex justify-between items-center">
            <div>
              <h2 className="text-lg font-semibold">
                Últimos Envíos en Seguimiento
              </h2>
              <p className="text-xs text-gray-500 dark:text-gray-400">
                Haz clic en &quot;Detalle Supervisor&quot; para ver línea de tiempo, alertas y notificaciones, o en &quot;Tracking Público&quot; para la vista del destinatario.
              </p>
            </div>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="bg-gray-100 dark:bg-gray-900/50 text-gray-600 dark:text-gray-400 text-xs uppercase">
                  <th className="p-3.5 border-b dark:border-gray-700">ID de Envío</th>
                  <th className="p-3.5 border-b dark:border-gray-700">Estado Actual</th>
                  <th className="p-3.5 border-b dark:border-gray-700">Intentos / Secuencia</th>
                  <th className="p-3.5 border-b dark:border-gray-700">Zona / Franja</th>
                  <th className="p-3.5 border-b dark:border-gray-700">Hub</th>
                  <th className="p-3.5 border-b dark:border-gray-700">Promesa Vence</th>
                  <th className="p-3.5 border-b dark:border-gray-700 text-right">Vistas</th>
                </tr>
              </thead>
              <tbody>
                {envios.length === 0 ? (
                  <tr>
                    <td
                      colSpan={7}
                      className="p-8 text-center text-gray-500 dark:text-gray-400"
                    >
                      No hay envíos registrados aún.
                    </td>
                  </tr>
                ) : (
                  envios.map((envio) => (
                    <tr
                      key={envio._id}
                      className="border-b dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors text-sm"
                    >
                      <td className="p-3.5 font-mono font-semibold">
                        <Link
                          href={`/envios/${envio.shipmentId}`}
                          className="text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          {envio.shipmentId}
                        </Link>
                      </td>
                      <td className="p-3.5">
                        <span
                          className={`px-2.5 py-1 rounded-full text-xs font-bold border ${badgeColorEstado(
                            envio.estado
                          )}`}
                        >
                          {envio.estado}
                        </span>
                      </td>
                      <td className="p-3.5 text-xs">
                        Intento #{envio.intentos || 1} · Seq {envio.ultimaSecuencia || 0}
                      </td>
                      <td className="p-3.5 text-xs">
                        {envio.zonaId || "-"} ({envio.franja || "MAÑANA"})
                      </td>
                      <td className="p-3.5 text-xs font-medium">{envio.hubId || "-"}</td>
                      <td className="p-3.5 text-xs text-gray-500 dark:text-gray-400">
                        {envio.promesa?.venceEn
                          ? new Date(envio.promesa.venceEn).toLocaleString()
                          : "-"}
                      </td>
                      <td className="p-3.5 text-right space-x-3 text-xs">
                        <Link
                          href={`/envios/${envio.shipmentId}`}
                          className="font-semibold text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          Detalle Supervisor
                        </Link>
                        <Link
                          href={`/tracking/${envio.shipmentId}`}
                          className="font-semibold text-emerald-600 dark:text-emerald-400 hover:underline"
                        >
                          Tracking Público
                        </Link>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  );
}
