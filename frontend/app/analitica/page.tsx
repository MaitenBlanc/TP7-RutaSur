"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { ThemeToggle } from "@/app/components/ThemeToggle";

const SIMULADOR_BASE =
  process.env.NEXT_PUBLIC_SIMULADOR_URL || "http://localhost:4000";

export default function AnaliticaDashboard() {
  const [metricas, setMetricas] = useState<any[]>([]);
  const [filtrosDisponibles, setFiltrosDisponibles] = useState<{
    zonas: string[];
    hubs: string[];
    fechas: string[];
  }>({ zonas: [], hubs: [], fechas: [] });

  const [zonaFiltro, setZonaFiltro] = useState("TODAS");
  const [hubFiltro, setHubFiltro] = useState("TODOS");
  const [fechaFiltro, setFechaFiltro] = useState("TODAS");

  const [cuartoIndicador, setCuartoIndicador] = useState<any>({
    datosPorHub: [],
    control: { estado: "NO_ACTIVADO", groupId: null, eventosLeidos: 0 },
  });

  const [recuentoManualSemilla, setRecuentoManualSemilla] = useState<any>(null);
  const [mensajeAccion, setMensajeAccion] = useState<string | null>(null);

  const cargarAnalitica = useCallback(async () => {
    try {
      const params = new URLSearchParams();
      if (zonaFiltro !== "TODAS") params.set("zonaId", zonaFiltro);
      if (hubFiltro !== "TODOS") params.set("hubId", hubFiltro);
      if (fechaFiltro !== "TODAS") params.set("fecha", fechaFiltro);

      const res = await fetch(`/api/analitica?${params.toString()}`);
      const data = await res.json();
      if (Array.isArray(data)) {
        setMetricas(data);
      } else {
        setMetricas(data.metricas || []);
        setFiltrosDisponibles(
          data.filtrosDisponibles || { zonas: [], hubs: [], fechas: [] }
        );
        setCuartoIndicador(
          data.cuartoIndicador || {
            datosPorHub: [],
            control: { estado: "NO_ACTIVADO", groupId: null, eventosLeidos: 0 },
          }
        );
      }
    } catch (e) {
      console.error("Error cargando analítica:", e);
    }
  }, [zonaFiltro, hubFiltro, fechaFiltro]);

  useEffect(() => {
    cargarAnalitica();
    const interval = setInterval(cargarAnalitica, 2500);
    return () => clearInterval(interval);
  }, [cargarAnalitica]);

  const activarCuartoIndicador = async () => {
    setMensajeAccion(
      "Activando 4° indicador: creando nuevo grupo de consumidores y leyendo envios.estados desde offset 0..."
    );
    const res = await fetch("/api/analitica", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ accion: "activar-cuarto-indicador" }),
    });
    const data = await res.json();
    setMensajeAccion(data.message);
    setTimeout(cargarAnalitica, 1000);
  };

  const ejecutarVerificacionSemillaFija = async () => {
    try {
      setMensajeAccion(
        "Pausando flujo continuo, limpiando metricas_diarias y ejecutando corrida determinística con semilla fija (seed=2026)..."
      );
      await fetch(`${SIMULADOR_BASE}/api/flujo`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ activo: false }),
      });
      await fetch("/api/analitica", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ accion: "limpiar-metricas" }),
      });

      const res = await fetch(`${SIMULADOR_BASE}/api/escenarios/corrida-semilla`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ seed: 2026, cantidad: 15 }),
      });
      const data = await res.json();
      setRecuentoManualSemilla(data.recuentoManual);
      setZonaFiltro("TODAS");
      setHubFiltro("TODOS");
      setFechaFiltro("TODAS");
      setMensajeAccion(data.message);
      setTimeout(cargarAnalitica, 1200);
    } catch {
      setMensajeAccion("Error al ejecutar la corrida con semilla fija.");
    }
  };

  // CÁLCULO DE LOS 3 INDICADORES DE NEGOCIO
  const totalExitosos1erIntento = metricas.reduce(
    (acc, m) => acc + (m.primerIntentoExitosos ?? m.entregasExitosas ?? 0),
    0
  );
  const totalFallidos1erIntento = metricas.reduce(
    (acc, m) => acc + (m.primerIntentoFallidos ?? m.entregasFallidas ?? 0),
    0
  );
  const totalPrimerIntento = totalExitosos1erIntento + totalFallidos1erIntento;
  const tasaPrimerIntento =
    totalPrimerIntento > 0
      ? Math.round((totalExitosos1erIntento / totalPrimerIntento) * 100)
      : 0;

  // Hallar la combinación Zona + Franja con más fallos
  const peorZonaFranja = [...metricas].sort(
    (a, b) =>
      (b.primerIntentoFallidos ?? b.entregasFallidas ?? 0) -
      (a.primerIntentoFallidos ?? a.entregasFallidas ?? 0)
  )[0];

  // Indicador 2: Distribución de motivos de fallo
  const motivos = metricas.reduce(
    (acc, curr) => {
      if (curr.motivosFallo) {
        acc.AUSENTE += curr.motivosFallo.AUSENTE || 0;
        acc.DIRECCION_INCORRECTA += curr.motivosFallo.DIRECCION_INCORRECTA || 0;
        acc.RECHAZADO += curr.motivosFallo.RECHAZADO || 0;
        acc.ZONA_INACCESIBLE += curr.motivosFallo.ZONA_INACCESIBLE || 0;
      }
      return acc;
    },
    { AUSENTE: 0, DIRECCION_INCORRECTA: 0, RECHAZADO: 0, ZONA_INACCESIBLE: 0 }
  );
  const totalMotivos =
    motivos.AUSENTE +
    motivos.DIRECCION_INCORRECTA +
    motivos.RECHAZADO +
    motivos.ZONA_INACCESIBLE;

  // Indicador 3: Cumplimiento de SLA por Hub y Día (¿Qué hub llega tarde?)
  const slaPorHubMap: Record<
    string,
    { hubId: string; enSla: number; fueraSla: number }
  > = {};
  for (const m of metricas) {
    const h = m.hubId || "HUB-NORTE";
    if (!slaPorHubMap[h]) {
      slaPorHubMap[h] = { hubId: h, enSla: 0, fueraSla: 0 };
    }
    slaPorHubMap[h].enSla += m.entregasEnSla ?? m.entregasExitosas ?? 0;
    slaPorHubMap[h].fueraSla += m.entregasFueraSla ?? m.entregasFallidas ?? 0;
  }
  const slaPorHubLista = Object.values(slaPorHubMap).sort((a, b) =>
    a.hubId.localeCompare(b.hubId)
  );

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-6 md:p-8 font-sans text-gray-800 dark:text-gray-100">
      <div className="max-w-7xl mx-auto space-y-6">
        <header className="flex flex-col md:flex-row justify-between md:items-center gap-4 border-b dark:border-gray-800 pb-4">
          <div>
            <h1 className="text-3xl font-bold text-blue-900 dark:text-blue-400">
              Panel Analítico de Gerencia
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Agregaciones precalculadas en <code>metricas_diarias</code> + Reprocesamiento histórico con nuevo grupo de consumidores
            </p>
          </div>
          <div className="flex items-center gap-3">
            <Link
              href="/"
              className="px-3 py-1.5 rounded-lg text-sm font-medium bg-white dark:bg-gray-800 border dark:border-gray-700 hover:bg-gray-100 dark:hover:bg-gray-700"
            >
              ← Torre de Control
            </Link>
            <Link
              href="/tracking"
              className="px-3 py-1.5 rounded-lg text-sm font-medium bg-white dark:bg-gray-800 border dark:border-gray-700 hover:bg-gray-100 dark:hover:bg-gray-700"
            >
              Tracking Público
            </Link>
            <ThemeToggle />
          </div>
        </header>

        {/* BARRA DE FILTROS (Zona, Hub, Fecha) Y VERIFICACIÓN DE SEMILLA FIJA */}
        <section className="bg-white dark:bg-gray-800 p-5 rounded-xl shadow-sm border dark:border-gray-700 flex flex-col lg:flex-row justify-between items-start lg:items-center gap-4">
          <div className="flex flex-wrap items-center gap-4">
            <div>
              <label className="block text-xs text-gray-500 mb-1">Filtrar por Zona</label>
              <select
                value={zonaFiltro}
                onChange={(e) => setZonaFiltro(e.target.value)}
                className="px-3 py-1.5 rounded-lg border dark:border-gray-700 bg-gray-50 dark:bg-gray-900 text-sm"
              >
                <option value="TODAS">Todas las zonas</option>
                {filtrosDisponibles.zonas.map((z) => (
                  <option key={z} value={z}>
                    {z}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs text-gray-500 mb-1">Filtrar por Hub</label>
              <select
                value={hubFiltro}
                onChange={(e) => setHubFiltro(e.target.value)}
                className="px-3 py-1.5 rounded-lg border dark:border-gray-700 bg-gray-50 dark:bg-gray-900 text-sm"
              >
                <option value="TODOS">Todos los hubs</option>
                {filtrosDisponibles.hubs.map((h) => (
                  <option key={h} value={h}>
                    {h}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs text-gray-500 mb-1">Filtrar por Fecha</label>
              <select
                value={fechaFiltro}
                onChange={(e) => setFechaFiltro(e.target.value)}
                className="px-3 py-1.5 rounded-lg border dark:border-gray-700 bg-gray-50 dark:bg-gray-900 text-sm"
              >
                <option value="TODAS">Todas las fechas</option>
                {filtrosDisponibles.fechas.map((f) => (
                  <option key={f} value={f}>
                    {f}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="flex flex-wrap gap-3">
            <button
              onClick={ejecutarVerificacionSemillaFija}
              className="px-4 py-2 rounded-lg text-xs font-bold bg-emerald-600 hover:bg-emerald-700 text-white shadow-sm transition"
            >
              Verificar Corrida con Semilla Fija (Seed=2026)
            </button>
            <button
              onClick={activarCuartoIndicador}
              className="px-4 py-2 rounded-lg text-xs font-bold bg-indigo-600 hover:bg-indigo-700 text-white shadow-sm transition"
            >
              Activar 4° Indicador (Reprocesar Histórico)
            </button>
          </div>
        </section>

        {mensajeAccion && (
          <div className="bg-blue-50 dark:bg-blue-950/50 border border-blue-200 dark:border-blue-800 text-blue-800 dark:text-blue-200 px-4 py-3 rounded-xl text-xs font-medium">
            {mensajeAccion}
          </div>
        )}

        {/* HALLAZGO DE MEJORA DE NEGOCIO */}
        {peorZonaFranja && (peorZonaFranja.primerIntentoFallidos || peorZonaFranja.entregasFallidas) > 0 && (
          <section className="bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 p-4 rounded-xl flex flex-col md:flex-row justify-between md:items-center gap-3">
            <div>
              <span className="text-xs font-bold uppercase text-amber-800 dark:text-amber-300">
                Diagnóstico Operativo
              </span>
              <p className="text-sm font-semibold text-amber-950 dark:text-amber-100 mt-0.5">
                Mayor concentración de fallos detectada en{" "}
                <strong>{peorZonaFranja.zonaId}</strong> durante la franja{" "}
                <strong>{peorZonaFranja.franja || "MAÑANA"}</strong> (
                {peorZonaFranja.primerIntentoFallidos ??
                  peorZonaFranja.entregasFallidas}{" "}
                fallos registrados).
              </p>
            </div>
            <div className="bg-white dark:bg-gray-800 px-4 py-2 rounded-lg border border-amber-300 dark:border-amber-700 text-xs font-bold text-amber-900 dark:text-amber-300">
              Decisión de mejora recomendada: Ofrecer franja vespertina (TARDE) en{" "}
              {peorZonaFranja.zonaId}
            </div>
          </section>
        )}

        {/* COMPARATIVA CON SEMILLA FIJA (Criterio de aceptación CU4) */}
        {recuentoManualSemilla && (
          <section className="bg-emerald-50 dark:bg-emerald-950/30 border border-emerald-200 dark:border-emerald-800 p-5 rounded-xl space-y-3">
            <div className="flex justify-between items-center">
              <h2 className="text-sm font-bold text-emerald-900 dark:text-emerald-200">
                Verificación Criterio: Recuento Manual (Semilla {recuentoManualSemilla.seed}) vs Analítica en MongoDB
              </h2>
              <button
                onClick={() => setRecuentoManualSemilla(null)}
                className="text-xs underline text-emerald-700 dark:text-emerald-300"
              >
                Ocultar
              </button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
              <div className="bg-white dark:bg-gray-800 p-3.5 rounded-lg border dark:border-gray-700">
                <div className="font-bold mb-1">1. Primer Intento (Exitosos / Fallidos)</div>
                <div>
                  Recuento Manual:{" "}
                  <strong>
                    {recuentoManualSemilla.primerIntentoExitosos} /{" "}
                    {recuentoManualSemilla.primerIntentoFallidos}
                  </strong>
                </div>
                <div>
                  Panel Analítica:{" "}
                  <strong>
                    {totalExitosos1erIntento} / {totalFallidos1erIntento}
                  </strong>
                </div>
              </div>
              <div className="bg-white dark:bg-gray-800 p-3.5 rounded-lg border dark:border-gray-700">
                <div className="font-bold mb-1">2. Motivos de Fallo (Ausente / Dir / Rech / Zona)</div>
                <div>
                  Recuento Manual:{" "}
                  <strong>
                    {recuentoManualSemilla.motivosFallo.AUSENTE} /{" "}
                    {recuentoManualSemilla.motivosFallo.DIRECCION_INCORRECTA} /{" "}
                    {recuentoManualSemilla.motivosFallo.RECHAZADO} /{" "}
                    {recuentoManualSemilla.motivosFallo.ZONA_INACCESIBLE}
                  </strong>
                </div>
                <div>
                  Panel Analítica:{" "}
                  <strong>
                    {motivos.AUSENTE} / {motivos.DIRECCION_INCORRECTA} /{" "}
                    {motivos.RECHAZADO} / {motivos.ZONA_INACCESIBLE}
                  </strong>
                </div>
              </div>
              <div className="bg-white dark:bg-gray-800 p-3.5 rounded-lg border dark:border-gray-700">
                <div className="font-bold mb-1">3. Cumplimiento de SLA por Hub</div>
                {Object.entries(recuentoManualSemilla.slaPorHub || {}).map(
                  ([hub, val]: [string, any]) => {
                    const calc = slaPorHubMap[hub] || { enSla: 0, fueraSla: 0 };
                    return (
                      <div key={hub}>
                        {hub}: Manual <strong>{val.enSla} OK / {val.fueraSla} Tarde</strong> · Analítica{" "}
                        <strong>{calc.enSla} OK / {calc.fueraSla} Tarde</strong>
                      </div>
                    );
                  }
                )}
              </div>
            </div>
          </section>
        )}

        {/* LOS 3 INDICADORES PRINCIPALES DE CU4 */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* INDICADOR 1: Tasa de entrega en primer intento */}
          <section className="bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border dark:border-gray-700">
            <h2 className="text-base font-bold mb-1">
              1. Tasa de Entrega en Primer Intento
            </h2>
            <p className="text-xs text-gray-500 mb-4">
              ¿Dónde y cuándo fallamos más? (Zona, franja horaria, día)
            </p>

            <div className="flex items-end gap-2 mb-3">
              <span className="text-4xl font-bold text-green-600 dark:text-green-400">
                {tasaPrimerIntento}%
              </span>
              <span className="text-xs text-gray-500 mb-1">
                éxito en 1° intento
              </span>
            </div>

            <div className="w-full bg-red-100 dark:bg-red-900/30 rounded-full h-3 mb-2 overflow-hidden flex">
              <div
                className="bg-green-500 h-3 transition-all duration-700"
                style={{ width: `${tasaPrimerIntento}%` }}
              />
            </div>
            <div className="flex justify-between text-xs text-gray-500">
              <span>{totalExitosos1erIntento} Exitosos (1° int.)</span>
              <span>{totalFallidos1erIntento} Fallidos (1° int.)</span>
            </div>
          </section>

          {/* INDICADOR 2: Distribución de motivos de fallo */}
          <section className="bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border dark:border-gray-700">
            <h2 className="text-base font-bold mb-1">
              2. Distribución de Motivos de Fallo
            </h2>
            <p className="text-xs text-gray-500 mb-4">
              ¿Por qué fallamos? (Dimensiones: Zona, motivo)
            </p>

            <div className="space-y-3">
              {Object.entries(motivos).map(([motivo, valor]) => {
                const cantidad = valor as number;
                const pct =
                  totalMotivos > 0 ? Math.round((cantidad / totalMotivos) * 100) : 0;
                return (
                  <div key={motivo}>
                    <div className="flex justify-between text-xs mb-1">
                      <span className="font-semibold">
                        {motivo.replace("_", " ")}
                      </span>
                      <span>
                        {cantidad} ({pct}%)
                      </span>
                    </div>
                    <div className="w-full bg-gray-100 dark:bg-gray-700 rounded-full h-2">
                      <div
                        className="bg-orange-500 h-2 rounded-full transition-all duration-700"
                        style={{ width: `${pct}%` }}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </section>

          {/* INDICADOR 3: Cumplimiento de SLA por Hub */}
          <section className="bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border dark:border-gray-700">
            <h2 className="text-base font-bold mb-1">
              3. Cumplimiento de SLA por Hub
            </h2>
            <p className="text-xs text-gray-500 mb-4">
              ¿Qué hub llega tarde? (Dimensiones: Hub, día)
            </p>

            <div className="space-y-3">
              {slaPorHubLista.length === 0 ? (
                <div className="text-xs text-gray-500 py-4 text-center">
                  Sin datos de SLA aún.
                </div>
              ) : (
                slaPorHubLista.map((item) => {
                  const totalHub = item.enSla + item.fueraSla;
                  const pctCumplimiento =
                    totalHub > 0 ? Math.round((item.enSla / totalHub) * 100) : 0;
                  return (
                    <div key={item.hubId} className="space-y-1">
                      <div className="flex justify-between text-xs">
                        <span className="font-bold">{item.hubId}</span>
                        <span>
                          {pctCumplimiento}% en SLA ({item.enSla} OK /{" "}
                          <strong className="text-red-600 dark:text-red-400">
                            {item.fueraSla} tarde
                          </strong>
                          )
                        </span>
                      </div>
                      <div className="w-full bg-red-100 dark:bg-red-900/40 rounded-full h-2.5 overflow-hidden">
                        <div
                          className="bg-blue-600 h-2.5 rounded-full transition-all duration-700"
                          style={{ width: `${pctCumplimiento}%` }}
                        />
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </section>
        </div>

        {/* INDICADOR 4: REPROCESAMIENTO HISTÓRICO DESDE EL INICIO */}
        <section className="bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border border-indigo-200 dark:border-indigo-900 space-y-4">
          <div className="flex flex-col md:flex-row justify-between md:items-center gap-3 border-b dark:border-gray-700 pb-3">
            <div>
              <h2 className="text-base font-bold text-indigo-900 dark:text-indigo-300">
                4. Indicador de Reprocesamiento Histórico: Tiempo Medio entre INGRESADO_HUB y EN_REPARTO por Hub
              </h2>
              <p className="text-xs text-gray-500">
                Un grupo de consumidores nuevo lee <code>envios.estados</code> desde el offset 0 y completa el histórico sin reiniciar los demás servicios.
              </p>
            </div>
            <div className="flex items-center gap-3 text-xs">
              <span className="px-3 py-1 rounded-full bg-indigo-50 dark:bg-indigo-950/60 border border-indigo-200 dark:border-indigo-800 font-mono">
                Grupo: {cuartoIndicador?.control?.groupId || "No iniciado"} · Eventos leídos:{" "}
                {cuartoIndicador?.control?.eventosLeidos || 0}
              </span>
              <button
                onClick={activarCuartoIndicador}
                className="px-3.5 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-700 text-white font-semibold"
              >
                Reprocesar Histórico Ahora
              </button>
            </div>
          </div>

          {(!cuartoIndicador?.datosPorHub || cuartoIndicador.datosPorHub.length === 0) ? (
            <div className="p-6 text-center text-sm text-gray-500">
              Haz clic en <strong>&quot;Activar 4° Indicador (Reprocesar Histórico)&quot;</strong> para que un nuevo grupo de consumidores lea todo el log de Kafka desde el inicio y calcule este indicador.
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {cuartoIndicador.datosPorHub.map((item: any) => (
                <div
                  key={item.hubId}
                  className="p-4 rounded-xl bg-indigo-50/50 dark:bg-indigo-950/20 border border-indigo-100 dark:border-indigo-900"
                >
                  <div className="text-xs font-bold text-indigo-700 dark:text-indigo-300">
                    {item.hubId}
                  </div>
                  <div className="text-3xl font-bold mt-1">
                    {item.promedioMinutos} min
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    Promedio entre INGRESADO_HUB y EN_REPARTO ({item.cantidadEnviosCalculados}{" "}
                    envíos históricos procesados)
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>

        {/* TABLA DETALLADA POR ZONA, FRANJA Y FECHA (Indicador 1 + 2 + 3) */}
        <section className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border dark:border-gray-700 overflow-hidden">
          <div className="p-5 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
            <h2 className="text-base font-bold">
              Desglose de <code>metricas_diarias</code> por Fecha, Zona, Franja Horaria y Hub
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse text-sm">
              <thead>
                <tr className="bg-gray-100 dark:bg-gray-900/50 text-gray-600 dark:text-gray-400 text-xs uppercase">
                  <th className="p-3.5 border-b dark:border-gray-700">Fecha</th>
                  <th className="p-3.5 border-b dark:border-gray-700">Zona</th>
                  <th className="p-3.5 border-b dark:border-gray-700">Franja</th>
                  <th className="p-3.5 border-b dark:border-gray-700">Hub</th>
                  <th className="p-3.5 border-b dark:border-gray-700 text-right">1° Int. Éxito</th>
                  <th className="p-3.5 border-b dark:border-gray-700 text-right">1° Int. Fallo</th>
                  <th className="p-3.5 border-b dark:border-gray-700 text-right">Tasa 1° Int.</th>
                  <th className="p-3.5 border-b dark:border-gray-700 text-right">En SLA / Fuera SLA</th>
                </tr>
              </thead>
              <tbody>
                {metricas.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="p-8 text-center text-gray-500">
                      No hay métricas para los filtros seleccionados.
                    </td>
                  </tr>
                ) : (
                  metricas.map((m) => {
                    const ex1 = m.primerIntentoExitosos ?? m.entregasExitosas ?? 0;
                    const fa1 = m.primerIntentoFallidos ?? m.entregasFallidas ?? 0;
                    const tot1 = ex1 + fa1;
                    const tasa = tot1 > 0 ? Math.round((ex1 / tot1) * 100) : 0;
                    return (
                      <tr
                        key={m._id}
                        className="border-b dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/40"
                      >
                        <td className="p-3.5">{m.fecha}</td>
                        <td className="p-3.5 font-semibold">{m.zonaId}</td>
                        <td className="p-3.5">
                          <span className="px-2 py-0.5 rounded text-xs font-semibold bg-gray-100 dark:bg-gray-700">
                            {m.franja || "MAÑANA"}
                          </span>
                        </td>
                        <td className="p-3.5">{m.hubId}</td>
                        <td className="p-3.5 text-right text-green-600 dark:text-green-400 font-semibold">
                          {ex1}
                        </td>
                        <td className="p-3.5 text-right text-red-600 dark:text-red-400 font-semibold">
                          {fa1}
                        </td>
                        <td className="p-3.5 text-right font-bold">{tasa}%</td>
                        <td className="p-3.5 text-right text-xs">
                          {m.entregasEnSla ?? ex1} OK / {m.entregasFueraSla ?? fa1} Tarde
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  );
}
