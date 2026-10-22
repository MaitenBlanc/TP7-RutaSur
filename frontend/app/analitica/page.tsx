"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { ThemeToggle } from "@/app/components/ThemeToggle";

export default function AnaliticaDashboard() {
  const [metricas, setMetricas] = useState<any[]>([]);

  useEffect(() => {
    fetch("/api/analitica")
      .then((res) => res.json())
      .then((data) => setMetricas(data))
      .catch(console.error);
  }, []);

  // Cálculos de indicadores de negocio
  const totalExitosas = metricas.reduce((acc, curr) => acc + (curr.entregasExitosas || 0), 0);
  const totalFallidas = metricas.reduce((acc, curr) => acc + (curr.entregasFallidas || 0), 0);
  const totalEnvios = totalExitosas + totalFallidas;
  const tasaExito = totalEnvios > 0 ? Math.round((totalExitosas / totalEnvios) * 100) : 0;

  const motivos = metricas.reduce((acc, curr) => {
    if (curr.motivosFallo) {
      acc.AUSENTE += curr.motivosFallo.AUSENTE || 0;
      acc.DIRECCION_INCORRECTA += curr.motivosFallo.DIRECCION_INCORRECTA || 0;
      acc.RECHAZADO += curr.motivosFallo.RECHAZADO || 0;
      acc.ZONA_INACCESIBLE += curr.motivosFallo.ZONA_INACCESIBLE || 0;
    }
    return acc;
  }, { AUSENTE: 0, DIRECCION_INCORRECTA: 0, RECHAZADO: 0, ZONA_INACCESIBLE: 0 });

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-8 font-sans text-gray-800 dark:text-gray-100 transition-colors duration-300">
      <div className="max-w-6xl mx-auto">
        
        <header className="mb-8 flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold text-blue-900 dark:text-blue-400">Panel Analítico</h1>
            <p className="text-gray-500 dark:text-gray-400">Indicadores de negocio y rendimiento (CU4)</p>
          </div>
          <div className="flex gap-4 items-center">
            <Link href="/" className="text-sm font-medium text-blue-600 dark:text-blue-400 hover:underline">
              ← Volver a la Torre de Control
            </Link>
            <ThemeToggle />
          </div>
        </header>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-8">
          {/* INDICADOR 1: Tasa de entrega */}
          <section className="bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 transition-colors duration-300">
            <h2 className="text-lg font-semibold mb-2">Tasa de Entrega Global</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">¿Cuántos paquetes entregamos vs fallamos?</p>
            
            <div className="flex items-end gap-2 mb-2">
              <span className="text-5xl font-bold text-green-600 dark:text-green-400">{tasaExito}%</span>
              <span className="text-gray-500 dark:text-gray-400 mb-1">de éxito</span>
            </div>
            
            <div className="w-full bg-red-100 dark:bg-red-900/30 rounded-full h-4 mb-2 overflow-hidden flex">
              <div className="bg-green-500 h-4 transition-all duration-1000" style={{ width: `${tasaExito}%` }}></div>
            </div>
            <div className="flex justify-between text-xs text-gray-500 dark:text-gray-400">
              <span>{totalExitosas} Exitosas</span>
              <span>{totalFallidas} Fallidas</span>
            </div>
          </section>

          {/* INDICADOR 2: Motivos de Fallo */}
          <section className="bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 transition-colors duration-300">
            <h2 className="text-lg font-semibold mb-2">Distribución de Motivos de Fallo</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">¿Por qué están fallando las entregas?</p>
            
            <div className="space-y-4">
              {Object.entries(motivos).map(([motivo, valor]) => {
                const cantidad = valor as number; 
                
                const porcentaje = totalFallidas > 0 ? Math.round((cantidad / totalFallidas) * 100) : 0;
                return (
                  <div key={motivo}>
                    <div className="flex justify-between text-sm mb-1">
                      <span className="font-medium">{motivo.replace('_', ' ')}</span>
                      <span>{cantidad} ({porcentaje}%)</span>
                    </div>
                    <div className="w-full bg-gray-100 dark:bg-gray-700 rounded-full h-2">
                      <div className="bg-orange-500 h-2 rounded-full transition-all duration-1000" style={{ width: `${porcentaje}%` }}></div>
                    </div>
                  </div>
                );
              })}
            </div>
          </section>
        </div>

        {/* INDICADOR 3: Desglose por Zona y Hub */}
        <section className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden transition-colors duration-300">
          <div className="p-6 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
            <h2 className="text-lg font-semibold">Métricas por Zona y Fecha</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="bg-gray-100 dark:bg-gray-900/50 text-gray-600 dark:text-gray-400 text-sm">
                  <th className="p-4 border-b dark:border-gray-700">Fecha</th>
                  <th className="p-4 border-b dark:border-gray-700">Hub</th>
                  <th className="p-4 border-b dark:border-gray-700">Zona</th>
                  <th className="p-4 border-b dark:border-gray-700 text-right">Exitosas</th>
                  <th className="p-4 border-b dark:border-gray-700 text-right">Fallidas</th>
                </tr>
              </thead>
              <tbody>
                {metricas.map((m) => (
                  <tr key={m._id} className="border-b dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/50">
                    <td className="p-4 text-sm">{m.fecha}</td>
                    <td className="p-4 text-sm font-medium">{m.hubId}</td>
                    <td className="p-4 text-sm">{m.zonaId}</td>
                    <td className="p-4 text-sm text-right text-green-600 dark:text-green-400">{m.entregasExitosas || 0}</td>
                    <td className="p-4 text-sm text-right text-red-600 dark:text-red-400">{m.entregasFallidas || 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

      </div>
    </div>
  );
}