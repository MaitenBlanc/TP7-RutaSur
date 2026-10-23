"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { ThemeToggle } from "@/app/components/ThemeToggle";

export default function TrackingPublicoBuscador() {
  const router = useRouter();
  const [codigo, setCodigo] = useState("");
  const [recientes, setRecientes] = useState<any[]>([]);

  useEffect(() => {
    fetch("/api/envios")
      .then((res) => res.json())
      .then((data) => {
        const lista = Array.isArray(data) ? data : data.envios || [];
        setRecientes(lista.slice(0, 8));
      })
      .catch(() => {});
  }, []);

  const buscar = (e: React.FormEvent) => {
    e.preventDefault();
    if (!codigo.trim()) return;
    router.push(`/tracking/${encodeURIComponent(codigo.trim())}`);
  };

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-6 md:p-8 font-sans text-gray-800 dark:text-gray-100">
      <div className="max-w-3xl mx-auto space-y-8">
        <header className="flex justify-between items-center border-b dark:border-gray-800 pb-4">
          <div>
            <h1 className="text-2xl font-bold text-emerald-700 dark:text-emerald-400">
              RutaSur · Seguimiento Público de Paquetes
            </h1>
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Vista para el destinatario (sin datos internos de operación)
            </p>
          </div>
          <div className="flex items-center gap-3">
            <Link
              href="/"
              className="text-xs font-semibold text-blue-600 dark:text-blue-400 hover:underline"
            >
              ← Torre de Control
            </Link>
            <ThemeToggle />
          </div>
        </header>

        <section className="bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border dark:border-gray-700 space-y-4">
          <h2 className="text-lg font-semibold">Consultá el estado de tu envío en vivo</h2>
          <form onSubmit={buscar} className="flex gap-3">
            <input
              type="text"
              value={codigo}
              onChange={(e) => setCodigo(e.target.value)}
              placeholder="Ingresá tu código (ej: SHIP-FELIZ-1001)"
              className="flex-1 px-4 py-2.5 rounded-lg border dark:border-gray-700 bg-gray-50 dark:bg-gray-900 font-mono text-sm"
            />
            <button
              type="submit"
              className="px-5 py-2.5 rounded-lg bg-emerald-600 hover:bg-emerald-700 text-white font-semibold text-sm transition"
            >
              Consultar
            </button>
          </form>

          {recientes.length > 0 && (
            <div className="pt-2 space-y-2">
              <div className="text-xs text-gray-500">
                Accesos rápidos a envíos activos en la demo:
              </div>
              <div className="flex flex-wrap gap-2">
                {recientes.map((env) => (
                  <Link
                    key={env.shipmentId}
                    href={`/tracking/${env.shipmentId}`}
                    className="px-3 py-1 rounded-full text-xs font-mono bg-gray-100 dark:bg-gray-700 hover:bg-emerald-100 dark:hover:bg-emerald-900/40 transition"
                  >
                    {env.shipmentId} ({env.estado})
                  </Link>
                ))}
              </div>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

