"use client";

import { faCircleCheck, faCircleXmark, faClock } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { useEffect, useState } from "react";
import { ThemeToggle } from "./components/ThemeToggle";

export default function TorreDeControl() {
  const [envios, setEnvios] = useState<any[]>([]);

  // Función para leer la DB
  const fetchEnvios = async () => {
    try {
      const res = await fetch("/api/envios");
      const data = await res.json();
      setEnvios(data);
    } catch (error) {
      console.error("Error cargando envíos:", error);
    }
  };

  useEffect(() => {
    fetchEnvios(); // Carga inicial

    // Conexión a MongoDB Change Streams vía Server-Sent Events
    const eventSource = new EventSource("/api/stream");
    eventSource.onmessage = () => {
      // Si MongoDB avisa que hubo un cambio, recarga la tabla instantáneamente
      fetchEnvios();
    };

    return () => eventSource.close();
  }, []);

  // Función para interactuar con el Simulador (Panel de Demo)
  const dispararEscenario = async (escenario: string) => {
    try {
      await fetch(`http://localhost:4000/api/escenarios/${escenario}`, {
        method: "POST",
      });
    } catch (error) {
      alert("Error conectando al simulador. ¿Está encendido el puerto 4000?");
    }
  };

  return (
   <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-8 font-sans text-gray-800 dark:text-gray-100 transition-colors duration-300">
      <div className="max-w-6xl mx-auto">
        <header className="mb-8 flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold text-blue-900 dark:text-blue-400">
              RutaSur Logística
            </h1>
            <p className="text-gray-500 dark:text-gray-400">Torre de Control en Tiempo Real</p>
          </div>
          <div className="flex gap-4 items-center">
            <ThemeToggle />
            
            <span className="flex items-center text-sm font-medium text-green-600 dark:text-green-400 bg-green-100 dark:bg-green-900/30 px-3 py-1 rounded-full border border-green-200 dark:border-green-800">
              <span className="w-2 h-2 bg-green-500 dark:bg-green-400 rounded-full animate-pulse mr-2"></span>
              Conexión SSE Activa
            </span>
          </div>
        </header>

        {/* PANEL DE DEMO: Botones del simulador */}
        <section className="bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 mb-8 transition-colors duration-300">
          <h2 className="text-lg font-semibold mb-4 border-b dark:border-gray-700 pb-2">
            Panel de Demo (Simulador)
          </h2>
          <div className="flex flex-wrap gap-4">
            <button
              onClick={() => dispararEscenario("feliz")}
              className="bg-blue-600 text-white px-4 py-2 rounded shadow hover:bg-blue-700 transition flex items-center gap-2"
            >
              {/* <FontAwesomeIcon icon={byPrefixAndName.far['circle-check']} style={{color: "rgb(101, 230, 99)",}} /> */}
              <FontAwesomeIcon icon={faCircleCheck} />
              {/* <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 640">
                <path
                  fill="rgb(101, 230, 99)"
                  d="M320 576C178.6 576 64 461.4 64 320C64 178.6 178.6 64 320 64C461.4 64 576 178.6 576 320C576 461.4 461.4 576 320 576zM320 112C205.1 112 112 205.1 112 320C112 434.9 205.1 528 320 528C434.9 528 528 434.9 528 320C528 205.1 434.9 112 320 112zM390.7 233.9C398.5 223.2 413.5 220.8 424.2 228.6C434.9 236.4 437.3 251.4 429.5 262.1L307.4 430.1C303.3 435.8 296.9 439.4 289.9 439.9C282.9 440.4 276 437.9 271.1 433L215.2 377.1C205.8 367.7 205.8 352.5 215.2 343.2C224.6 333.9 239.8 333.8 249.1 343.2L285.1 379.2L390.7 234z"
                />
              </svg> */}
              Disparar Envío Feliz
            </button>
            <button
              onClick={() => dispararEscenario("ausente")}
              className="bg-orange-500 text-white px-4 py-2 rounded shadow hover:bg-orange-600 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faCircleXmark} />
              Disparar Fallo (Ausente)
            </button>
            <button
              onClick={() => dispararEscenario("demorado")}
              className="bg-red-600 text-white px-4 py-2 rounded shadow hover:bg-red-700 transition flex items-center gap-2"
            >
              <FontAwesomeIcon icon={faClock} />
              Disparar Demorado (Alerta SLA)
            </button>
          </div>
        </section>

        {/* TORRE DE CONTROL: Tabla de envíos en vivo */}
<section className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden transition-colors duration-300">
          <div className="p-6 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
            <h2 className="text-lg font-semibold">
              Últimos Envíos Registrados
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="bg-gray-100 dark:bg-gray-900/50 text-gray-600 dark:text-gray-400 text-sm">
                  <th className="p-4 border-b dark:border-gray-700">ID de Envío</th>
                  <th className="p-4 border-b dark:border-gray-700">Estado Actual</th>
                  <th className="p-4 border-b dark:border-gray-700">Zona</th>
                  <th className="p-4 border-b dark:border-gray-700">Hub</th>
                  <th className="p-4 border-b dark:border-gray-700">Última Actividad</th>
                </tr>
              </thead>
              <tbody>
                {envios.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="p-8 text-center text-gray-500 dark:text-gray-400">
                      No hay envíos registrados aún.
                    </td>
                  </tr>
                ) : (
                  envios.map((envio) => (
                    <tr
                      key={envio._id}
                      className="border-b dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
                    >
                      <td className="p-4 font-mono text-sm">
                        {envio.shipmentId}
                      </td>
                      <td className="p-4">
                        <span
                          className={`px-2 py-1 rounded-full text-xs font-bold
                          ${
                            envio.estado === "ENTREGADO"
                              ? "bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-300"
                              : envio.estado === "ENTREGA_FALLIDA"
                                ? "bg-red-100 dark:bg-red-900/40 text-red-800 dark:text-red-300"
                                : envio.estado === "REPROGRAMADO"
                                  ? "bg-orange-100 dark:bg-orange-900/40 text-orange-800 dark:text-orange-300"
                                  : "bg-blue-100 dark:bg-blue-900/40 text-blue-800 dark:text-blue-300"
                          }`}
                        >
                          {envio.estado}
                        </span>
                      </td>
                      <td className="p-4 text-sm">{envio.zonaId || "-"}</td>
                      <td className="p-4 text-sm">{envio.hubId || "-"}</td>
                      <td className="p-4 text-sm text-gray-500 dark:text-gray-400">
                        {new Date(envio.updatedAt).toLocaleTimeString()}
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