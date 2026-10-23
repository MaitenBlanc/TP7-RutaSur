import { NextRequest, NextResponse } from "next/server";
import mongoose from "mongoose";
import { connectDB } from "@/lib/mongodb";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  await connectDB();

  const stream = new ReadableStream({
    start(controller) {
      let closed = false;
      const encoder = new TextEncoder();

      const pushEvent = (origen: string, change: unknown) => {
        if (closed) return;
        try {
          const payload = JSON.stringify({ origen, change, ts: Date.now() });
          controller.enqueue(encoder.encode(`data: ${payload}\n\n`));
        } catch {
          // Stream cerrado por el cliente
        }
      };

      try {
        const db = mongoose.connection;
        // Los change streams de envios y alertas alimentan las actualizaciones en vivo
        const enviosStream = db.collection("envios").watch();
        const alertasStream = db.collection("alertas").watch();
        const notificacionesStream = db.collection("notificaciones").watch();

        enviosStream.on("change", (change) => pushEvent("envios", change));
        alertasStream.on("change", (change) => pushEvent("alertas", change));
        notificacionesStream.on("change", (change) => pushEvent("notificaciones", change));

        const closeAll = () => {
          if (closed) return;
          closed = true;
          enviosStream.close().catch(() => {});
          alertasStream.close().catch(() => {});
          notificacionesStream.close().catch(() => {});
          try {
            controller.close();
          } catch {}
        };

        enviosStream.on("error", closeAll);
        alertasStream.on("error", closeAll);
        notificacionesStream.on("error", closeAll);

        req.signal.addEventListener("abort", closeAll);
      } catch (error) {
        console.error("Error al iniciar Change Streams:", error);
        closed = true;
        controller.close();
      }
    },
  });

  return new NextResponse(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}
