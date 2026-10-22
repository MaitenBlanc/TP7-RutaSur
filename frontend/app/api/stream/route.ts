import { NextRequest, NextResponse } from 'next/server';
import mongoose from 'mongoose';
import { connectDB } from '@/lib/mongodb';

export const dynamic = 'force-dynamic';

export async function GET(req: NextRequest) {
 await connectDB();

  const stream = new ReadableStream({
    start(controller) {
      try {
        const db = mongoose.connection;
        const collection = db.collection('envios');

        // Inicializa el Change Stream de MongoDB
        const changeStream = collection.watch();

        changeStream.on('change', (change) => {
          // Empuja los cambios al frontend vía SSE
          const data = `data: ${JSON.stringify(change)}\n\n`;
          controller.enqueue(new TextEncoder().encode(data));
        });

        changeStream.on('error', (err) => {
          console.error("Change Stream Error:", err);
          controller.close();
        });

        // Limpia la conexión si el usuario cierra el navegador
        req.signal.addEventListener('abort', () => {
          changeStream.close();
        });
      } catch (error) {
        console.error("Error al iniciar Change Stream:", error);
        controller.close();
      }
    }
  });

  return new NextResponse(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    },
  });
}