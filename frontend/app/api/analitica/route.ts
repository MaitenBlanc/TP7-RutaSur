import { NextResponse } from 'next/server';
import mongoose from 'mongoose';
import { connectDB } from '../../../lib/mongodb';

export const dynamic = 'force-dynamic';

export async function GET() {
  await connectDB();
  // Traer todas las métricas agregadas por el servicio de analítica
  const metricas = await mongoose.connection.collection('metricas_diarias').find().toArray();
  return NextResponse.json(metricas);
}