import { NextResponse } from 'next/server';
import mongoose from 'mongoose';
import { connectDB } from '../../../lib/mongodb';

export async function GET() {
  await connectDB();
  // Trae los últimos 15 envíos ordenados por fecha de actualización
  const envios = await mongoose.connection.collection('envios')
    .find()
    .sort({ updatedAt: -1 })
    .limit(15)
    .toArray();
    
  return NextResponse.json(envios);
}