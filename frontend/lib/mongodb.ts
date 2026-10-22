import mongoose from 'mongoose';

// Docker usará la variable de entorno, en local usará localhost
const MONGODB_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/rutasur';

export const connectDB = async () => {
  if (mongoose.connection.readyState >= 1) return;
  
  try {
    await mongoose.connect(MONGODB_URI);
    console.log('Frontend conectado a MongoDB');
  } catch (error) {
    console.error('Error conectando a MongoDB desde el frontend:', error);
  }
};