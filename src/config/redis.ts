import { Redis, RedisOptions } from 'ioredis';
import dotenv from 'dotenv';

dotenv.config();

export const redisOptions: RedisOptions = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT) || 6379,
  maxRetriesPerRequest: null, // Required by BullMQ
};

// General connection (rate limiter, etc.)
export const redisConnection = new Redis(redisOptions);

// Dedicated publisher for SSE events
export const redisPublisher = new Redis(redisOptions);

redisConnection.on('connect', () => {
  console.log('[Redis] Connected successfully.');
});