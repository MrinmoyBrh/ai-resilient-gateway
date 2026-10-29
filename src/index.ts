import Fastify from 'fastify';
import cors from '@fastify/cors';
import dotenv from 'dotenv';
import { initDB } from './config/db.js';
import { startWorker } from './workers/worker.js';
import { jobRoutes } from './gateway/routes/jobs.js';

dotenv.config();

const fastify = Fastify({ logger: true });

async function main() {
  try {
    // 1. Initialize DB tables
    await initDB();

    // 2. Register Plugins & Routes
    await fastify.register(cors, { origin: '*' });
    await fastify.register(jobRoutes);

    // 3. Start BullMQ Background Worker
    startWorker();
    console.log('[Worker] BullMQ worker listening for queue events.');

    // 4. Start HTTP Server
    const port = Number(process.env.PORT) || 3000;
    await fastify.listen({ port, host: '0.0.0.0' });
    console.log(`[Gateway] HTTP Server running on http://localhost:${port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

main();