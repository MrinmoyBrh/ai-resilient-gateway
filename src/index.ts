// import Fastify from 'fastify';
// import cors from '@fastify/cors';
// import dotenv from 'dotenv';
// import { initDB } from './config/db.js';
// import { startWorker } from './workers/worker.js';
// import { jobRoutes } from './gateway/routes/jobs.js';
// import { start } from 'node:repl';

// dotenv.config();

// const fastify = Fastify({ logger: true });

// async function main() {
//   try {
//     // 1. Initialize DB tables
//     await initDB();

//     // 2. Register Plugins & Routes
//     await fastify.register(cors, { origin: '*' });
//     await fastify.register(jobRoutes);

//     // 3. Start BullMQ Background Worker
//     startWorker();
//     console.log('[Worker] BullMQ worker listening for queue events.');

//     // 4. Start HTTP Server
//     const port = Number(process.env.PORT) || 3000;
//     await fastify.listen({ port, host: '0.0.0.0' });
//     console.log(`[Gateway] HTTP Server running on http://localhost:${port}`);
//   } catch (err) {
//     fastify.log.error(err);
//     process.exit(1);
//   }
// }

// start();
import Fastify from 'fastify';
import dotenv from 'dotenv';
import { initDB } from './config/db.js';
import { jobRoutes } from './gateway/routes/jobs.js';
import { startWorker } from './workers/worker.js';

dotenv.config();

const fastify = Fastify({ logger: false });

async function bootstrap() {
  try {
    // 1. Initialize PostgreSQL schemas & tables
    console.log('[PostgreSQL] Initializing tables...');
    await initDB();
    console.log('[PostgreSQL] Database tables initialized successfully.');

    // 2. Start BullMQ background worker
    startWorker();
    console.log('[Worker] BullMQ worker listening for queue events.');

    // 3. Register Gateway routes
    await fastify.register(jobRoutes);

    // 4. Start HTTP Server
    const port = Number(process.env.PORT) || 3000;
    const address = await fastify.listen({ port, host: '0.0.0.0' });
    console.log(`[Gateway] HTTP Server running on ${address}`);
  } catch (err) {
    console.error('[Bootstrap Error]:', err);
    process.exit(1);
  }
}

bootstrap();