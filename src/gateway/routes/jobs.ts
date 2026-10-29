import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { aiQueue } from '../../workers/queue.js';
import { pool } from '../../config/db.js';
import { tenantRateLimiter } from '../middleware/rateLimiter.js';
import { AIJobPayload } from '../../types/index.js';
import { Redis } from 'ioredis';

interface SubmitBody {
  prompt: string;
}

export async function jobRoutes(fastify: FastifyInstance) {
  // 1. Submit Job
  fastify.post<{ Body: SubmitBody }>(
    '/v1/jobs/submit',
    { preHandler: tenantRateLimiter },
    async (req: FastifyRequest<{ Body: SubmitBody }>, reply: FastifyReply) => {
      const { prompt } = req.body || {};
      const tenant = (req as any).tenant;

      if (!prompt) {
        return reply.status(400).send({ error: 'Missing prompt in request body' });
      }

      const jobRes = await pool.query(
        `INSERT INTO jobs (tenant_id, prompt, status) VALUES ($1, $2, 'queued') RETURNING id`,
        [tenant.id, prompt]
      );

      const jobId = jobRes.rows[0].id;

      const payload: AIJobPayload = {
        jobId,
        tenantId: tenant.id,
        prompt,
        taskType: 'incident_triage',
      };

      await aiQueue.add('triage-job', payload);

      return reply.status(202).send({
        message: 'Job queued successfully',
        jobId,
        status: 'queued',
        checkUrl: `/v1/jobs/${jobId}`,
        streamUrl: `/v1/jobs/${jobId}/stream`,
      });
    }
  );

  // 2. Poll Status
  fastify.get<{ Params: { id: string } }>(
    '/v1/jobs/:id',
    async (req: FastifyRequest<{ Params: { id: string } }>, reply: FastifyReply) => {
      const { id } = req.params;
      const res = await pool.query(`SELECT * FROM jobs WHERE id = $1`, [id]);
      if (res.rows.length === 0) return reply.status(404).send({ error: 'Job not found' });
      return reply.send(res.rows[0]);
    }
  );

  // 3. Real-Time SSE Stream Endpoint
  fastify.get<{ Params: { id: string } }>(
    '/v1/jobs/:id/stream',
    async (req: FastifyRequest<{ Params: { id: string } }>, reply: FastifyReply) => {
      const { id } = req.params;

      // Set standard SSE headers
      reply.raw.setHeader('Content-Type', 'text/event-stream');
      reply.raw.setHeader('Cache-Control', 'no-cache');
      reply.raw.setHeader('Connection', 'keep-alive');
      reply.raw.setHeader('Access-Control-Allow-Origin', '*');

      // Check current DB status first in case it finished before stream opened
      const currentRes = await pool.query(`SELECT status, result, error_message FROM jobs WHERE id = $1`, [id]);
      if (currentRes.rows.length === 0) {
        reply.raw.write(`event: error\ndata: ${JSON.stringify({ error: 'Job not found' })}\n\n`);
        return reply.raw.end();
      }

      const initialStatus = currentRes.rows[0];
      reply.raw.write(`event: update\ndata: ${JSON.stringify(initialStatus)}\n\n`);

      if (initialStatus.status === 'completed' || initialStatus.status === 'failed') {
        return reply.raw.end();
      }

      // Create a dedicated Redis subscriber client for this active connection
      const subscriber = new Redis({
        host: process.env.REDIS_HOST || '127.0.0.1',
        port: Number(process.env.REDIS_PORT) || 6379,
      });

      await subscriber.subscribe(`job-events:${id}`);

      subscriber.on('message', (_channel, message) => {
        reply.raw.write(`event: update\ndata: ${message}\n\n`);
        const parsed = JSON.parse(message);
        if (parsed.status === 'completed' || parsed.status === 'failed') {
          subscriber.unsubscribe();
          subscriber.quit();
          reply.raw.end();
        }
      });

      // Cleanup if the client disconnects prematurely
      req.raw.on('close', () => {
        subscriber.unsubscribe();
        subscriber.quit();
      });
    }
  );
}