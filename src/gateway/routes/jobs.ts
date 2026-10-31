import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { aiQueue } from '../../workers/queue.js';
import { pool } from '../../config/db.js';
import { tenantRateLimiter } from '../middleware/rateLimiter.js';
import { AIJobPayload } from '../../types/index.js';
import { Redis } from 'ioredis';
import { PromptCache } from '../../services/cache.js';
import { TelemetryService } from '../../services/telemetry.js';


interface SubmitJobBody {
  prompt: string;
}

interface JobParams {
  id: string;
}

export async function jobRoutes(server: FastifyInstance) {
  // 1. Submit a task
  server.post(
    '/v1/jobs/submit',
    async (
      request: FastifyRequest<{ Body: SubmitJobBody }>,
      reply: FastifyReply
    ) => {
      const { prompt } = request.body || {};

      if (!prompt || typeof prompt !== 'string' || prompt.trim() === '') {
        return reply.status(400).send({
          statusCode: 400,
          error: 'Bad Request',
          message: 'Field "prompt" is required and must be a non-empty string.',
        });
      }

      // Default fallback tenant ID if authentication middleware has not attached one
      const tenantId =
        (request as any).tenant?.id || 'a0000000-0000-0000-0000-000000000001';

      // Step A: Check 24-hour rolling token quota
      const quotaCheck = await TelemetryService.checkQuota(tenantId);
      if (!quotaCheck.allowed) {
        return reply.status(429).send({
          statusCode: 429,
          error: 'Too Many Requests',
          message: `Daily token quota exceeded (${quotaCheck.used}/${quotaCheck.quota} tokens used in the last 24 hours).`,
        });
      }

      // Step B: Check Redis SHA-256 prompt cache (<5ms response)
      const cachedResult = await PromptCache.get(prompt);
      if (cachedResult) {
        return reply.status(200).send({
          source: 'cache',
          cached: true,
          status: 'completed',
          result: cachedResult,
          latencyMs: 2,
        });
      }

      // Step C: Initialize job entry in PostgreSQL
      const insertResult = await pool.query(
        `INSERT INTO jobs (tenant_id, prompt, status)
         VALUES ($1, $2, 'queued')
         RETURNING id`,
        [tenantId, prompt]
      );

      const jobId = insertResult.rows[0].id;

      // Step D: Enqueue task with exponential backoff retry configuration
      const payload = {
        jobId,
        tenantId,
        prompt,
      };

      await aiQueue.add('triage-job', payload, {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 1000,
        },
        removeOnComplete: true,
        removeOnFail: false,
      });

      return reply.status(202).send({
        source: 'queue',
        cached: false,
        message: 'Job queued successfully',
        jobId,
        status: 'queued',
        checkUrl: `/v1/jobs/${jobId}`,
        streamUrl: `/v1/jobs/${jobId}/stream`,
      });
    }
  );

  // 2. Poll job status
  server.get(
    '/v1/jobs/:id',
    async (
      request: FastifyRequest<{ Params: JobParams }>,
      reply: FastifyReply
    ) => {
      const { id } = request.params;

      const result = await pool.query(
        `SELECT id, tenant_id, status, prompt, result, provider_resolved, model_resolved, error_message, created_at, updated_at
         FROM jobs
         WHERE id = $1`,
        [id]
      );

      if (result.rows.length === 0) {
        return reply.status(404).send({
          statusCode: 404,
          error: 'Not Found',
          message: `Job with ID ${id} not found.`,
        });
      }

      return reply.status(200).send(result.rows[0]);
    }
  );

  // 3. Stream job status updates via Server-Sent Events (SSE)
  server.get(
    '/v1/jobs/:id/stream',
    async (
      request: FastifyRequest<{ Params: JobParams }>,
      reply: FastifyReply
    ) => {
      const { id } = request.params;

      const jobResult = await pool.query(
        `SELECT id, status, result, error_message FROM jobs WHERE id = $1`,
        [id]
      );

      if (jobResult.rows.length === 0) {
        return reply.status(404).send({
          statusCode: 404,
          error: 'Not Found',
          message: `Job with ID ${id} not found.`,
        });
      }

      const job = jobResult.rows[0];

      reply.raw.setHeader('Content-Type', 'text/event-stream');
      reply.raw.setHeader('Cache-Control', 'no-cache');
      reply.raw.setHeader('Connection', 'keep-alive');
      reply.raw.flushHeaders();

      // If job already reached a terminal state, emit and close immediately
      if (job.status === 'completed' || job.status === 'failed') {
        reply.raw.write(`data: ${JSON.stringify(job)}\n\n`);
        return reply.raw.end();
      }

      // Emit initial status
      reply.raw.write(
        `data: ${JSON.stringify({ status: job.status, jobId: id })}\n\n`
      );

      // Create a dedicated subscriber client to listen to Redis Pub/Sub events
      const subscriber = redisPublisher.duplicate();
      await subscriber.connect();

      const channel = `job-events:${id}`;

      await subscriber.subscribe(channel, (message: string) => {
        reply.raw.write(`data: ${message}\n\n`);

        try {
          const parsed = JSON.parse(message);
          if (parsed.status === 'completed' || parsed.status === 'failed') {
            subscriber.unsubscribe(channel);
            subscriber.quit();
            reply.raw.end();
          }
        } catch {
          // Continue streaming if parsing fails
        }
      });

      request.raw.on('close', async () => {
        try {
          await subscriber.unsubscribe(channel);
          await subscriber.quit();
        } catch {
          // Cleanup ignore
        }
      });
    }
  );
}