import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { aiQueue } from '../../workers/queue.js';
import { pool } from '../../config/db.js';
import { AIJobPayload } from '../../types/index.js';

interface SubmitBody {
  prompt: string;
}

export async function jobRoutes(fastify: FastifyInstance) {
  // 1. Submit an AI Task (Returns immediate 202 Accepted)
  fastify.post('/v1/jobs/submit', async (req: FastifyRequest<{ Body: SubmitBody }>, reply: FastifyReply) => {
    const apiKey = req.headers['x-api-key'] as string;
    const { prompt } = req.body;

    if (!apiKey) {
      return reply.status(401).send({ error: 'Missing x-api-key header' });
    }

    if (!prompt) {
      return reply.status(400).send({ error: 'Missing prompt in request body' });
    }

    // Authenticate tenant
    const tenantRes = await pool.query(`SELECT id, rpm_limit FROM tenants WHERE api_key = $1`, [apiKey]);
    if (tenantRes.rows.length === 0) {
      return reply.status(403).send({ error: 'Invalid API key' });
    }

    const tenant = tenantRes.rows[0];

    // Create job record in PostgreSQL
    const jobRes = await pool.query(
      `INSERT INTO jobs (tenant_id, prompt, status) VALUES ($1, $2, 'queued') RETURNING id`,
      [tenant.id, prompt]
    );

    const jobId = jobRes.rows[0].id;

    // Enqueue in BullMQ Redis
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
    });
  });

  // 2. Check Job Status & Output
  fastify.get('/v1/jobs/:id', async (req: FastifyRequest<{ Params: { id: string } }>, reply: FastifyReply) => {
    const { id } = req.params;

    const res = await pool.query(
      `SELECT id, status, prompt, result, provider_resolved, model_resolved, latency_ms, error_message, created_at, updated_at 
       FROM jobs WHERE id = $1`,
      [id]
    );

    if (res.rows.length === 0) {
      return reply.status(404).send({ error: 'Job not found' });
    }

    return reply.send(res.rows[0]);
  });
}