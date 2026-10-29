import { Worker, Job } from 'bullmq';
import { redisConnection } from '../config/redis.js';
import { pool } from '../config/db.js';
import { AI_QUEUE_NAME } from './queue.js';
import { LLMOrchestrator } from './orchestrator.js';
import { AIJobPayload, AIJobResult } from '../types/index.js';

export function startWorker() {
  const worker = new Worker<AIJobPayload, AIJobResult>(
    AI_QUEUE_NAME,
    async (job: Job<AIJobPayload>) => {
      console.log(`[Worker] Processing Job ID: ${job.data.jobId} (Tenant: ${job.data.tenantId})`);

      // Update status in PostgreSQL to 'processing'
      await pool.query(
        `UPDATE jobs SET status = 'processing', updated_at = NOW() WHERE id = $1`,
        [job.data.jobId]
      );

      // Execute AI task with automatic failover
      const result = await LLMOrchestrator.executeWithFallback(job.data);

      // Update job to completed
      await pool.query(
        `UPDATE jobs 
         SET status = 'completed', 
             result = $1, 
             provider_resolved = $2, 
             model_resolved = $3, 
             latency_ms = $4, 
             updated_at = NOW() 
         WHERE id = $5`,
        [
          JSON.stringify(result.content),
          result.providerResolved,
          result.modelResolved,
          result.latencyMs,
          job.data.jobId,
        ]
      );

      // Insert audit usage log
      await pool.query(
        `INSERT INTO usage_logs (tenant_id, job_id, provider, model, prompt_tokens, completion_tokens, latency_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [
          job.data.tenantId,
          job.data.jobId,
          result.providerResolved,
          result.modelResolved,
          result.tokens.prompt,
          result.tokens.completion,
          result.latencyMs,
        ]
      );

      return result;
    },
    {
      connection: redisConnection,
      concurrency: 5, // Process 5 tasks simultaneously
    }
  );

  worker.on('completed', (job) => {
    console.log(`[Worker] Job ${job.data.jobId} completed successfully.`);
  });

  worker.on('failed', async (job, err) => {
    console.error(`[Worker] Job ${job?.data?.jobId} failed:`, err.message);
    if (job) {
      await pool.query(
        `UPDATE jobs SET status = 'failed', error_message = $1, updated_at = NOW() WHERE id = $2`,
        [err.message, job.data.jobId]
      );
    }
  });

  return worker;
}