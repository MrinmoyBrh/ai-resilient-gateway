import { Worker, Job } from 'bullmq';
import { redisOptions, redisPublisher } from '../config/redis.js';
import { pool } from '../config/db.js';
import { LLMOrchestrator } from './orchestrator.js';
import { AIJobPayload } from '../types/index.js';

export function startWorker() {
  const worker = new Worker(
    'ai-tasks',
    async (job: Job<AIJobPayload>) => {
      console.log(`[Worker] Started processing job: ${job.data.jobId}`);
      const { jobId, tenantId } = job.data;

      // 1. Mark processing & broadcast via SSE
      await pool.query(
        `UPDATE jobs SET status = 'processing', updated_at = NOW() WHERE id = $1`,
        [jobId]
      );
      await redisPublisher.publish(
        `job-events:${jobId}`,
        JSON.stringify({ status: 'processing', jobId })
      );

      // 2. Execute LLM inference
      const result = await LLMOrchestrator.executeWithFallback(job.data);

      // 3. Mark completed & broadcast via SSE
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
          jobId,
        ]
      );

      await pool.query(
        `INSERT INTO usage_logs (tenant_id, job_id, provider, model, prompt_tokens, completion_tokens, latency_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [
          tenantId,
          jobId,
          result.providerResolved,
          result.modelResolved,
          result.tokens.prompt,
          result.tokens.completion,
          result.latencyMs,
        ]
      );

      await redisPublisher.publish(
        `job-events:${jobId}`,
        JSON.stringify({
          status: 'completed',
          jobId,
          result: result.content,
          latencyMs: result.latencyMs,
        })
      );

      console.log(`[Worker] Finished job ${jobId}`);
      return result;
    },
    {
      connection: redisOptions, // Pass options object here as well
      concurrency: 5,
    }
  );

  worker.on('active', (job) => {
    console.log(`[Worker] Job active: ${job.id}`);
  });

  worker.on('failed', async (job, err) => {
    if (!job) return;
    const { jobId } = job.data;
    console.error(`[Worker] Job failed: ${jobId}`, err);

    await pool.query(
      `UPDATE jobs SET status = 'failed', error_message = $1, updated_at = NOW() WHERE id = $2`,
      [err.message, jobId]
    );

    await redisPublisher.publish(
      `job-events:${jobId}`,
      JSON.stringify({ status: 'failed', jobId, error: err.message })
    );
  });
}