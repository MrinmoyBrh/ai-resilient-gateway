import { Worker, Job } from 'bullmq';
import { redisOptions, redisPublisher } from '../config/redis.js';
import { pool } from '../config/db.js';
import { TelemetryService } from '../services/telemetry.js';
import { LLMOrchestrator } from './orchestrator.js';
import { AIJobPayload } from '../types/index.js';
import { PromptCache } from '../services/cache.js';
import { log } from 'console';



export function startWorker() {
  const worker = new Worker(
    'ai-tasks',
    async (job: Job<AIJobPayload>) => {
      console.log(`[Worker] Started processing job: ${job.data.jobId}`);
      const { jobId, tenantId, prompt } = job.data;

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
      
      // 4. Cache the result into Redis for identical future prompts
      await PromptCache.set(prompt, result.content);
      
      // 5. Insert telemetry usage logs
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

      // 6. Broadcast completion event via Redis Pub/Sub
      await redisPublisher.publish(
        `job-events:${jobId}`,
        JSON.stringify({
          status: 'completed',
          jobId,
          result: result.content,
          latencyMs: result.latencyMs,
        })
      );
      await TelemetryService.recordUsage({
        tenantId: job.data.tenantId,
        jobId: job.data.jobId,
        provider: result.providerResolved,
        model: result.modelResolved,
        promptTokens: result.tokens.prompt,
        completionTokens: result.tokens.completion,
        latencyMs: result.latencyMs,
        isCached: false,
      });

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
    const attemptsMade = job.attemptsMade;
    const maxAttempts = job.opts.attempts || 1;

    console.warn(
    `[Worker] Job ${job.data.jobId} failed attempt ${attemptsMade}/${maxAttempts}: ${err.message}`
  );

  // If retries remain, keep status in retry loop and broadcast retry event
  if (attemptsMade < maxAttempts) {
    await redisPublisher.publish(
      `job-events:${job.data.jobId}`,
      JSON.stringify({
        status: 'retrying',
        jobId: job.data.jobId,
        attempt: attemptsMade,
        maxAttempts,
      })
    );
    return;
  }

  // All attempts exhausted: Move permanently to terminal failed state (DLQ)
  console.error(`[Worker] Job ${job.data.jobId} exhausted all retries. Moving to terminal failed state.`);

    await pool.query(
      `UPDATE jobs SET status = 'failed', error_message = $1, updated_at = NOW() WHERE id = $2`,
      [err.message, job.data.jobId]
    );

    await redisPublisher.publish(
      `job-events:${job.data.jobId}`,
      JSON.stringify({ status: 'failed', jobId: job.data.jobId, error: err.message })
    );
  });
}
