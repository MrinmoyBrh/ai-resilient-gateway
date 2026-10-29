import { Queue } from 'bullmq';
import { redisConnection } from '../config/redis.js';
import { AIJobPayload } from '../types/index.js';

export const AI_QUEUE_NAME = 'ai-orchestration-queue';

export const aiQueue = new Queue('ai-tasks', {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 2000, // 2s, then 4s, then 8s
    },
    removeOnComplete: { count: 1000 },
    removeOnFail: { count: 5000 },
  },
});