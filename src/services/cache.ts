import crypto from 'node:crypto';
import { redisConnection } from '../config/redis.js';

const CACHE_TTL_SECONDS = 3600; // 1 hour TTL

export class PromptCache {
  /**
   * Generates a deterministic hash from a normalized prompt.
   * Trims whitespace and converts to lowercase to catch near-identical requests.
   */
  static getCacheKey(prompt: string): string {
    const normalized = prompt.trim().toLowerCase();
    const hash = crypto.createHash('sha256').update(normalized).digest('hex');
    return `cache:prompt:${hash}`;
  }

  /**
   * Look up an existing cached response.
   */
  static async get(prompt: string): Promise<any | null> {
    const key = this.getCacheKey(prompt);
    const data = await redisConnection.get(key);
    if (!data) return null;
    return JSON.parse(data);
  }

  /**
   * Store a completed prompt response with an expiration TTL.
   */
  static async set(prompt: string, payload: any): Promise<void> {
    const key = this.getCacheKey(prompt);
    await redisConnection.setex(key, CACHE_TTL_SECONDS, JSON.stringify(payload));
  }
  static async get(prompt: string): Promise<any | null> {
  const key = this.getCacheKey(prompt);
  const data = await redisConnection.get(key);
  console.log(`[Cache GET] Key: ${key} -> Hit: ${!!data}`);
  if (!data) return null;
  return JSON.parse(data);
 }

  static async set(prompt: string, payload: any): Promise<void> {
   const key = this.getCacheKey(prompt);
   console.log(`[Cache SET] Storing key: ${key}`);
   await redisConnection.setex(key, CACHE_TTL_SECONDS, JSON.stringify(payload));
}
}
 