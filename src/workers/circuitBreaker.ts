import { redisConnection } from '../config/redis.js';

export type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

interface CircuitBreakerConfig {
  failureThreshold: number; // e.g. 3 consecutive failures
  cooldownPeriodMs: number; // e.g. 30 seconds before attempting HALF_OPEN
}

export class CircuitBreaker {
  private provider: string;
  private failureThreshold: number;
  private cooldownPeriodMs: number;

  constructor(provider: string, config: CircuitBreakerConfig = { failureThreshold: 3, cooldownPeriodMs: 30000 }) {
    this.provider = provider;
    this.failureThreshold = config.failureThreshold;
    this.cooldownPeriodMs = config.cooldownPeriodMs;
  }

  private getKey(suffix: string) {
    return `cb:${this.provider}:${suffix}`;
  }

  async getState(): Promise<CircuitState> {
    const state = await redisConnection.get(this.getKey('state'));
    if (!state || state === 'CLOSED') return 'CLOSED';

    if (state === 'OPEN') {
      const openedAt = Number(await redisConnection.get(this.getKey('opened_at'))) || 0;
      if (Date.now() - openedAt > this.cooldownPeriodMs) {
        // Cooldown passed, allow probe requests
        await redisConnection.set(this.getKey('state'), 'HALF_OPEN');
        return 'HALF_OPEN';
      }
      return 'OPEN';
    }

    return 'HALF_OPEN';
  }

  async recordSuccess() {
    const pipeline = redisConnection.pipeline();
    pipeline.set(this.getKey('state'), 'CLOSED');
    pipeline.del(this.getKey('failures'));
    pipeline.del(this.getKey('opened_at'));
    await pipeline.exec();
  }

  async recordFailure() {
    const failures = await redisConnection.incr(this.getKey('failures'));

    if (failures >= this.failureThreshold) {
      const pipeline = redisConnection.pipeline();
      pipeline.set(this.getKey('state'), 'OPEN');
      pipeline.set(this.getKey('opened_at'), Date.now());
      pipeline.expire(this.getKey('state'), Math.ceil(this.cooldownPeriodMs / 1000) * 2);
      await pipeline.exec();
      console.warn(`[CircuitBreaker] Provider "${this.provider}" TRIPPED to OPEN state!`);
    }
  }
}