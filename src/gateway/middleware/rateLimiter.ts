import { FastifyRequest, FastifyReply } from 'fastify';
import { redisConnection } from '../../config/redis.js';
import { pool } from '../../config/db.js';

export async function tenantRateLimiter(req: FastifyRequest, reply: FastifyReply) {
  const apiKey = req.headers['x-api-key'] as string;

  if (!apiKey) {
    return reply.status(401).send({ error: 'Missing x-api-key header' });
  }

  // 1. Fetch tenant limits from PostgreSQL
  const tenantRes = await pool.query(
    `SELECT id, name, rpm_limit FROM tenants WHERE api_key = $1`,
    [apiKey]
  );

  if (tenantRes.rows.length === 0) {
    return reply.status(403).send({ error: 'Invalid API key' });
  }

  const tenant = tenantRes.rows[0];
  const rpmLimit = tenant.rpm_limit || 10; // Default fallback to 10 RPM
  const windowMs = 60 * 1000; // 60-second sliding window
  const now = Date.now();
  const clearBefore = now - windowMs;

  const key = `rate_limit:${tenant.id}`;

  // 2. Atomic Redis Pipeline using Sorted Sets (ZSET)
  const pipeline = redisConnection.pipeline();
  pipeline.zremrangebyscore(key, 0, clearBefore); // Evict requests outside window
  pipeline.zcard(key);                           // Count valid requests in current window
  pipeline.zadd(key, now, `${now}:${Math.random()}`); // Record current request timestamp
  pipeline.expire(key, 60);                      // Auto-expire idle keys

  const results = await pipeline.exec();

  if (!results) {
    return; // Fall through on Redis error to maintain availability
  }

  const currentCount = (results[1][1] as number) || 0;

  // Set standard rate limit headers
  reply.header('X-RateLimit-Limit', rpmLimit);
  reply.header('X-RateLimit-Remaining', Math.max(0, rpmLimit - currentCount));

  // 3. Exceeded check
  if (currentCount >= rpmLimit) {
    return reply.status(429).send({
      statusCode: 429,
      error: 'Too Many Requests',
      message: `Rate limit of ${rpmLimit} requests per minute exceeded. Please retry after the sliding window cools down.`,
      tenant: tenant.name,
    });
  }

  // Attach tenant to request context for downstream handlers
  (req as any).tenant = tenant;
}