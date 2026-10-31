import { pool } from '../config/db.js';

interface LogUsageParams {
  tenantId: string;
  jobId: string;
  provider: string;
  model: string;
  promptTokens: number;
  completionTokens: number;
  latencyMs: number;
  isCached?: boolean;
}

export class TelemetryService {
  static async recordUsage(params: LogUsageParams): Promise<void> {
    const totalTokens = (params.promptTokens || 0) + (params.completionTokens || 0);

    await pool.query(
      `INSERT INTO usage_logs (
        tenant_id, job_id, provider, model, 
        prompt_tokens, completion_tokens, total_tokens, 
        latency_ms, is_cached
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        params.tenantId,
        params.jobId,
        params.provider,
        params.model,
        params.promptTokens || 0,
        params.completionTokens || 0,
        totalTokens,
        params.latencyMs || 0,
        params.isCached || false,
      ]
    );
  }
  static async checkQuota(tenantId: string): Promise<{ allowed: boolean; used: number; quota: number }> {
    const query = `
        SELECT 
            t.daily_token_quota,
            COALESCE(SUM(u.total_tokens), 0)::int AS used_tokens
        FROM tenants t
        LEFT JOIN usage_logs u ON t.id = u.tenant_id 
            AND u.created_at >= NOW() - INTERVAL '24 hours'
        WHERE t.id = $1
        GROUP BY t.id, t.daily_token_quota;
    `;

    const res = await pool.query(query, [tenantId]);
    if (res.rows.length === 0) {
        return { allowed: true, used: 0, quota: 50000 };
    }

    const { daily_token_quota, used_tokens } = res.rows[0];
    return {
        allowed: used_tokens < daily_token_quota,
        used: used_tokens,
        quota: daily_token_quota,
    };
  }
  
  static async checkQuota(tenantId: string): Promise<{ allowed: boolean; used: number; quota: number }> {
    const query = `
        SELECT 
            t.daily_token_quota,
            COALESCE(SUM(COALESCE(u.prompt_tokens, 0) + COALESCE(u.completion_tokens, 0)), 0)::int AS used_tokens
        FROM tenants t
        LEFT JOIN usage_logs u ON t.id = u.tenant_id 
            AND u.created_at >= NOW() - INTERVAL '24 hours'
        WHERE t.id = $1
        GROUP BY t.id, t.daily_token_quota;
    `;

    const res = await pool.query(query, [tenantId]);
    if (res.rows.length === 0) {
        return { allowed: true, used: 0, quota: 50000 };
    }

    const { daily_token_quota, used_tokens } = res.rows[0];
    return {
        allowed: used_tokens < daily_token_quota,
        used: used_tokens,
        quota: daily_token_quota,
    };
  }
  static async getTenantAnalytics(tenantId: string) {
    const summaryQuery = `
      SELECT 
        COUNT(*)::int AS total_requests,
        COUNT(*) FILTER (WHERE is_cached = TRUE)::int AS cached_requests,
        COALESCE(SUM(total_tokens), 0)::int AS total_tokens,
        COALESCE(SUM(prompt_tokens), 0)::int AS total_prompt_tokens,
        COALESCE(SUM(completion_tokens), 0)::int AS total_completion_tokens,
        COALESCE(ROUND(AVG(latency_ms)), 0)::int AS avg_latency_ms
      FROM usage_logs
      WHERE tenant_id = $1;
    `;

    const providerQuery = `
      SELECT 
        provider,
        COUNT(*)::int AS request_count,
        COALESCE(SUM(total_tokens), 0)::int AS tokens_used,
        COALESCE(ROUND(AVG(latency_ms)), 0)::int AS avg_latency_ms
      FROM usage_logs
      WHERE tenant_id = $1
      GROUP BY provider;
    `;

    const tenantQuery = `
      SELECT id, name, daily_token_quota, is_active
      FROM tenants
      WHERE id = $1;
    `;

    const [summaryRes, providerRes, tenantRes] = await Promise.all([
      pool.query(summaryQuery, [tenantId]),
      pool.query(providerQuery, [tenantId]),
      pool.query(tenantQuery, [tenantId]),
    ]);

    return {
      tenant: tenantRes.rows[0] || null,
      summary: summaryRes.rows[0] || {
        total_requests: 0,
        cached_requests: 0,
        total_tokens: 0,
        total_prompt_tokens: 0,
        total_completion_tokens: 0,
        avg_latency_ms: 0,
      },
      providers: providerRes.rows,
    };
  }
}