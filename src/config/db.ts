import pkg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pkg;

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:postgres@127.0.0.1:5432/ai_gateway_db',
});

export async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tenants (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name VARCHAR(100) NOT NULL,
      api_key VARCHAR(100) UNIQUE NOT NULL,
      daily_token_quota INT DEFAULT 50000,
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    -- Ensure column exists if table was created in an older run
    ALTER TABLE tenants ADD COLUMN IF NOT EXISTS daily_token_quota INT DEFAULT 50000;

    CREATE TABLE IF NOT EXISTS jobs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
      prompt TEXT NOT NULL,
      status VARCHAR(50) DEFAULT 'queued',
      result JSONB,
      provider_resolved VARCHAR(50),
      model_resolved VARCHAR(100),
      error_message TEXT,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS usage_logs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
      job_id UUID REFERENCES jobs(id) ON DELETE SET NULL,
      provider VARCHAR(50) NOT NULL,
      model VARCHAR(100) NOT NULL,
      prompt_tokens INT DEFAULT 0,
      completion_tokens INT DEFAULT 0,
      total_tokens INT DEFAULT 0,
      latency_ms INT DEFAULT 0,
      is_cached BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_usage_logs_tenant_created ON usage_logs(tenant_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_jobs_tenant_id ON jobs(tenant_id);

    -- Default test tenant
    INSERT INTO tenants (id, name, api_key, daily_token_quota)
    VALUES ('a0000000-0000-0000-0000-000000000001', 'Test Free Tier', 'test_free_key', 50000)
    ON CONFLICT (id) DO NOTHING;
  `);
}