// import { Pool } from 'pg';
// import dotenv from 'dotenv';

// dotenv.config();

// export const pool = new Pool({
//   connectionString: process.env.DATABASE_URL,
//   max: 20,
//   idleTimeoutMillis: 30000,
//   connectionTimeoutMillis: 5000,
// });

// export async function initDB(): Promise<void> {
//   const client = await pool.connect();
//   try {
//     await client.query(`
//       CREATE TABLE IF NOT EXISTS tenants (
//         id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//         name VARCHAR(100) NOT NULL,
//         api_key VARCHAR(64) UNIQUE NOT NULL,
//         tier VARCHAR(20) DEFAULT 'free',
//         rpm_limit INT DEFAULT 30,
//         created_at TIMESTAMPTZ DEFAULT NOW()
//       );

//       CREATE TABLE IF NOT EXISTS jobs (
//         id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//         tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
//         status VARCHAR(20) DEFAULT 'queued',
//         prompt TEXT NOT NULL,
//         result JSONB,
//         provider_resolved VARCHAR(50),
//         model_resolved VARCHAR(50),
//         latency_ms INT,
//         error_message TEXT,
//         created_at TIMESTAMPTZ DEFAULT NOW(),
//         updated_at TIMESTAMPTZ DEFAULT NOW()
//       );

//       CREATE TABLE IF NOT EXISTS usage_logs (
//         id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//         tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
//         job_id UUID REFERENCES jobs(id) ON DELETE SET NULL,
//         provider VARCHAR(50) NOT NULL,
//         model VARCHAR(50) NOT NULL,
//         prompt_tokens INT NOT NULL,
//         completion_tokens INT NOT NULL,
//         latency_ms INT NOT NULL,
//         cost_usd NUMERIC(10, 6) DEFAULT 0.000000,
//         created_at TIMESTAMPTZ DEFAULT NOW()
//       );

//       -- Seed a default free-tier tenant key for testing
//       INSERT INTO tenants (id, name, api_key, tier, rpm_limit)
//       VALUES ('a0000000-0000-0000-0000-000000000001', 'Default Free Tenant', 'test_free_key', 'free', 30)
//       ON CONFLICT (api_key) DO NOTHING;
//     `);
//     console.log('[PostgreSQL] Database tables initialized successfully.');
//   } finally {
//     client.release();
//   }
// }
import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

pool.on('error', (err) => {
  console.error('[PostgreSQL Pool Error]:', err);
});

export async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS tenants (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        name VARCHAR(100) NOT NULL,
        api_key VARCHAR(255) UNIQUE NOT NULL,
        rpm_limit INT DEFAULT 10,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
        prompt TEXT NOT NULL,
        status VARCHAR(50) DEFAULT 'queued',
        result JSONB,
        provider_resolved VARCHAR(50),
        model_resolved VARCHAR(100),
        latency_ms INT,
        error_message TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS usage_logs (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
        job_id UUID REFERENCES jobs(id) ON DELETE CASCADE,
        provider VARCHAR(50) NOT NULL,
        model VARCHAR(100) NOT NULL,
        prompt_tokens INT DEFAULT 0,
        completion_tokens INT DEFAULT 0,
        latency_ms INT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // Ensure default test tenant exists
    const seedCheck = await client.query(`SELECT COUNT(*) FROM tenants`);
    if (parseInt(seedCheck.rows[0].count, 10) === 0) {
      await client.query(`
        INSERT INTO tenants (name, api_key, rpm_limit)
        VALUES ('Test Free Tenant', 'test_free_key', 10);
      `);
      console.log('[PostgreSQL] Seeded default tenant with key "test_free_key".');
    }
  } finally {
    client.release();
  }
}