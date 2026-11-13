CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS customers (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID NOT NULL,
 company_name TEXT NOT NULL, contact_details JSONB NOT NULL,
 version BIGINT NOT NULL DEFAULT 1, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS customers_tenant_created_idx ON customers(tenant_id, created_at DESC, id);
CREATE TABLE IF NOT EXISTS migration_history (
 id BIGSERIAL PRIMARY KEY, migration_id TEXT NOT NULL UNIQUE, started_at TIMESTAMPTZ NOT NULL,
 finished_at TIMESTAMPTZ, duration_ms BIGINT, status TEXT NOT NULL, error_code TEXT,
 error_detail TEXT, lock_waits BIGINT NOT NULL DEFAULT 0, schema_fingerprint TEXT,
 verification_result JSONB, operator_approval BOOLEAN NOT NULL DEFAULT FALSE,
 server_version TEXT NOT NULL DEFAULT version()
);
ALTER TABLE migration_history ADD COLUMN IF NOT EXISTS error_detail TEXT;
ALTER TABLE migration_history ADD COLUMN IF NOT EXISTS lock_waits BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_history ADD COLUMN IF NOT EXISTS schema_fingerprint TEXT;
ALTER TABLE migration_history ADD COLUMN IF NOT EXISTS verification_result JSONB;
ALTER TABLE migration_history ADD COLUMN IF NOT EXISTS operator_approval BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE IF NOT EXISTS migration_exceptions (
 customer_id UUID PRIMARY KEY, tenant_id UUID NOT NULL, category TEXT NOT NULL,
 detail TEXT NOT NULL, failed_at TIMESTAMPTZ NOT NULL DEFAULT now(), resolved_at TIMESTAMPTZ,
 retry_count INTEGER NOT NULL DEFAULT 0, source_fingerprint TEXT, resolution_status TEXT NOT NULL DEFAULT 'open'
);
ALTER TABLE migration_exceptions ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE migration_exceptions ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;
ALTER TABLE migration_exceptions ADD COLUMN IF NOT EXISTS resolution_status TEXT NOT NULL DEFAULT 'open';
CREATE TABLE IF NOT EXISTS application_instances (
 instance_id TEXT PRIMARY KEY, app_version TEXT NOT NULL, read_mode TEXT NOT NULL DEFAULT 'legacy',
 write_mode TEXT NOT NULL DEFAULT 'legacy', deployment_status TEXT NOT NULL DEFAULT 'active',
 last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE application_instances ADD COLUMN IF NOT EXISTS read_mode TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE application_instances ADD COLUMN IF NOT EXISTS write_mode TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE application_instances ADD COLUMN IF NOT EXISTS deployment_status TEXT NOT NULL DEFAULT 'active';
CREATE TABLE IF NOT EXISTS audit_events (
 id BIGSERIAL PRIMARY KEY, operation_id UUID NOT NULL UNIQUE, tenant_id UUID NOT NULL, customer_id UUID NOT NULL,
 app_version TEXT NOT NULL, action TEXT NOT NULL, previous_version BIGINT NOT NULL, record_version BIGINT NOT NULL,
 contact_checksum TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS operation_id UUID;
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS previous_version BIGINT;
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS contact_checksum TEXT;
CREATE TABLE IF NOT EXISTS migration_control (
 id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id), phase TEXT NOT NULL DEFAULT 'baseline',
 status TEXT NOT NULL DEFAULT 'succeeded', read_mode TEXT NOT NULL DEFAULT 'legacy',
 verification_status TEXT NOT NULL DEFAULT 'pending', verification_result JSONB,
 contract_approved_at TIMESTAMPTZ,
 rollback_after TIMESTAMPTZ, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO migration_control(id) VALUES(TRUE) ON CONFLICT(id) DO NOTHING;
ALTER TABLE migration_control ADD COLUMN IF NOT EXISTS verification_result JSONB;
CREATE TABLE IF NOT EXISTS migration_events (
 id BIGSERIAL PRIMARY KEY, event_type TEXT NOT NULL, status TEXT NOT NULL,
 details JSONB NOT NULL DEFAULT '{}'::jsonb, occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS demo_datasets (
 id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(id), seed BIGINT NOT NULL,
 seeded_customers BIGINT NOT NULL, tenant_count INTEGER NOT NULL, config JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS backfill_runs (
 id BIGSERIAL PRIMARY KEY, worker_id TEXT NOT NULL, started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 finished_at TIMESTAMPTZ, batches BIGINT NOT NULL DEFAULT 0, rows_migrated BIGINT NOT NULL DEFAULT 0,
 rows_invalid BIGINT NOT NULL DEFAULT 0, errors BIGINT NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'running',
 batch_size INTEGER NOT NULL DEFAULT 250, worker_count INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS http_request_metrics (
 id BIGSERIAL PRIMARY KEY, occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 app_version TEXT NOT NULL, method TEXT NOT NULL, route TEXT NOT NULL,
 status_code INTEGER NOT NULL, duration_ms DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS http_request_metrics_time_idx ON http_request_metrics(occurred_at DESC);
