# Rollback and recovery playbook

## Before contract

- **Expansion failure:** Keep V1 serving. Inspect `migrate history` and actual catalog state. The runner uses short lock timeouts and can retry additive DDL. Confirm column definitions and trigger presence before rerunning; `IF NOT EXISTS` alone is not proof of matching state.
- **Compatibility trigger failure:** Keep V1, stop V2 writes if needed, and inspect SQLSTATE and migration history. Repair the trigger SQL and reapply only after checking current columns. Do not drop JSONB or disable the bridge while V1 clients remain.
- **Backfill interruption:** Stop/restart `go run ./cmd/backfill` (or terminate one worker). Each batch is one transaction; committed markers are durable, while interrupted uncommitted writes roll back. Run `migrate backfill status` and resume. The worker quarantines malformed rows independently.
- **Malformed data:** Inspect `migration_exceptions` and use `go run ./cmd/remediate --help` for a specific customer ID. Remediation repairs the JSONB email, records audit evidence, resolves that exception, and the backfill can resume. Verify before cutover.
- **Index or constraint failure:** Check `pg_index.indisvalid` and `pg_constraint.convalidated`. `migrate indexes` removes an invalid tenant/email index concurrently and rebuilds it. Fix invalid source rows before retrying constraint validation. Both commands retain failure status in migration history.
- **Verification failure:** Do not switch reads. Inspect `reports/verification.json`, reconcile journal/audit mismatches, fix the cause, and rerun the verifier. Its report is point-in-time and must be refreshed after later writes.
- **Read-switch failure:** Before contract, run `go run ./cmd/migrate reads legacy`, then restart V2 with `READ_MODE=legacy`. Keep the compatibility trigger and V1 available while investigating. New-read startup checks verification, marker consistency, index, and validated constraint.
- **Lock contention:** Let the bounded DDL attempt fail with SQLSTATE `55P03`, inspect blockers, allow the transaction to finish, then retry. Do not increase timeouts without assessing application impact.
- **Orchestrator interruption:** Compare migration history to catalog state. For the concurrent index, catalog validity is authoritative. The contract command reconciles an already-dropped legacy column to avoid claiming that V1 remains supported.

## Contract boundary

Before contract, V1 can read/write the preserved JSONB through the trigger, and V2 can return to legacy reads. Contract requires explicit `--approve`, a fresh PASS, no active V1 heartbeat, V2 new reads, no unresolved exceptions, valid schema gates, and an expired rollback window. It drops the trigger and legacy columns without `CASCADE` in a bounded transaction.

After contract, V1 is incompatible and the old JSONB document is gone. There is no truthful SQL `down` migration that recreates historical JSONB semantics. Recovery options are restoring a verified backup or preserved snapshot, reconstructing the legacy document from relational fields where semantics permit, or rolling forward with corrected V2 code. Verify the chosen recovery against audit and application semantics before directing traffic.

The heartbeat registry cannot prove there are no unknown or disconnected clients. Production approval also needs external deployment inventory, backup validation, and an operator-reviewed rollback decision.
