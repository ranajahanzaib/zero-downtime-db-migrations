# Failure scenarios

Run these only against the local Compose database or a separately created disposable PostgreSQL database. Never point them at production-like external data. The current repository implements bounded recovery mechanisms and integration coverage for core database failures; not every scenario has a complete automated fault harness.

## A. Backfill worker crash

Run `go run ./cmd/backfill -batch 250 -workers 4 -pause 10ms`, send SIGTERM while it is active, then rerun the same command. Each selected row remains locked until the bounded transaction commits; the marker is committed with extracted values. Completed batches are skipped and interrupted batches are eligible again. Confirm with `go run ./cmd/migrate backfill status` and `go run ./cmd/verify`.

## B. Concurrent updates

Run V1 and V2 concurrently after expansion and issue versioned PATCH requests to the same tenant/customer. For repeatable HTTP overlap, pass the customer ID and tenant ID to the traffic generator's `--shared-customer` and `--shared-tenant` options. One writer with the stale version receives 409. Check the JSONB and relational values and reconcile `reports/write-journal.jsonl` with audit rows using the verifier.

## C. Malformed historical data

The PostgreSQL integration test creates an invalid legacy email alongside valid records, then checks that a valid record is processed and the malformed one is independently recorded in `migration_exceptions`. The normal expansion trigger rejects new malformed writes; introduce malformed historical fixtures only before expansion in a disposable schema. The `cmd/remediate` command repairs a selected quarantined customer's email, records an audit event, and permits a later backfill retry. An unresolved exception blocks verification and contract.

## D. Lock contention

The real-PostgreSQL integration test holds `LOCK TABLE customers IN SHARE MODE` on one session and attempts a conflicting `ALTER TABLE` with a 100 ms lock timeout. It asserts SQLSTATE `55P03`, performs an ordinary read, and releases the lock in cleanup. This exercises PostgreSQL lock behavior rather than mocking it. Migration DDL uses a two-second `lock_timeout`; release application/maintenance blockers before retry.

For manual testing, open a transaction against the local demo, acquire a conflicting table lock, and commit/rollback within a short bounded interval. Do not use an unbounded lock-holder process.

## E. Unsafe early contract

Keep a V1 container running and run `go run ./cmd/migrate contract --approve` only after the normal gates are configured. The V1 heartbeat precondition refuses the operation. Confirm legacy columns still exist. The integration test itself validates the contracted schema's modern insert path; contract precondition refusal is exercised manually in the demo workflow.

## F. Application rollback

Before contract, switch V2 to `legacy` with `go run ./cmd/migrate reads legacy` and restart V2 with `READ_MODE=legacy`. V1 remains compatible because the trigger maintains the legacy representation. Do not attempt V1 rollback after contract.

## G. Interrupted orchestration

Terminate a migration process between commands, inspect `go run ./cmd/migrate history`, and inspect the PostgreSQL catalog. Rerun idempotent additive phases only after checking their actual state. `migrate indexes` explicitly checks `indisvalid` and removes/rebuilds an invalid concurrent index. A contract interrupted after commit is detected from the absent legacy column and recorded as reconciled.

## H. Failed concurrent index build

The runner handles an invalid index left under the expected index name by dropping it concurrently and rebuilding from the repository SQL. It verifies `pg_index.indisvalid`; object existence is insufficient. The integration suite verifies the successful concurrent path. A deterministic cancellation/duplicate-data failed-build fixture is not yet automated.

## Unsafe migration comparison

The explicit `DEMO_UNSAFE_CONFIRM=DROP_LEGACY_COLUMN make unsafe-compare` command first runs the staged demo, then resets the Compose database, runs V1 traffic, drops `contact_details`, captures actual traffic outcomes, and resets the Compose volume again. It refuses a non-local or non-demo `DATABASE_URL`; Compose credentials and database name are pinned to the demo defaults. The reset destroys all data in this repository's Compose volume, so use it only when that volume contains disposable demo data. It does not accept an external database URL.
