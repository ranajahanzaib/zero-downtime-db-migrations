# Deployment order

1. Start the disposable Compose database, apply `migrate baseline`, seed deterministic tenants/customers, and run only V1.
2. Confirm V1 health and traffic. Apply `migrate expand`; the nullable columns and `BEFORE` trigger keep V1 reads/writes compatible.
3. Start V2 in `legacy` or `shadow` mode. V2 writes relational values; the trigger mirrors the three migrated fields into JSONB while preserving unrelated keys.
4. Run bounded `backfill` workers. Resolve quarantined records with `cmd/remediate`, then resume the worker.
5. Run `migrate indexes` and `migrate constraints`; confirm catalog state. Generate acknowledged traffic and run `cmd/verify`, then `migrate verify` to record the current PASS gate.
6. Set `ROLLBACK_WINDOW` and run `migrate reads new`; restart V2 instances with `READ_MODE=new`. Observe traffic and keep the bridge installed. Roll back with `migrate reads legacy` and V2 legacy mode if needed.
7. Drain all V1 instances and wait beyond the heartbeat grace period and configured rollback window. Confirm deployment inventory independently; heartbeats cannot detect every disconnected or unknown client.
8. Re-run verification after traffic is quiesced, apply `migrate verify`, then explicitly run `migrate contract --approve`. Contract drops legacy data; ordinary V1 rollback is no longer possible without restore or reconstruction.

`make demo` automates stages 1–7 with bounded readiness polling, but never executes contract. Set `DEMO_CUSTOMERS=100000` for the plan's standard backfill acceptance workload. The demonstration's seeded database and tenant header are not production authentication or deployment inventory.
