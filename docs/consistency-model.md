# Consistency model

The JSONB document remains the legacy representation through the compatibility window. The bridge trigger synchronizes only `email`, `phone`, and `timezone`, preserving unrelated keys. Missing/null optional values normalize to SQL NULL; required email cannot be removed by a modern update. A statement changing both representations to different values is rejected.

Applications update using `version = expected_version`; the database row lock serializes writers and a stale version returns HTTP 409. Backfill selects and locks each row in the same transaction that validates and writes the extracted values. Therefore a concurrent application update either commits first and the backfill sees its current row, or waits until the batch releases its lock; it cannot be overwritten from an earlier read.

Verification compares all extracted fields and migration markers. It is a point-in-time report, not a distributed transaction across app instances. Invalid rows are recorded in `migration_exceptions` and block a clean cutover. The local demo header is tenant scoping only and is not authentication.
