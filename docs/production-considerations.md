# Production considerations

This local reference does not model replicas, replication lag, multi-region writes, billions of rows, external consumers, disaster recovery, connection-pool exhaustion, or a production authentication system. Those require workload-specific capacity tests, replication-aware cutover criteria, deployment inventory beyond this process heartbeat table, backups and restore drills, and monitoring of lock queues and application latency. Synthetic local traffic is not a production availability guarantee.
