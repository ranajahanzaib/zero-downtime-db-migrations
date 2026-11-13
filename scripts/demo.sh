#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"
: "${DATABASE_URL:=postgres://demo:demo@localhost:5432/migrations?sslmode=disable}"
export DATABASE_URL

case "$DATABASE_URL" in
  *localhost:5432/migrations*|*127.0.0.1:5432/migrations*) ;;
  *) printf '%s\n' 'make demo only runs against the local migrations demo database on port 5432.' >&2; exit 2 ;;
esac

mkdir -p reports
rm -f reports/write-journal.jsonl reports/traffic-results.json reports/traffic-v1.json reports/verification.json reports/verification.md reports/migration-report.json reports/migration-report.md
demo_bin_dir="$(mktemp -d "${TMPDIR:-/tmp}/zdmm-demo.XXXXXX")"
go build -o "$demo_bin_dir/traffic" ./cmd/traffic
go build -o "$demo_bin_dir/backfill" ./cmd/backfill
traffic_pids=()
cleanup() {
  for pid in "${traffic_pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$demo_bin_dir"
}
trap cleanup EXIT INT TERM

wait_ready() {
  local url="$1" deadline=$((SECONDS + 60))
  until curl --silent --show-error --fail "$url/ready" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then printf 'readiness deadline expired: %s\n' "$url" >&2; return 1; fi
    sleep 1
  done
}

docker compose up -d postgres app-v1 app-v1-b dashboard
wait_ready http://localhost:8081
wait_ready http://localhost:8083
go run ./cmd/migrate baseline
go run ./cmd/demo seed --customers "${DEMO_CUSTOMERS:-10000}" --seed "${DEMO_SEED:-1}"

# V1 remains under real HTTP load while expansion and V2 startup occur.
"$demo_bin_dir/traffic" --url http://localhost:8081 --requests 0 --duration 25s --rate 20 --concurrency 8 --journal reports/write-journal.jsonl --out reports/traffic-v1.json &
traffic_pids+=("$!")
go run ./cmd/migrate expand
docker compose --profile v2 up -d --build app-v2 app-v2-b
wait_ready http://localhost:8082
wait_ready http://localhost:8084

# Mixed traffic continues while independent backfill workers process locked batches.
"$demo_bin_dir/traffic" --v1-url http://localhost:8081,http://localhost:8083 --v2-url http://localhost:8082,http://localhost:8084 --requests 0 --duration "${DEMO_TRAFFIC_DURATION:-60s}" --rate "${DEMO_TRAFFIC_RATE:-25}" --concurrency 16 --seed "${DEMO_SEED:-1}" --journal reports/write-journal.jsonl --out reports/traffic-results.json &
traffic_pids+=("$!")
"$demo_bin_dir/backfill" --batch "${BACKFILL_BATCH_SIZE:-250}" --workers "${BACKFILL_WORKERS:-4}" --pause 10ms &
backfill_pid=$!
wait "${traffic_pids[0]}"
wait "${traffic_pids[1]}"
traffic_pids=()
wait "$backfill_pid"

go run ./cmd/migrate indexes
go run ./cmd/migrate constraints
go run ./cmd/verify
go run ./cmd/migrate verify
ROLLBACK_WINDOW="${ROLLBACK_WINDOW:-15s}" go run ./cmd/migrate reads new

# Restart V2 only after the verifier gates new-column reads.
V2_READ_MODE=new docker compose --profile v2 up -d --force-recreate app-v2 app-v2-b
wait_ready http://localhost:8082
wait_ready http://localhost:8084
"$demo_bin_dir/traffic" --v1-url http://localhost:8081,http://localhost:8083 --v2-url http://localhost:8082,http://localhost:8084 --requests 0 --duration "${DEMO_OBSERVE_DURATION:-15s}" --rate "${DEMO_TRAFFIC_RATE:-25}" --concurrency 16 --seed "${DEMO_SEED:-1}" --journal reports/write-journal.jsonl --out reports/traffic-results.json

# Drain V1 explicitly. Graceful shutdown marks its registered instances drained.
docker compose stop app-v1 app-v1-b
go run ./cmd/verify
go run ./cmd/migrate verify
go run ./cmd/migrate status
printf '%s\n' 'Safe demo complete. Contract was intentionally not run.'
