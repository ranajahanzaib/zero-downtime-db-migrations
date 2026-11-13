#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"
: "${DATABASE_URL:=postgres://demo:demo@localhost:5432/migrations?sslmode=disable}"
export DATABASE_URL
for pair in "${POSTGRES_USER:-demo}:demo" "${POSTGRES_PASSWORD:-demo}:demo" "${POSTGRES_DB:-migrations}:migrations"; do
  if [[ "${pair%%:*}" != "${pair#*:}" ]]; then
    echo 'Refusing: Compose must use the isolated demo database user, password, and database.' >&2
    exit 2
  fi
done
export POSTGRES_USER=demo POSTGRES_PASSWORD=demo POSTGRES_DB=migrations

if [[ "${DEMO_UNSAFE_CONFIRM:-}" != "DROP_LEGACY_COLUMN" ]]; then
  echo 'Refusing: set DEMO_UNSAFE_CONFIRM=DROP_LEGACY_COLUMN to authorize this destructive local comparison.' >&2
  exit 2
fi
python3 - "$DATABASE_URL" <<'PY'
import sys
from urllib.parse import urlparse
u = urlparse(sys.argv[1])
if u.scheme not in ("postgres", "postgresql") or u.hostname not in ("localhost", "127.0.0.1") or u.port != 5432 or u.path != "/migrations":
    raise SystemExit("Refusing: only localhost:5432/migrations is allowed")
PY

mkdir -p reports
unsafe_started=0
traffic_pid=
cleanup() {
  if [[ -n "$traffic_pid" ]]; then kill "$traffic_pid" 2>/dev/null || true; wait "$traffic_pid" 2>/dev/null || true; fi
  if [[ "$unsafe_started" == 1 ]]; then
    docker compose down -v >/dev/null 2>&1 || true
    docker compose up -d postgres >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

echo 'Measuring the staged workflow first.'
echo 'Resetting the local Compose volume first; existing data in that volume will be destroyed.'
make reset
unsafe_started=1
docker compose up -d postgres
deadline=$((SECONDS + 90))
until docker compose exec -T postgres pg_isready -U demo -d migrations >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then echo 'PostgreSQL readiness timed out' >&2; exit 1; fi
  sleep 1
done
make demo
cp reports/traffic-results.json reports/safe-traffic-results.json

echo 'Resetting only the Compose project for a deliberately unsafe V1 run.'
make reset
unsafe_started=1
docker compose up -d postgres app-v1
deadline=$((SECONDS + 90))
until curl --silent --fail http://localhost:8081/ready >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then echo 'V1 readiness timed out' >&2; exit 1; fi
  sleep 1
done
go run ./cmd/demo seed --customers "${DEMO_CUSTOMERS:-10000}" --seed "${DEMO_SEED:-1}"
go run ./cmd/traffic --url http://localhost:8081 --requests 0 --duration "${UNSAFE_TRAFFIC_DURATION:-20s}" --rate "${DEMO_TRAFFIC_RATE:-25}" --concurrency 8 --journal reports/unsafe-write-journal.jsonl --out reports/unsafe-traffic-results.json &
traffic_pid=$!

# Wait for measured requests to appear before dropping the column V1 needs.
deadline=$((SECONDS + 30))
until docker compose exec -T postgres psql -U demo -d migrations -Atqc 'select count(*) from http_request_metrics' 2>/dev/null | awk '$1 >= 10 { found=1 } END { exit !found }'; do
  if (( SECONDS >= deadline )); then echo 'No request metrics appeared before the deadline' >&2; exit 1; fi
  sleep 1
done
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U demo -d migrations -c 'ALTER TABLE customers DROP COLUMN contact_details'
wait "$traffic_pid"
traffic_pid=

python3 - <<'PY'
import json
from pathlib import Path

def load(path):
    try:
        return json.loads(Path("reports", path).read_text())
    except (OSError, json.JSONDecodeError):
        return {}

safe, unsafe = load("safe-traffic-results.json"), load("unsafe-traffic-results.json")
metrics = [("Requests", "requests_sent"), ("Successful responses", "successful_responses"), ("HTTP 5xx", "http_5xx"), ("Timeouts", "timeouts"), ("p95 latency ms", "p95_latency_ms"), ("p99 latency ms", "p99_latency_ms")]
def cell(data, key):
    v = data.get(key, "unavailable")
    return f"{v:.2f}" if isinstance(v, float) else str(v)
lines = ["# Migration strategy measurements", "", "Actual traffic-generator results for these two disposable local runs; no outcome is hardcoded.", "", "| Metric | Staged | Unsafe |", "|---|---:|---:|"]
lines += [f"| {label} | {cell(safe,key)} | {cell(unsafe,key)} |" for label,key in metrics]
Path("reports/migration-comparison.md").write_text("\n".join(lines)+"\n")
Path("reports/migration-comparison.json").write_text(json.dumps({"staged":safe,"unsafe":unsafe}, indent=2)+"\n")
print("\n".join(lines))
PY

echo 'The Compose volume will now be reset and a clean baseline PostgreSQL service restored.'
