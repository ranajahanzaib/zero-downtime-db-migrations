DATABASE_URL ?= postgres://demo:demo@localhost:5432/migrations?sslmode=disable
export DATABASE_URL
.PHONY: up down reset baseline expand backfill verify contract test vet fmt demo dashboard traffic unsafe-compare
up:
	docker compose up -d
down:
	docker compose down
reset:
	docker compose down -v
demo:
	./scripts/demo.sh
unsafe-compare:
	./scripts/unsafe-compare.sh
baseline:
	go run ./cmd/migrate baseline
expand:
	go run ./cmd/migrate expand
backfill:
	go run ./cmd/backfill
verify:
	go run ./cmd/verify
contract:
	go run ./cmd/migrate contract --approve
dashboard:
	go run ./cmd/dashboard
traffic:
	go run ./cmd/traffic -url http://localhost:8081
test:
	go test ./...
vet:
	go vet ./...
fmt:
	test -z "$$(gofmt -l cmd internal)"
