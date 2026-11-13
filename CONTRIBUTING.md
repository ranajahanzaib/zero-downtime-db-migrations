# Contributing

Contributions that improve the examples, documentation, or tooling are welcome. For substantial changes, open an issue first so the proposed behavior can be discussed before implementation.

## Development setup

Install Go 1.24 or later. Docker Compose is required for PostgreSQL integration checks and the end-to-end demo.

```sh
git clone https://github.com/ranajahanzaib/zero-downtime-db-migrations.git
cd zero-downtime-db-migrations
cp .env.example .env
make up
```

The credentials in `.env.example` are fixed for the disposable local demo database. Do not reuse them outside that environment or put real credentials in commits, issues, or pull requests.

## Making a change

Create a focused branch, follow the existing Go and SQL style, and add or update tests for behavior changes. Keep migration phases safe to retry where possible, document irreversible or operationally significant behavior, and preserve the distinction between local synthetic evidence and production guarantees.

Before opening a pull request, run the checks relevant to the change:

```sh
make fmt
go test ./...
go test -race ./...
go vet ./...
docker compose config -q
```

To run the real PostgreSQL integration test, start a disposable PostgreSQL database and set `RUN_POSTGRES_TESTS=1` and `DATABASE_URL` for that database before running `go test ./test/integration -run TestRealPostgresMigrationLifecycle -count=1`.

## Pull requests and issues

Pull requests should describe the problem, the change, and the validation performed. Call out migration compatibility, data-loss risks, and any limitations that affect operators. Link related issues where applicable. Use the issue templates for reproducible bug reports and focused feature proposals.

Please report security vulnerabilities through the private process in [SECURITY.md](.github/SECURITY.md), not through a public issue.
