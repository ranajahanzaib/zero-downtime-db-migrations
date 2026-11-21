# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- V1 `PATCH /api/customers/{id}` failed with HTTP 500 (`operator is not unique: unknown -> unknown`); the JSON parameter is now cast to `jsonb`. Added an integration assertion.
- Docker image build failed because the Dockerfile and CI used Go 1.24 while `go.mod` requires Go 1.25.
- Traffic generator counted the request in flight at the end of a run as a timeout/network failure.
- `make demo` labelled V1-only traffic `unknown` (verifier reported lost writes), let two traffic phases overlap, and reused stale images; it now labels, sequences, and rebuilds correctly.
