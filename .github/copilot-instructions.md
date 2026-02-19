# GitHub Copilot Instructions for stellar-etl

## Overview

stellar-etl is a Go-based data pipeline that extracts data from the history of the Stellar network. It exports ledger data, transactions, operations, effects, assets, trades, diagnostic events, and ledger entry changes.

## Repository Structure

- `cmd/` – CLI command implementations (one file per export command)
- `internal/input/` – Data extraction logic (from datastores or captive core)
- `internal/transform/` – Data transformation logic; `schema.go` defines output structs
- `internal/utils/` – Shared utility functions
- `docker/` – Dockerfile and related files
- `testdata/` – Golden files used by integration tests

## Build

```sh
go build
```

Build the Docker image locally:

```sh
make docker-build
```

## Testing

### Unit tests

```sh
# Run all unit tests
go test -v -cover ./internal/transform

# Run a single test
go test -v -run ^TestTransformAsset$ ./internal/transform
```

### Integration tests

Integration tests require Docker and GCP credentials (not available in CI without secrets):

```sh
make int-test
```

## Linting

Pre-commit hooks enforce code style (golangci-lint, gofmt, goimports, prettier, and standard hooks):

```sh
pre-commit run --show-diff-on-failure --color=always --all-files
```

## Coding Conventions

- Go version: **1.25.3**
- Follow standard Go formatting (`gofmt`/`goimports`).
- Use the existing `utils` package for shared helpers rather than duplicating logic.
- New export commands should follow the pattern in `cmd/export_ledgers.go`.
- Transformation structs are defined in `internal/transform/schema.go`.
- All new commands need a corresponding test file (`*_test.go`) in the `cmd/` folder and golden test data under `testdata/`.
- Branch naming: prefix with `major/`, `minor/`, or `patch/` depending on the change type.

## Adding New Commands

1. Add `cmd/export_<name>.go` (use `cobra add <command>` to scaffold).
2. Add `cmd/export_<name>_test.go` with CLI tests using `runCLI`.
3. Add `internal/input/<name>.go` for extraction logic.
4. Add `internal/transform/<name>.go` for transformation logic and extend `schema.go` with the new struct.
5. Store test golden files in `testdata/<name>/`.
