# CLAUDE.md

This file provides guidance to Claude Code when working in this repository.

## Project Overview

Stellar ETL is a Go-based data pipeline that extracts data from the Stellar blockchain network and outputs it in JSONL or Parquet format for ingestion into BigQuery. Data flows through three layers: CLI commands (`cmd/`) → data extraction (`internal/input/`) → transformation (`internal/transform/`).

## Build & Test

```bash
# Build
go build

# Unit tests (all transform tests)
go test -v -cover ./internal/transform

# Unit test (single)
go test -v -run ^TestTransformLedger$ ./internal/transform

# Integration tests (requires Docker + GCP credentials)
make int-test

# Integration test (single, with golden file update)
docker-compose build && docker-compose run \
  -v $(HOME)/.config/gcloud/application_default_credentials.json:/usr/credential.json:ro \
  -v $(PWD)/testdata:/usr/src/etl/testdata \
  -e GOOGLE_APPLICATION_CREDENTIALS=/usr/credential.json \
  integration-tests \
  go test -v -run ^TestExportAssets$ ./cmd -timeout 30m -args -update=true

# Lint (golangci-lint + formatters via pre-commit)
make lint
```

## Architecture

### Data Flow

1. **`cmd/`** — Cobra CLI commands (`export_ledgers`, `export_transactions`, etc.). Each command parses flags, calls the `input` package, loops over results calling `transform`, and writes output via `ExportEntry` (JSONL) or `WriteParquet`.

2. **`internal/input/`** — Extracts raw Stellar ledger data. Supports two backends controlled by `--captive-core`:
   - **Default (datastore):** reads compressed `LedgerCloseMetaBatch` XDR files from a GCS bucket.
   - **Captive-core:** runs a local Stellar Core instance.

3. **`internal/transform/`** — Converts raw XDR/history-archive types into BigQuery-compatible structs. Each data type has a `TransformXxx()` function returning an `XxxOutput` struct defined in `schema.go`.

### Output Formats

- **JSONL** (default): one JSON object per line
- **Parquet**: enabled via `--parquet-path` flag; requires `SchemaParquet` interface with a `ToParquet()` method on each output struct (see `schema_parquet.go`)

### IDs

`internal/toid` generates deterministic int64 IDs for ledgers, transactions, and operations using the TOID scheme.

## Key Conventions

### Adding a New Export Command

Four files are required:
- `cmd/export_new_type.go` — Cobra command; follow the pattern in `export_ledgers.go`
- `cmd/export_new_type_test.go` — Integration test using golden files in `testdata/`
- `internal/input/new_type.go` — Data extraction logic
- `internal/transform/new_type.go` — `TransformNewType()` function + `NewTypeOutput` struct added to `schema.go`

### Transform Functions

- Named `TransformXxx(...)` returning `(XxxOutput, error)`
- `XxxOutput` structs use `guregu/null` (`null.Int`, `null.String`, etc.) for nullable fields
- JSON tags align with BigQuery column names

### Error Handling in Export Commands

- Transform errors are non-fatal by default (logged, counted, skipped)
- `--strict-export` flag makes them fatal via `cmdLogger.StrictExport`
- Stats are printed at the end via `PrintTransformStats(attempts, failures)`

### Integration Test Golden Files

Tests in `cmd/` compare output against golden files in `testdata/`. Run with `-update=true` to regenerate golden files when output schemas change.

## Branch Naming

Prefix branches by change type before opening a PR:
- `major/<name>` — breaking changes
- `minor/<name>` — new features
- `patch/<name>` — bug fixes
