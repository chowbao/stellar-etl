# CLAUDE.md — Context for Claude Code Agents

This file provides context for [Claude Code](https://claude.ai/code) agents working on the `stellar-etl` repository. It was generated via an autonomous agentic loop using [Open Ralph Wiggum](https://github.com/Th0rgal/open-ralph-wiggum) to demonstrate that Ralph and Claude Code work together correctly.

## Project Overview

**Stellar ETL** is a Go data pipeline that extracts, transforms, and loads data from the Stellar blockchain network history into BigQuery.

## Build & Test

```sh
# Build
go build

# Unit tests (all)
go test -v -cover ./internal/transform

# Unit test (single)
go test -v -run ^TestTransformLedger$ ./internal/transform

# Integration tests (requires Docker + GCP credentials)
make int-test

# Lint
make lint
```

## Architecture

Data flows through three layers:

1. **`cmd/`** — Cobra CLI commands (`export_ledgers`, `export_transactions`, etc.). Each command parses flags, calls the `input` package, loops over results calling `transform`, and writes output via `ExportEntry` (JSONL) or `WriteParquet`.

2. **`internal/input/`** — Extracts raw Stellar ledger data. Supports two backends:
   - **Default (datastore):** reads compressed `LedgerCloseMetaBatch` XDR files from GCS
   - **Captive-core:** runs a local Stellar Core instance

3. **`internal/transform/`** — Converts raw XDR types into BigQuery-compatible structs. Each type has a `TransformXxx()` function returning an `XxxOutput` struct.

**Output formats:** JSONL (default) or Parquet (`--parquet-path` flag).

## Key Conventions

- Transform functions: `TransformXxx(...)` returning `(XxxOutput, error)`
- Nullable fields use `guregu/null` (`null.Int`, `null.String`, etc.)
- Transform errors are non-fatal by default; use `--strict-export` to make them fatal
- Branch naming: `major/<name>`, `minor/<name>`, `patch/<name>`

## Claude Code GitHub Actions

This repo has Claude Code GitHub Actions configured:

- **`claude.yml`** — Responds to `@claude` mentions in issues/PRs/comments
- **`claude-code-review.yml`** — Automatically reviews PRs using Claude Code

To trigger Claude Code on a PR, comment `@claude <instruction>`.
