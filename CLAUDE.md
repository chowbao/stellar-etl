# CLAUDE.md

## Project Overview

Stellar-ETL is a Go-based data pipeline that extracts data from the Stellar blockchain network's historical ledger records. It reads compressed `LedgerCloseMetaBatch` XDR binary files from a GCS datastore (or optionally via captive-core) and transforms them into BigQuery-compatible JSON (JSONL) or Parquet output.

**Module path:** `github.com/stellar/stellar-etl/v2`
**Go version:** 1.25.3
**License:** Apache 2.0

## Build & Test Commands

```sh
# Build
go build

# Unit tests (all transform tests)
go test -v -cover ./internal/transform

# Unit tests (all internal packages)
go test -v -cover ./internal/...

# Single unit test
go test -v -run ^TestTransformLedger$ ./internal/transform

# Integration tests (requires Docker + GCP credentials)
make int-test

# Integration tests with golden file update
make int-test-update

# Lint (runs golangci-lint + formatters via pre-commit)
make lint

# Docker build
make docker-build
```

## Architecture

Data flows through three layers:

1. **`cmd/`** -- Cobra CLI commands (`export_ledgers`, `export_transactions`, etc.). Each command parses flags, calls the `input` package, loops over results calling `transform`, and writes output via `ExportEntry` (JSONL) or `WriteParquet`.

2. **`internal/input/`** -- Extracts raw Stellar ledger data. Two backends controlled by `--captive-core`:
   - **Default (datastore):** reads compressed `LedgerCloseMetaBatch` XDR files from a GCS bucket.
   - **Captive-core (deprecated):** runs a local Stellar Core instance.

3. **`internal/transform/`** -- Converts raw XDR/history-archive types into BigQuery-compatible structs. Each data type has a `TransformXxx()` function returning an `XxxOutput` struct defined in `schema.go`.

**Supporting packages:**
- **`internal/toid/`** -- Generates deterministic int64 IDs for ledgers, transactions, and operations using the TOID scheme.
- **`internal/utils/`** -- Shared utilities: flag parsing (`MustCommonFlags`, `MustArchiveFlags`, etc.), XDR helpers, environment/network config, logger, and datastore creation.

**Output formats:** JSONL (default, one JSON object per line) or Parquet (`--write-parquet` flag). Parquet support uses the `SchemaParquet` interface with `ToParquet()` on each output struct (see `schema_parquet.go`).

## Directory Structure

```
stellar-etl/
  main.go                    # Entry point, calls cmd.Execute()
  cmd/                       # Cobra CLI commands and test utilities
    root.go                  # Root command, viper config init
    command_utils.go         # ExportEntry, WriteParquet, PrintTransformStats, MaybeUpload
    export_ledgers.go        # export_ledgers command (pattern for all exports)
    export_transactions.go
    export_operations.go
    export_effects.go
    export_assets.go
    export_trades.go
    export_contract_events.go
    export_ledger_entry_changes.go  # Most complex: batch export with type filters
    export_ledger_transaction.go
    export_token_transfers.go
    get_ledger_range_from_times.go  # Utility command
    upload_to_gcs.go         # GCS upload logic
    version.go               # Version command
    test_utils.go            # RunCLITest, golden file helpers
    *_test.go                # Integration tests using golden files
  internal/
    input/                   # Data extraction from ledger backends
      ledgers.go             # GetLedgers (datastore)
      ledgers_history_archive.go  # GetLedgersHistoryArchive (captive-core)
      transactions.go
      operations.go
      trades.go
      assets.go / assets_history_archive.go
      changes.go / changes_test.go
      all_history.go
      ledger_range.go
      orderbooks.go
    transform/               # XDR -> BigQuery-compatible struct conversion
      schema.go              # All XxxOutput struct definitions (JSON tags)
      schema_parquet.go      # All XxxOutputParquet struct definitions (Parquet tags)
      parquet_converter.go   # ToParquet() implementations
      ledger.go / ledger_test.go
      transaction.go / transaction_test.go
      operation.go / operation_test.go
      effects.go / effects_test.go
      trade.go / trade_test.go
      asset.go / asset_test.go
      account.go / account_signer.go
      claimable_balance.go / offer.go / offer_normalized.go
      trustline.go / liquidity_pool.go
      contract_code.go / contract_data.go / contract_events.go
      config_setting.go / ttl.go / restored_key.go
      token_transfer.go
      ledger_transaction.go
      test_variables_test.go  # Shared test fixtures for transform tests
    toid/                    # Transaction/operation ID generation
      main.go / main_test.go
      synt_offer_id.go
    utils/                   # Shared utilities
      main.go                # Flag helpers, XDR utils, environment config, datastore creation
      logger.go              # EtlLogger with StrictExport mode
  docker/
    Dockerfile               # Multi-stage production build (CGO enabled)
    Dockerfile.test          # Test image for integration tests
    stellar-core.cfg         # Mainnet captive-core config
    stellar-core_testnet.cfg
    stellar-core_futurenet.cfg
  testdata/                  # Golden files for integration tests
    ledgers/ transactions/ operations/ effects/ assets/ trades/
    contract_events/ ledger_transactions/ token_transfers/
    changes/ accounts/ trustlines/ offers/ claimable_balances/
    signers/ orderbooks/ ranges/
  docs/
    backfill.md              # BigQuery backfill guide using JS UDFs
  .github/
    workflows/
      build.yml              # Docker build + push (manual trigger)
      integration-tests.yml  # Integration tests on PR/push to master
      internal.yml           # Build + unit tests for internal/ on PR
      lint-tests.yml         # Pre-commit linting on PR
      codeql.yml             # CodeQL security analysis
      release.yml            # Auto-version + release on PR merge to master
      claude.yml             # Claude Code Action for issue/PR comments
      claude-code-review.yml # Claude Code review automation
      copilot-setup-steps.yml
    CODEOWNERS               # @stellar/data-eng owns everything
    pull_request_template.md
    release-drafter.yml
```

## Key Conventions

### Adding a new export command

Four files are required:
1. `cmd/export_new_type.go` -- Cobra command; follow the pattern in `export_ledgers.go`
2. `cmd/export_new_type_test.go` -- Integration test using golden files in `testdata/`
3. `internal/input/new_type.go` -- Data extraction logic
4. `internal/transform/new_type.go` -- `TransformNewType()` function + `NewTypeOutput` struct added to `schema.go`

If Parquet support is needed, also add a `NewTypeOutputParquet` struct to `schema_parquet.go` and a `ToParquet()` method.

### Transform functions

- Named `TransformXxx(...)` returning `(XxxOutput, error)`
- `XxxOutput` structs use `guregu/null` (`null.Int`, `null.String`) for nullable fields, and `zero` package for zero-value nullable types
- JSON tags must align with BigQuery column names
- Parquet structs are separate types with `parquet:` struct tags

### Error handling in export commands

- Transform errors are **non-fatal by default** (logged, counted, skipped)
- `--strict-export` flag makes them fatal via `cmdLogger.StrictExport` / `cmdLogger.LogError()`
- Stats are printed at the end via `PrintTransformStats(attempts, failures)`

### CLI flag patterns

- Common flags are added via `utils.AddCommonFlags()` and read via `utils.MustCommonFlags()`
- Archive-specific flags: `utils.AddArchiveFlags()` / `utils.MustArchiveFlags()`
- Cloud storage flags: `utils.AddCloudStorageFlags()` / `utils.MustCloudStorageFlags()`
- Core/batch flags: `utils.AddCoreFlags()` / `utils.MustCoreFlags()`
- Export type filters: `utils.AddExportTypeFlags()` / `utils.MustExportTypeFlags()`

### Integration test golden files

Tests in `cmd/` use `RunCLITest()` which compares CLI output against golden files in `testdata/`. Run with `-update=true` (via `make int-test-update`) to regenerate golden files when output schemas change.

### Branch naming

Prefix branches by change type before opening a PR:
- `major/<name>` -- breaking changes
- `minor/<name>` -- new features
- `patch/<name>` -- bug fixes

The `release.yml` workflow auto-calculates the next semver based on the branch prefix when a PR is merged to master.

## Linting

- **golangci-lint v2** with config in `.golangci.yml`:
  - Enabled: `importas` (consistent import aliases), `misspell`
  - Disabled: `errcheck`, `ineffassign`, `staticcheck`
  - Formatters: `gofmt`, `goimports`
- **pre-commit** hooks (`.pre-commit-config.yaml`):
  - Large file checks, merge conflict detection, private key detection
  - End-of-file fixer, trailing whitespace, BOM removal
  - golangci-lint with `--fix`
  - prettier for JSON/markdown/YAML files
- Run locally: `make lint`

## CI/CD

| Workflow | Trigger | What it does |
|---|---|---|
| `internal.yml` | PR to master | Builds `internal/` and runs unit tests |
| `lint-tests.yml` | PR to master | Runs pre-commit on changed files |
| `integration-tests.yml` | PR/push to master | Runs full integration tests in Docker, checks 55% coverage threshold |
| `codeql.yml` | PR/push to master + weekly | CodeQL security analysis for Go |
| `build.yml` | Manual dispatch | Docker build + push to DockerHub and Google Artifact Registry |
| `release.yml` | PR merge to master | Auto-tags version (based on branch prefix), creates GitHub release |

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `github.com/stellar/go-stellar-sdk` | Stellar SDK: XDR types, ingest, ledger backends, history archives, captive core |
| `github.com/stellar/go-stellar-xdr-json` | XDR JSON serialization |
| `github.com/spf13/cobra` | CLI framework |
| `github.com/spf13/viper` | Configuration management |
| `github.com/guregu/null` | Nullable types for BigQuery-compatible JSON output |
| `github.com/xitongsys/parquet-go` | Parquet file writing |
| `github.com/sirupsen/logrus` | Logging (via stellar SDK's log wrapper) |
| `github.com/stretchr/testify` | Test assertions |
| `cloud.google.com/go/storage` | GCS client for datastore access and uploads |
| `github.com/dgryski/go-farm` | FarmHash for ID generation |

## Network Support

Commands support three Stellar networks via flags:
- **Mainnet** (default): Public network
- **Testnet** (`--testnet`): Test network for development
- **Futurenet** (`--futurenet`): Future network for protocol testing

Network configuration (passphrase, archive URLs, core config paths) is resolved in `utils.GetEnvironmentDetails()`.

## Common Flags (all export commands)

| Flag | Description | Default |
|---|---|---|
| `--start-ledger` / `-s` | Start of export range | 2 (genesis) |
| `--end-ledger` / `-e` | End of export range | 0 |
| `--output` / `-o` | Output file path | `exported_{type}.txt` |
| `--strict-export` | Make transform errors fatal | false |
| `--testnet` | Use testnet | false |
| `--futurenet` | Use futurenet | false |
| `--captive-core` | Use captive core (deprecated) | false |
| `--datastore-path` | GCS bucket path for txmeta files | `sdf-ledger-close-meta/v1/ledgers` |
| `--buffer-size` | Max txmeta files in memory | 200 |
| `--num-workers` | Datastore reader worker count | 10 |
| `--write-parquet` | Also write Parquet output | false |
| `--extra-fields` / `-u` | Additional JSON metadata fields | (empty) |
| `--cloud-provider` | Cloud provider for upload (e.g., `gcp`) | (empty) |
| `--cloud-storage-bucket` | Upload destination bucket | `stellar-etl-cli` |
