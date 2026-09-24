<!--
SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
SPDX-License-Identifier: AGPL-3.0-only
-->

# UsageBassoon — Agent Context

## Purpose

A Python CLI and library (pipx-installable, import usagebassoon) that persists `tokscale` JSON token usage statistics to DuckDB, MotherDuck, or BigQuery. This allows users to aggregate token usage across agentic environments.

## Repository Structure
```
usagebassoon/
├── pyproject.toml            # Hatchling packaging; Python >= 3.12
├── src/usagebassoon/
│   ├── __init__.py           # Public API exports
│   ├── __main__.py           # python -m usagebassoon entry point
│   ├── cli/                  # Typer commands and shared CLI helpers; one module per command
│   │   └── reports/          # Terminal report rendering; one module per report type
│   ├── parsers/              # Typed tokscale payload parsers; one module per payload kind
│   ├── contracts/            # Versioned graph, models, pricing, and report JSON contracts
│   ├── backends/             # StorageBackend and DuckDB, MotherDuck, BigQuery adapters
│   ├── buckets/              # SnapshotBucket and local/GCS storage adapters
│   ├── sql/                  # Dialect-specific packaged SQL assets
│   │   ├── duckdb/{ddl.sql, migrations.sql, views.sql}  # Also serves motherduck
│   │   └── bigquery/{ddl.sql, migrations.sql, views.sql}
│   ├── api.py                # Public Python query and connection API
│   ├── archiver.py           # SnapshotArchiver publication, retention, and restore
│   ├── collector.py          # tokscale subprocess acquisition and RawCollection
│   ├── config.py             # Configuration loading, intervals, and backend construction
│   ├── contracts.py          # Contract validation and schema drift detection
│   ├── curation.py           # User-owned tags and notes
│   ├── diagnostics.py        # Doctor checks and read-only diagnostic queries
│   ├── display.py            # Safe terminal rendering of untrusted values
│   ├── drift.py              # Persisted schema drift records and formatting
│   ├── frames.py             # Arrow query results to pandas or Polars DataFrames
│   ├── ingest.py             # Validated plans, collection bundles, and ingest records
│   ├── json_types.py         # Recursive JSON value types
│   ├── logger.py             # Privacy-conscious rotating operational logging
│   ├── normalizer.py         # CollectionBundle to canonical Arrow tables
│   ├── orchestrator.py       # Top-level collection and data shuttling
│   ├── persistence.py        # Transactional batch persistence and retries
│   ├── privacy.py            # Output-time obfuscation for shared artifacts
│   ├── reconcile.py          # Collection reconciliation and issue record types
│   ├── scheduling.py         # Native schedulers and collection worker loop
│   ├── schema_assets.py      # Ordered packaged SQL for schema initialization
│   ├── sql_safety.py         # Public relation query validation and generation
│   ├── system_metadata.py    # Best-effort collector-host metadata
│   └── version.py            # Installed distribution version lookup
├── tests/                    # Unit, integration, CLI, backend, bucket, and SQL parity coverage
│   └── fixtures/             # Sanitized tokscale payload captures; treat as immutable
├── .github/workflows/        # CI, dialect parity, and release workflows
└── justfile                  # Development, test, and maintenance tasks
```

Golden fixture filenames use `golden-<capture-date>-tokscale-<exact-version>.<payload-kind>.json`, for example `golden-2026-09-10-tokscale-4.15.1.graph.json`. The tokscale version is the exact referenced version, never `latest`; prerelease versions remain unchanged, such as `tokscale-4.16.0-rc.1`.

## Architecture

- **Dependencies**: `duckdb`, `pandas`, `pydantic`, `pyarrow`, `typer`, `rich`, `plotext`, `polars` (optional).
- **Dev Environment**: `uv`, `hatchling`, `twine`, `pyrefly`, `ruff`, `pytest`, `just`, `pre-commit`.

UsageBassoon separates collection, ingest, normalization, warehouse persistence, and snapshot archival. Follow this path when changing collection behavior:

```text
scheduling.py / CLI
        → orchestrator.py
        ↔ collector.py          tokscale subprocess calls and raw acquisition outcomes
        ↔ ingest.py             validated GraphPlan / ModelsPlan for subsequent requests
        → RawCollection
        → ingest.py             contracts.py validation → parsers/ → CollectionBundle
        → normalizer.py         canonical Arrow tables in NormalizedBundle
        → persistence.py        batch construction, backend transactions, and retries
        → backends/base.py      StorageBackend → DuckDB / MotherDuck / BigQuery
        → sql/                  dialect-specific DDL and views
        → CLI / Python API
```

`orchestrator.py` owns the collection sequence and passes results between stages. `collector.py` owns `tokscale` command resolution, subprocess execution, and raw payload acquisition; `RawCollection` contains source payloads only. The collector does not validate contracts, parse payloads, or persist data.

`ingest.py` owns validation and parsing coordination. It produces `GraphPlan` and `ModelsPlan` to guide further collection, constructs `IngestEvidence` from acquisition outcomes and prior status, and builds `CollectionBundle`. `contracts.py` detects contract violations and schema drift; `parsers/` converts validated payloads into typed data. Ingest decides which report and pricing failures can be tolerated while preserving valid token facts. Graph and models remain required.

`normalizer.py` owns `NormalizedBundle` and converts `CollectionBundle` into canonical Arrow tables, including derived columns. It does not execute backend transactions. `persistence.py` reads prior ingest status, assembles and persists batches, and handles bounded transaction retries. `backends/base.py` defines `StorageBackend` and `AbstractStorageBackend`; backend implementations execute warehouse-specific storage operations. SQL DDL and views remain dialect-specific under `sql/`.

Snapshot archival is a separate path after persistence:

```text
orchestrator.py → archiver.py         SnapshotArchiver
                → buckets/base.py     SnapshotBucket → local / GCS
```

`SnapshotArchiver` owns capture, Parquet format, catalog publication, retention, and restore semantics. `SnapshotBucket` implementations own version-aware object storage and compare-and-swap operations; they do not define snapshot policy. Snapshot storage providers belong in `buckets/`, not `backends/`.

Diagnostic inspection is a separate read-only path:

```text
cli/doctor.py → config.py           load configuration and open a StorageBackend
              → diagnostics.py      inspect connectivity, schema, transactions, and persisted issues
              → backends/base.py    StorageBackend → DuckDB / MotherDuck / BigQuery
              → diagnostics.py      assemble DoctorReport
              → cli/doctor.py       render checks and exit status
```

`diagnostics.py` owns doctor-specific backend queries for ingest runs, reconciliation issues, and unresolved schema drift, along with health-check coordination and `DoctorReport`. The query results use `IngestIssue` from `ingest.py`, `ReconciliationIssueRecord` from `reconcile.py`, and `SchemaDriftRecord` from `drift.py`. `ingest.py` and `reconcile.py` operate on data passed through the collection pipeline; `persistence.py` handles collection state reads and batch persistence. Diagnostic queries use the backend opened by `cli/doctor.py`.

## Commands

Run all project tasks via `just` from the repository root. Use `just --list` to inspect available recipes.

- USE `just install-dev` to synchronize all uv dependency groups.
- USE `just test` to run the test suite; pass pytest arguments with `just test <args>`. The recipe sets `USAGEBASSOON_LOG_DIRECTORY` to a temporary `/tmp` directory and removes it afterward, so tests work in restricted environments.
- USE `just test-bq-live <test-name> <reset>` to run the `bigquery_live` test; `<test-name>` can be the name of the python module `tests/test_backend_bigquery_live.py` or a specific test within (e.g. `tests/test_backend_bigquery_live.py::test_live_batch_matches_duckdb_and_retries_idempotently`); set `<reset>` (defaults to `0`) to `1` to set the `USAGEBASSOON_BIGQUERY_LIVE_RESET` environment variable; `USAGEBASSOON_BIGQUERY_LIVE=1` is always set for this command; if a test run exceeds your execution timeout, inspect `.test_logs/pytest-bq-live.log` to view partial progress or the final failure trace including a UTC timestamp at the start of the file of when the test began for your reference. Do NOT attempt to use `just test` or `uv run pytest` when testing `bigquery_live` which can cause execution timeouts to your environment that are impossible to diagnose.
- USE `just test-gcs-live <test-name>` to run the `gcs_live` test; `<test-name`> can be the name of the python module `tests/test_bucket_gcs_live.py` or a specific test within; `USAGEBASSOON_GCS_LIVE=1` is always set for this command; if a test run exceeds your execution timeout, inspect `.test_logs/pytest-gcs-live.log` to view partial progress or the final failure trace including a UTC timestamp at the start of the file of when the test began for your reference. Do NOT attempt to use `just test` or `uv run pytest` when testing `gcs_live`.
- USE `just coverage` to run test suite with terminal coverage reporting.
- USE `just lint` to perform Ruff linting and formatting checks for Python, GitHub action workflows, and YAML files.
- USE `just lint-fix` to apply Ruff lint and formatting corrections.
- USE `just typecheck` to perform Pyrefly type checks.
- USE `just spell-diff` to run typos spelling checker.
- PREFER `just ci` for combined test, lint, and typecheck recipes.
- USE `just build` to clean `dist/` and build an sdist and wheel with Hatchling.
- USE `just check-dist` to clean, build, and validate artifacts with Twine.
- USE `just clean` to remove build artifacts and local caches.

## Rules

- ALL Python code should target Python 3.12+ syntax only.
- ALL generated/edited Python source code MUST pass Ruff and Pyrefly checks through `just lint` and `just typecheck`, respecitively. You MUST run Ruff and Pyrefly as a matter of routine after generating or editing any Python source code.
- Do NOT suppress diagnostics to make checks pass. Do NOT introduce implicit `Any` or use bare generic types.
- PREFER PEP 695 syntax for **all** new generic declarations and type aliases. USE modern built-in generic and union syntax.
- USE Google-style docstrings for **all** source code.
- KEEP comments concise yet clear. Do NOT use numbered headers (e.g. "1." or "(1)" etc).
- FOR module-level docstrings, ADD the name of the module to the start of the docstring (e.g. """my_module.py — ...).
- NO version string is ever hard-coded in source; `hatch-vcs` manages version numbering from git tags (`v0.1.0` → `0.1.0`).
- Do NOT wrap lines when generating markdown text.
- USE Keep a Changelog standards in `CHANGELOG.md`; preserve release heading and bullet formatting because `just release` and the GitHub release workflow extract release notes from it.
- ALWAYS use the `usagebassoon_it` dataset when live testing with BigQuery. NEVER perform tests on any other dataset. **NEVER** perform tests on a dataset called only `usagebassoon`.
- ALWAYS use the `gs://usagebassoon-test-snapshots-gen-lang-client-0670612427` Google Cloud Storage bucket for GCS testing. NEVER perform tests on any other GCS bucket.
- ALL CLI commands MUST use a dialect-specific SQL view; NEVER hardcode SQL queries that are not dialect agnostic.
- FOR CLI output assertions in *tests*, normalize captured stdout or stderr with `tests._cli.plain_cli_output(...)` before comparing text. GitHub Actions color output can insert ANSI escapes inside visible tokens such as `--version`, causing raw substring assertions to fail. Reproduce this environment with `CI=true GITHUB_ACTIONS=true TERM=xterm-256color just test <test>` when diagnosing this failure.

### Prohibitions
The following actions are **prohibited** and are reserved exclusively for the user. When encountering a task that involves a prohibited action, you MUST **stop** and **report** to the user the conflict:

- NEVER modify the golden JSON fixtures in `tests/fixtures/`.
- NEVER attempt to publish to PyPI or invoke any publishing command; consequently, NEVER use `just publish` or `just release`.
- NEVER attempt to perform a release to GitHub or invoke any release command.
- NEVER attempt to push git changes to GitHub.

## Design Brief

The main **goal** of this project is to *never* lose token-usage history. Users should be able to work from any emphemeral environment (e.g containers, rotating VMs) and be able to persist the token suage statistics from their agents with a single credential (e.g. `MOTHERDUCK_TOKEN`) and zero local secrets. Consequent design goals include:

- Persist the following statistics:
  - costs
  - input/output/cache-read/cache-write/reasoning tokens
  - data granularity: per-session, per-model, per-day
  - point-in-time pricing
  - session timestamps
  - project attribution
  - system details: OS, CPU, RAM, shell environment
- Idempotent design, merge-based ingestion — safe to run on a cron from N machines
- Query anything with SQL; export to `pandas` DataFrame in two lines; `polars` optionally supported
- User-curated organization: source-scoped workspace/client/session tags and session notes
- Terminal-first text-based reporting via `rich` and rendering of charts via `plotext`
- Pluggable storage: MotherDuck (hosted) or a local DuckDB file (zero deps)

The following are out-of-scope and/or antithetical to the design goals:

- TUI, web server, or HTML report generator
- Parsing of agent session files — in v1 we will use `tokscale` to extract token data
- Persistence of tokscale's generated summary fields which have non-deterministic provenance, including:
  - `title`,
  - `task_category`
  - `description`
  - `task_group`

### Important Design Decisions

- **tokscale derived metrics:** We invoke `tokscale` as a subprocess and treat its stdout as the only source of truth for token usage statistics. A future major version may make token statistics collection native.

- **Daily tokscale pricing:** `price_versions` stores the tokscale rates observed for each model with activity on a processed day. `daily_cost` calculates `cost_usd` from those rates and daily token components; `tokscale_cost_usd` remains diagnostic. Historical price drift before collection is the downstream user's responsibility.

- **No at-rest obfuscation of stored data:** Session data is stored raw at-rest and obfuscated at export-time. The CLI command `bassoon export` will *default* to obfuscating potentially personal information including session IDs, workspace names and paths, project names, and anything similar. For example, a project name may be pseudonymized as "project-alpha". Downstream users may optionally export their raw data as JSON via the flag `--raw-json`.

- **Library + CLI:** Everything the CLI does is importable Python.

- **Source identity:** `source_id` is a UUID in configuration, generated by `usagebassoon init`. It namespaces every collected fact and curation target. Reuse it only for environments intentionally representing one source.

- **Same-source concurrency:** Overlapping collections with the same `source_id` can race. This is a known issue; a comprehensive mutex table is planned but deliberately deferred. Measures are put in place to prevent same-source-same-environment overlapping calls.

- **Concurrent collection and snapshot safety:** Independent environments can collect into one backend and compete for snapshot publication without sharing a process lock.
  - `source_id` namespaces usage facts from different sources. Environments with distinct source IDs can persist to the same backend without overwriting one another’s facts. `source_id` is an identity, not a lock.
  - Remote data warehouse persistence batches run in a transaction and detects its concurrent-update abort with bounded jittered batch retries. The `run_id` ledger makes retrying that *same* batch idempotent, and collection-start timestamps prevent older observations from replacing newer current-state rows. BigQuery uses run-scoped staging tables and a transaction MERGE script; DuckDB and MotherDuck use transactional batches.
  - All snapshot destinations require compare-and-swap catalog reservations with owner, expiry, and fence checks. If multiple UsageBassoon instances from multiple sources attempt to perform a snapshot, a writer claims every configured destination before capture. Only the holder of the current reservation and fence can publish. A contender instance that loses the claim does not publish a snapshot. Reservations renew during capture and uploads. Remote catalog updates and object deletion use generation preconditions. Local catalog compare-and-swap holds an OS file lock through version check and atomic replacement.
  - Snapshots are atomic. A snapshot enters the catalog only after every table and its manifest are complete. Failed publication releases owned reservations and cleans up its objects.
  - Usage facts are persisted with domain-dependent tolerance. `tokscale graph` and `tokscale models` calls establish the collection plan. Pricing failures are tolerated per model; report failures are tolerated per day. Successful results still enter the bundle, and `ingest_status` records complete, partial, or failed outcomes for retry. Historical completed models and pricing work can be skipped on later runs. Required command failures abort before persistence.

- **Session and curation identity:** A session key is `(source_id, client, session_id)`; workspace remains metadata. Client and workspace are peer scopes, not a hierarchy. Effective session tags combine direct session tags with tags on its source-scoped client and workspace.

- **Daily facts:** `tokscale graph` supplies candidate dates and `daily_activity`. For each candidate day, date-filtered `tokscale models` supplies `daily_stats` at `(source_id, day, client, session_id, model)`. Completed historical targets skip by default; the current day refreshes.

- **Token calculation invariants:**
  - "reasoning" tokens are a component of the total token count such that total_tokens = input + cache_read + cache_write + reasoning + output tokens (fixture-verified against tokscale 4.15.1).
  - "reasoning" tokens are considered output tokens for pricing purposes (fixture-verified against tokscale 4.15.1).

- **Data ingest pipeline:** `bassoon collect`:
  1. Resolve tokscale (`TOKSCALE_BIN`, else `tokscale` on PATH, else `bunx tokscale@latest`). Record version from graph payload meta.
  2. Run `graph`, use its contribution dates to select daily models work, run date-filtered `models` per required day, then fetch prices for each model used on those days and `report --no-summarize`.
  3. Validate against the schema contract with pydantic strict mode. Required-field absence **fails the run** with a clear error; *unknown* fields or changed cardinalities are **drift events** — upserted in `schema_drift_events` by source, command domain, Tokscale version, and drift key, surfaced in output, and surfaced on the next `bassoon doctor` until a complete clean validation resolves them. Repeated payload sightings increment `observation_count` while preserving first-detection metadata.
  4. Graph totals are not reconciled with daily models totals; graph is only the activity and candidate-date source.
  5. Normalize to Arrow tables; compute derived columns. Stage each fact table in one batch.
  6. Stage the Arrow batch, match rows by natural key, update changed existing rows, insert new rows, and leave absent rows untouched. Use one transactional upsert/MERGE per collection run; **never** delete.
  7. Optionally: if `snapshots.interval` has elapsed, run `bassoon snapshot`.

- **Database management:** Locally, data will be managed and stored by DuckDB. Remotely, data will be managed and stored by either MotherDuck or GCP BigQuery. DDL and SQL views will be written natively to their respective dialect (e.g. `sql/duckdb/{ddl,views}.sql` and `sql/bigquery/{ddl,views}.sql`). `SQLGlot` will be used during CI to prevent structural drift. Dedicated pytests will be used during CI to prevent semantic drift against the golden fixtures.

- **Database CI: `.github/workflows/dialect-parity.yml`** — on every PR that touches `sql/` or `tests/fixtures/`:
  1. `SQLGlot` parses both dialects' DDL/views.
  2. Transpiles `bigquery/*` → duckdb dialect, asserts AST-equivalence against the duckdb tree (and vice-versa for the view sets).
  3. A **replay test** runs the same golden-fixture dataset through both dialects in DuckDB (translated BigQuery SQL) and asserts identical result sets. Transpiler parity is *structural*, not semantic.

- **Ingest semantics:** Date-filtered `models` rows are upserted at daily session/model grain. `graph` contributions are authoritative only for `daily_activity` and candidate dates. `session_model_stats` is an all-time calculated view over `daily_stats`. Tags and notes are owned by the user and are never touched by merge.

- **Snapshot semantics:** Snapshots are portable normalized-table archives, not raw-payload replay points. Every backend—including BigQuery—reads canonical Arrow tables and writes deterministic Parquet through UsageBassoon; never use a server-side BigQuery-to-GCS export.
  - A snapshot is restorable only after every expected table succeeds, its complete manifest is written, and the manifest is published in the archive catalog. Uncataloged prefixes are staging/orphans, never restore candidates.
  - The catalog defines `latest`, cadence, and FIFO retention. GCS catalog publication uses generation compare-and-swap plus a short fenced reservation; cleanup is generation-conditional and must never delete the latest published snapshot. Local archives use the same catalog semantics.
  - Restore validates catalog membership, complete table coverage, and destination schema compatibility before appending any data. The destination must be initialized and empty; a failed restore can leave partial data and must be retried from a fresh/emptied destination.
  - `interval` is an optional positive minimum publication cadence. It gates both manual and automatic snapshots; only a configured interval enables collection-triggered snapshots. The retention default is 3.

- **Schema contracts:** Each tokscale payload kind has a versioned contract — the expected field names, types, and cardinalities, pinned against a tokscale version. The contract lives in `src/usagebassoon/contracts/{models,graph,pricing,report}.json`, generated from golden fixtures and asserted in tests. Deviation produces `schema_drift_events` rows and a user-facing warning and asks for a bug report:

  - Contract requiredness describes what UsageBassoon needs from a payload to execute ingestion, not every field present in a golden fixture. Mark unused graph aggregates and capture metadata optional when their absence does not affect ingestion; their omission should not produce missing-field drift or fail collection. Optional fields that are present with an unexpected type still produce non-fatal type-change drift; omit a field from the contract entirely only when its type drift should not be monitored.

```
$ bassoon collect
⚠ schema drift detected (tokscale 4.15.2 vs contract 4.15.1):
    models: unknown field 'entries[].performance.gpu_util'
    pricing: field 'resolution.priceConsensus' changed type (number → object)
  → collection <status>.
    Run `bassoon doctor` for detail. Open an issue: https://github.com/israelflores8789/usagebassoon/issues/new
```

- **Data-engine agnostic abstraction:** `DatabaseBackend` in `backends/base.py` is deprecated and will be replaced with `StorageBackend`, a higher-order abstraction using Arrow. Implementations:
  - `duckdb_local.py` — local file; `register(arrow_table)` is zero-copy
  - `motherduck.py` — identical code path, `md:` connection string
  - `bigquery.py` — `google-cloud-bigquery`; Arrow staging then one transaction script per collection run. Concurrent-update aborts retry with jitter and active transactions are exposed to `bassoon doctor`.

  Derived columns (`total_tokens`, `session_label`) are computed **in the normalizer**; calculated costs remain dialect-paired views. The pipeline is:
  ```
  tokscale JSON → pydantic contract validation (strict, drift events)
              → pydantic models (typed objects)
              → Arrow normalizer (batch, derived columns, canonical schema)
              → StorageBackend (dialect-specific executor)
              → pandas/polars/Arrow on the way back via `to_arrow()`
  ```

- **Notable SQL semantics:**
  - `run_id` is generated and UUID-validated by Arrow, then stored as canonical text in SQL backends for dialect portability.
  - `last_seen_at` is metadata from tokscale's `last_active` or similar.
  - `last_collected_at` is a freshness marker internal to usagebassoon.

- **General storage model:** Usage facts, day/model price versions, schema-drift events, and reconciliation issues use current-state upserts. Reconciliation issues are keyed by source, check, and issue, track cumulative `observation_count`, and use a boolean `resolved` state. Existing natural keys are updated in place; new natural keys are inserted; rows absent from later snapshots are **never** deleted. Ingest runs and snapshot artifacts are append-only.

### Canonical Ingest Commands

These are the `tokscale` commands used to generate ingest data. Each command is authoritative for its data domain. Graph supplies activity and candidate dates only; it is not reconciled with daily models totals:

- `tokscale models --json --group-by client,session,model --since <YYYY-MM-DD> --until <YYYY-MM-DD>` — authoritative for daily statistics with session-level granularity.
- `tokscale report --json --no-summarize --since <YYYY-MM-DD> --until <YYYY-MM-DD>` — authoritative for session metadata.
- `tokscale graph` — authoritative for daily activity statistics.
- `tokscale pricing <model-id> --json` — authoritative for the rate observed while processing a daily usage fact.

### Implemented SQL views (per-dialect: `sql/{duckdb,bigquery}/views.sql`)

The DuckDB/MotherDuck and BigQuery assets implement the same 11 views:

- `daily_cost` applies the observed source/day/model rates to daily token facts. It returns `NULL` cost when a nonzero token category has no matching rate; reasoning uses the output rate.
- `session_model_stats` aggregates daily facts across time at source/client/session/model grain. Its cost is `NULL` unless every contributing daily fact has a known cost. `session_model_stats_current` is an alias with identical rows and no current-time filter.
- `report_daily_usage` exposes daily facts with workspace metadata. `report_session_models` exposes session/model totals with workspace and last-active metadata. These are the filterable report inputs; CLI reporting applies source, client, model, workspace, and effective-tag filters before aggregating.
- `report_summary` and `report_models` provide global session and model aggregates. They omit source/client/workspace dimensions, and their `SUM(cost_usd)` ignores `NULL` inputs, so totals may be partial when pricing is incomplete.
- `session_tags` resolves source-scoped client, workspace, and session tags while preserving tag scope. `tagged_sessions` joins those effective tags to session metadata.
- `noted_sessions` joins notes to sessions, while `session_notes` exposes the stable notes projection used by curation commands.

The report source views retain the dimensions needed for filtering; the pre-aggregated summary views do not. Consumers that need to distinguish incomplete pricing should use the detailed views or explicitly check cost completeness before aggregating.

### CLI

The installed command is `bassoon`. The commands below are implemented.

| Command | Purpose |
|---|---|
| `bassoon init` | create config + configured DDL + views |
| `bassoon collect` | one delta-ingest cycle (designed for cron) |
| `bassoon query <relation>` | bounded query of a supported relation; raw output with sharing warning |
| `bassoon report summary/sessions/daily/graph` | terminal reports; `--sanitize/--obfuscate` for sharing |
| `bassoon tag add/rename/remove` | source-aware user curation |
| `bassoon note set/edit/remove` | source-aware user curation |
| `bassoon restore` | restore a snapshot into an initialized, empty warehouse |
| `bassoon snapshot` | write a private snapshot to configured destinations |
| `bassoon export <relation> <path>` | export a supported table/view to parquet/csv/json; obfuscated by default; `--raw` for raw data |
| `bassoon audit` | collection audit log from `ingest_runs` |
| `bassoon doctor` | credentials, connectivity, reconciliation, unresolved schema_drift, with issue link |
| `bassoon schedule install/status/start/stop/logs/remove/worker` | install or manage native scheduling, or run the container worker |

### Privacy and sharing policy

- UsageBassoon stores raw operational data so collection, merge, curation, restore, and personal reports retain full fidelity.
- `bassoon report` subcommands are raw by default because they are user-facing terminal experiences. Interactive output reminds users to run the selected subcommand with `--sanitize` before sharing; saved reports omit that reminder.
- `bassoon doctor` is the shareable diagnostic path and sanitizes configuration paths, database locations, and connection credentials by default. `bassoon doctor --raw` always warns that raw output must not be pasted into public GitHub issues.
- `bassoon query` returns raw results from an allowlisted relation and always prints an unsuppressible sharing warning to stderr; it directs issue reporters to `bassoon doctor`.
- `bassoon export` obfuscates `session_id`, workspace fields, host names, free-form tags, and session-bearing reconciliation keys by default; notes are redacted. It prints a stderr reminder that `--raw` is available for intentional personal backup or data-management output.
- Schema-drift identifiers, paths, detail, versions, exact timestamps, and reconciliation messages remain unchanged in sanitized doctor/export output because they are generated structural diagnostics required for actionable bug reports. `client` values (for example `codex` and `opencode`) remain unchanged.
- Snapshots are raw restoration artifacts, not shareable exports. Treat snapshot storage as private.

### Proposed Python API

`pandas` is the *default*; `polars` is first-class:

```python
import usagebassoon

df  = usagebassoon.query("SELECT * FROM report_models")                   # pandas
df  = usagebassoon.query("SELECT * FROM daily_cost", engine="polars")     # polars
tbl = usagebassoon.query_arrow("SELECT * FROM sessions")                  # raw Arrow
con = usagebassoon.connect()          # duckdb conn, or ibis-style BigQuery session
```

### Default Configuration

```toml
# Platform-specific config path; override with USAGEBASSOON_CONFIG.
source_id = "018f2d70-0000-4000-8000-000000000000" # UUID source namespace; generated by `bassoon init`
backend = "duckdb" # duckdb | motherduck | bigquery
# local_database = "/path/to/usagebassoon.duckdb" # optional DuckDB path; defaults to the platform data directory

[tokscale]
# bin omitted: TOKSCALE_BIN, then tokscale on PATH, then bunx tokscale@latest.
env = []
timeout = "180s"
max_stdout_bytes = 67108864
max_stderr_bytes = 8388608

# [bigquery] # only when backend = "bigquery"
# project = "my-project" # required; no default
# dataset = "my-dataset" # required; no default
# location = "US"
# credentials: GOOGLE_APPLICATION_CREDENTIALS, or `gcloud auth application-default login`
# credentials_file = "/path/to/service-account.json" # optional; default unset
# maximum_bytes_billed = 1073741824
# timeout = "120s"

# [motherduck] # only when backend = "motherduck"
# database = "my-database" # required; no default

# [gcs] # optional GCS snapshot destination
# uri = "gs://my-bucket/usagebassoon/snapshots"
# project = "my-project" # required when GCS is configured
# credentials_file = "/path/to/service-account.json" # optional; default unset
# timeout = "60s"

[schedule]
interval = "15m" # default interval; scheduling is not installed by default

[collection]
max_retries = 3
retry_initial_seconds = 1.0

[logging]
directory = "~/.local/state/usagebassoon/logs"
max_files = 5
max_bytes = 5242880

# [snapshots] # optional; absent = feature off
# file_uri = "/path/to/local/snapshots" # optional local archive; default unset
# max_snapshots = 3 # rotating retention
# interval = "12h" # taken during collect when elapsed; unset by default
```

Default directories leverage XDG, macOS XDG equivalents, and Windows support through `platformdirs` by the following:

- for the config file, the defaults will go to:
  - Linux: `~/.config/usagebassoon/config.toml`
  - macOS: `~/Library/Application Support/UsageBassoon/config.toml`
  - Windows: `%LOCALAPPDATA%\UsageBassoon\config.toml`

- for the local database, the defaults will go to:
  - Linux: `~/.local/share/usagebassoon/usagebassoon.duckdb`
  - macOS: `~/Library/Application Support/UsageBassoon/usagebassoon.duckdb`
  - Windows: `%LOCALAPPDATA%\UsageBassoon\usagebassoon.duckdb`

- for the logs, the defaults will go to:
  - Linux: `~/.local/state/usagebassoon/logs/`
  - macOS: `~/Library/Logs/UsageBassoon/`
  - Windows: `%LOCALAPPDATA%\UsageBassoon\Logs\`

- for the snapshots, the defaults will go to:
  - Linux: `~/.local/share/usagebassoon/snapshots/`
  - macOS: `~/Library/Application Support/UsageBassoon/snapshots/`
  - Windows: `%LOCALAPPDATA%\UsageBassoon\snapshots\`
