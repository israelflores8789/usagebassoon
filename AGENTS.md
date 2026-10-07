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
│   ├── backends/             # StorageBackend contracts, factory, and database adapters
│   ├── buckets/              # SnapshotBucket abstractions, factory, and object adapters
│   ├── sql/                  # Dialect-specific packaged SQL assets
│   │   ├── duckdb/           # ddl.sql, views.sql (also serves MotherDuck)
│   │   └── bigquery/         # ddl.sql, views.sql, compaction.sql
│   ├── api.py                # Public Python query and connection API
│   ├── archiver.py           # Public snapshot orchestration
│   ├── audit.py              # Backend and archived run/source evidence
│   ├── snapshot/             # Portable format, catalog lifecycle, reading, restore
│   │   ├── format.py         # Documents, readers, and forward transformations
│   │   ├── catalog.py        # Shared policy, reservations, pins, retirement, repair
│   │   ├── reader.py         # Selection and verified disk-backed Arrow access
│   │   └── restore.py        # Provider-neutral recovery coordination
│   ├── collector.py          # tokscale subprocess acquisition and RawCollection
│   ├── config.py             # Configuration parsing, validation, intervals, and writing
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
│   ├── persistence.py        # Batch publication and bounded retries
│   ├── privacy.py            # Output-time obfuscation for shared artifacts
│   ├── reconcile.py          # Collection reconciliation and issue record types
│   ├── scheduling.py         # Native schedulers and collection worker loop
│   ├── schema_assets.py      # Ordered packaged SQL for schema initialization
│   ├── collection_lock.py    # Local user-environment collection exclusion
│   ├── storage_model.py      # Shared Arrow schemas, logical keys, and tie-break rules
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

- **Dependencies**: `duckdb`, `pandas`, `pydantic`, `pyarrow`, `typer`, `rich`, `plotext`, `platformdirs`, `sqlglot`.
  - Optional extras for Polars, BigQuery, and GCS.
- **Dev Environment**: `uv`, `hatchling`, `twine`, `pyrefly`, `ruff`, `pytest`, `just`, `pre-commit`.

**Terminology:** Use **storage backend** (or **backend**) for a `StorageBackend` implementation and its configured database destination. Use database, dataset, or warehouse only when referring to a platform-specific resource. A **snapshot bucket** (or **bucket**) is a `SnapshotBucket` storage adapter; a **snapshot destination** is its configured archive root. Snapshot buckets are independent of storage backends. **Direct transactional upsert** and **Append-and-compact** name persistence architectures, not individual platforms.

UsageBassoon separates collection, ingest, normalization, backend persistence, and snapshot archival. Follow this path when changing collection behavior:

```text
scheduling.py / CLI
        → orchestrator.py
        → collection_lock.py    exclude simultaneous local DuckDB collectors
        ↔ collector.py          tokscale subprocess calls and raw acquisition outcomes
        ↔ ingest.py             validated GraphPlan / ModelsPlan for subsequent requests
        → RawCollection
        → ingest.py             contracts.py validation → parsers/ → CollectionBundle
        → normalizer.py         canonical Arrow tables in NormalizedBundle
        → persistence.py        batch construction, backend-specific publication, and retries
        → backends/base.py      StorageBackend → DuckDB / MotherDuck / BigQuery
        → sql/                  installed dialect-specific views; no collection DDL
        → collection_lock.py    release the local collection lock
        → CLI / Python API
```

`orchestrator.py` owns the collection sequence and passes results between stages. `collector.py` owns `tokscale` command resolution, subprocess execution, and raw payload acquisition; `RawCollection` contains source payloads only. The collector does not validate contracts, parse payloads, or persist data.

`ingest.py` owns validation and parsing coordination. It produces `GraphPlan` and `ModelsPlan` to guide further collection, constructs `IngestEvidence` from acquisition outcomes and prior status, and builds `CollectionBundle`. `contracts.py` detects contract violations and schema drift; `parsers/` converts validated payloads into typed data. Ingest decides which report and pricing failures can be tolerated while preserving valid token facts. Graph and models remain required.

`normalizer.py` owns `NormalizedBundle` and converts `CollectionBundle` into canonical Arrow tables, including derived columns. It does not execute backend transactions. `persistence.py` reads collection planning state through one preflight view, assembles batches, and handles bounded retries. `collection_lock.py` excludes simultaneous local DuckDB collectors; remote collectors publish independently. `backends/base.py` defines `StorageBackend` and `AbstractStorageBackend`; implementations execute native publication and restore operations. SQL DDL and views remain dialect-specific under `sql/`.

Snapshot archival is scheduled independently of collection:

```text
CLI / independent scheduling       → archiver.py       SnapshotArchiver
                                   → snapshot/format.py / catalog.py / reader.py / restore.py
                                   → backends/base.py  StorageBackend → DuckDB / MotherDuck / BigQuery
                                   → buckets/base.py   SnapshotBucket → local / GCS

audit.py → canonical backend views or snapshot/reader.py
normalizer.py / backends / snapshot → storage_model.py
```

`SnapshotArchiver` is the public orchestrator; the `snapshot/` subpackage owns format contracts, catalog lifecycle, reading, and recovery coordination. It never imports `archiver.py`. `storage_model.py` remains core because collection, curation, compaction, and snapshots share its Arrow schemas, identities, and ordering. Backends stream canonical Arrow batches at one consistent read point: a DuckDB/MotherDuck transaction or BigQuery time travel at a captured timestamp. The archiver owns incremental Parquet serialization to private temporary files. `SnapshotBucket` implementations own bounded file transfer, version-aware object storage, and compare-and-swap operations; they do not define snapshot policy. Snapshot storage providers belong in `buckets/`, not `backends/`. Current implementations are local files and GCS; future object-store providers must satisfy the same `SnapshotBucket` contract.

Diagnostic inspection is a separate read-only path:

```text
cli/doctor.py → config.py           parse and validate configuration
              → backends/factory.py construct and preflight a StorageBackend
              → diagnostics.py      inspect connectivity, schema, transactions, and persisted issues
              → backends/base.py    StorageBackend → DuckDB / MotherDuck / BigQuery
              → diagnostics.py      assemble DoctorReport
              → cli/doctor.py       render checks and exit status
```

`diagnostics.py` owns doctor-specific backend queries for collection runs, reconciliation issues, and unresolved schema drift, along with health-check coordination and `DoctorReport`. The query results use `IngestIssue` from `ingest.py`, `ReconciliationIssueRecord` from `reconcile.py`, and `SchemaDriftRecord` from `drift.py`. `ingest.py` and `reconcile.py` operate on data passed through the collection pipeline; `persistence.py` handles collection state reads and batch persistence. Diagnostic queries use the backend opened by `cli/doctor.py`.

## Commands

Run all project tasks via `just` from the repository root. Use `just --list` to inspect available recipes.

- USE `just install-dev` to synchronize all uv dependency groups.
- USE `just test` to run the test suite; pass pytest arguments with `just test <args>`. The recipe sets `USAGEBASSOON_LOG_DIRECTORY` to a temporary `/tmp` directory and removes it afterward, so tests work in restricted environments.
- USE `just test-bq-live <test-name> <reset>` for `bigquery_live`; the default module is `tests/test_backend_bigquery_live.py`, and individual test selectors are accepted. The recipe sets `USAGEBASSOON_BIGQUERY_LIVE=1`; `<reset>` defaults to `0`, and `1` enables reset. Inspect `.test_logs/pytest-bq-live.log` after timeouts; it includes a UTC start timestamp and partial output. Do NOT use `just test` or `uv run pytest` for these live tests.
- USE `just test-md-live <test-name>` to run the `motherduck_live` test; this recipe always resets the dedicated integration database. Logs are output to `.test_logs/pytest-md-live.log`.
- USE `just test-gcs-live <test-name>` for `gcs_live`; the default module is `tests/test_bucket_gcs_live.py`, and individual test selectors are accepted. The recipe sets `USAGEBASSOON_GCS_LIVE=1`. Inspect `.test_logs/pytest-gcs-live.log` after timeouts; it includes a UTC start timestamp and partial output. Do NOT use `just test` or `uv run pytest` for these live tests.
- PRESERVE live-test module docstrings that document the raw pytest commands and implemented environment variables for developer clarity; agent requirements to use dedicated `just` recipes govern agent execution, not developer documentation or CI invocations.
- USE `just coverage` to run test suite with terminal coverage reporting.
- USE `just lint` for Python Ruff linting and formatting checks; USE `just lint-yaml` for GitHub Actions and YAML checks.
- USE `just lint-fix` to apply Ruff lint and formatting corrections.
- USE `just typecheck` to perform Pyrefly type checks.
- USE `just spell-diff` to run typos spelling checker.
- PREFER `just ci` for unit/local tests, Python lint, and type checks; run SQL parity, YAML checks, and live tests separately when relevant.
- USE `just build` to clean `dist/` and build an sdist and wheel with Hatchling.
- USE `just check-dist` to clean, build, and validate artifacts with Twine.
- USE `just clean` to remove build artifacts and local caches.

## Rules

- ALL Python code should target Python 3.12+ syntax only.
- ALWAYS use existing modules unless the task requires a new storage backend, snapshot bucket, CLI command, or core feature; this also includes test modules.
- ALL generated/edited Python source MUST pass Ruff and Pyrefly; RUN `just lint` and `just typecheck` after editing *any* Python code.
- Do NOT suppress diagnostics to make checks pass. Do NOT introduce implicit `Any` or use bare generic types.
- PREFER PEP 695 syntax for **all** new generic declarations and type aliases. USE modern built-in generic and union syntax.
- USE Google-style docstrings for **all** source code.
- KEEP comments concise yet clear. Do NOT use numbered headers (e.g. "1." or "(1)" etc).
- FOR module-level docstrings, ADD the name of the module to the start of the docstring (e.g. """my_module.py — ...).
- NEVER hard-code the application version in source; `hatch-vcs` derives it from git tags (`v0.1.0` → `0.1.0`). Release tags and versions use Semantic Versioning standards *with* the "v" prefix. Schema versions and pinned external contract versions are separate.
- Do NOT wrap lines when generating markdown text.
- USE Keep a Changelog standards in `CHANGELOG.md`; preserve release heading and bullet formatting because `just release` and the GitHub release workflow extract release notes from it.
- ALWAYS use the `usagebassoon_it` dataset when live testing with BigQuery. NEVER perform tests on any other dataset. **NEVER** perform tests on a dataset called only `usagebassoon`.
- ALWAYS use the `usagebassoon_it` database when live testing with MotherDuck. NEVER perform tests on any other database. **NEVER** perform tests on a database called only `usagebassoon`.
- ALWAYS use the `gs://usagebassoon-test-snapshots-<gcp-project-id>` Google Cloud Storage bucket for GCS testing. **NEVER** perform tests on any other GCS bucket.
- CLI read queries MUST use the shared views installed for each SQL dialect and dialect-agnostic query construction. Backend adapters own native SQL; schema provisioning and write commands use the corresponding backend operations.
- FOR CLI output assertions in *tests*, normalize captured stdout or stderr with `tests._cli.plain_cli_output(...)` before comparing text. GitHub Actions color output can insert ANSI escapes inside visible tokens such as `--version`, causing raw substring assertions to fail. Reproduce this environment with `CI=true GITHUB_ACTIONS=true TERM=xterm-256color just test <test>` when diagnosing this failure.
- EVERY change to the `StorageBackend` or `SnapshotBucket` protocol MUST update and verify all current backend or bucket implementations, respectively, in the same change. Explicitly implement supported behavior or documented inapplicability for each provider; do not rely on an inherited default to silently cover an unreviewed provider. Shared orchestration MUST select behavior through protocol operations or capabilities, never provider-name selectors. Future providers must satisfy the complete contract.
- ALWAYS log unexpected non-fatal exceptions where they are handled, including operation context and traceback; respect configured logging disablement and **never** log sensitive data; ALWAYS redact or obfuscate sensitive data according to privacy and sharing policy.
- ALWAYS handle operational exceptions gracefully: log the failure, mark the affected operation as failed, clean up safely, and let scheduled workers continue independent work and future cycles. Configuration errors are fatal; other failures should stop the worker only when continuing could corrupt data or lose history. Never report failed work as successful or let cleanup errors hide the original failure.

### Prohibitions

The following actions are **prohibited** and are reserved exclusively for the user. When encountering a task that involves a prohibited action, you MUST **stop** and **report** to the user the conflict:

- NEVER remove comments marked with the `AGENT: do not remove` preamble or their explanatory continuation lines.
- NEVER modify the golden JSON fixtures in `tests/fixtures/`.
- NEVER attempt to publish to PyPI or invoke any publishing command; consequently, NEVER use `just publish` or `just release`.
- NEVER attempt to perform a release to GitHub or invoke any release command.
- NEVER attempt to push git changes to GitHub.

### Persistence rules

- NEVER issue DDL from a collection path on any backend; DDL runs only in `bassoon init` or registered schema upgrades.
- NEVER add leases, fencing, or serialization to Append-and-compact collection or curation writes; duplicate work is accepted.
- ALWAYS reuse a batch's `run_id` and every row's `event_id` across retries of that batch, in both persistence architectures.
- ALWAYS keep the shared ordering and tie-break policy in `storage_model.py` as the single source; update every dialect and the `sql_parity` tests when it changes.
- NEVER move `collection_ledger` publication earlier than the fact and debug loads without separating client outcomes from the preflight completion signal.
- ALWAYS treat restore as requiring a stopped, quiescent destination.

### Append-and-compact rules

- Usage and curation observations MUST append to raw tables; permanent audit streams such as `collection_ledger` append directly to their own tables; NEVER UPDATE, DELETE, or MERGE raw or gold from these paths. Only scheduled compaction and atomic restore write gold.
- NEVER rebuild a gold unit from raw alone; recompute from existing gold plus retained raw observations.
- NEVER gate compaction progress or canonical-view pruning on `collected_at` timestamps or a compaction watermark; progress is per-arrival-bucket row counts.
- NEVER key raw expiration by usage `day` or by a client-supplied timestamp; use backend arrival time.
- NEVER let a compaction pass read more than one point in time, or commit gold changes without the matching `compaction_ledger` progress.
- ALWAYS document a new Append-and-compact backend with a mapping block like BigQuery's below. Before implementation, verify every publication, retention, canonical-view, tombstone, compaction, and health requirement in the architecture contracts below; document the platform mechanisms that satisfy them.

### BigQuery-specific rules

- NEVER partition BigQuery raw tables by usage `day` or by `DATE(collected_at)`; use ingestion-time (arrival-day) partitions with 90-day partition expiration.
- NEVER add staging tables, MERGE, or schema jobs to the BigQuery collection path; publication is Parquet `WRITE_APPEND` load jobs only.
- ALWAYS read raw and gold at the transaction cutoff with `FOR SYSTEM_TIME AS OF` in `compaction.sql`.
- ALWAYS import `google-cloud-bigquery-datatransfer` lazily and only in explicit provisioning, recovery preparation, maintenance inspection, or registered physical schema upgrades. Ordinary collection and curation must never import it.

## Design Brief

The main **goal** is to *never* lose token-usage history. Users in ephemeral environments (containers, rotating VMs) should be able to persist agent usage with a single injected credential or ambient authentication where supported, without local credential files (for example, `MOTHERDUCK_TOKEN`). Design goals include:

- Persist the following statistics:
  - costs
  - input/output/cache-read/cache-write/reasoning tokens
  - data granularity: per-session, per-model, per-day
  - point-in-time pricing
  - session timestamps
  - project attribution
  - system details: OS, CPU, RAM, shell environment
- Idempotent design, backend-specific ingestion — safe to run on a cron from N machines
- Query supported relations with bounded read-only SQL; return a pandas DataFrame in two lines, with optional Polars support
- User-curated organization: global workspace/client/session tags and source-scoped session notes
- Terminal-first text-based reporting via `rich` and rendering of charts via `plotext`
- Pluggable storage backends: DuckDB (local, no external service), MotherDuck, and BigQuery; independent snapshot buckets: local files and GCS

The following are out-of-scope and/or antithetical to the design goals:

- TUI, web server, or HTML report generator
- Parsing of agent session files — in v1 we will use `tokscale` to extract token data
- Persistence of tokscale's generated summary fields with non-deterministic provenance: `title`, `task_category`, `description`, and `task_group`

### Important Design Decisions

- **Interoperability:** The following axioms must **never** be broken:
  - **Backend-Agnosticism:** A user should be able to snapshot their data and move it to whatever data warehouse they wish.
  - **Bucket-Agnosticism:** A user should be able to move an existing snapshot archive to whatever object store they wish.
  - *Important Corallary:* Snapshots stored in any bucket must be able to be restored to any backend.

- **tokscale derived metrics:** We invoke `tokscale` as a subprocess and treat its stdout as the only source of truth for token usage statistics. A future major version may make token statistics collection native.

- **Daily tokscale pricing:** `price_versions` stores the tokscale rates observed for each model with activity on a processed day. `daily_cost` calculates `cost_usd` from those rates and daily token components; `tokscale_cost_usd` remains diagnostic. Historical price drift before collection is the downstream user's responsibility.

- **No at-rest obfuscation of stored data:** Persist full-fidelity data. `bassoon export` obfuscates sensitive fields by default; `--raw` exports original values in any supported format. See Privacy and sharing policy for command-specific behavior.

- **Library + CLI:** Everything the CLI does is importable Python.

- **Source identity:** `source_id` is a UUID in configuration, generated by `bassoon init`. It namespaces every collected fact and session note. For global tags it records the source emitting the winning mutation and does not scope the target. Reuse it only for environments intentionally representing one source.

- **Persistence architectures:** UsageBassoon uses two persistence architectures according to backend write behavior:
  - **Direct transactional upsert:** Apply each normalized batch directly to current-state tables in a bounded transaction. This architecture suits backends where batched upserts are inexpensive and practical; append-only audit and debug streams may coexist.
    - Current implementations: DuckDB and MotherDuck.
  - **Append-and-compact:** Its defining structure is append-only raw tables, compacted gold tables, tombstones for curation deletes, and scheduled transactional compaction.
    - Current implementation: BigQuery.
    - Candidate fits: Redshift and Microsoft Fabric; validate their backend behavior before implementation.
    - **Append-and-compact contract:** Every Append-and-compact backend provides the same behavior, whatever its native mechanisms.
      - **Publication:** Collection and curation publish independent atomic table appends with stable observation IDs: usage/curation and diagnostics enter raw, while permanent audit outcomes enter `collection_ledger` directly. Canonical deduplication makes retries logically idempotent. Publication across tables is not a transaction, and it waits for load acceptance, not for compaction. Collection and curation never issue DDL, staging-table workflows, synchronous MERGE or UPDATE, or leases and fencing against raw or gold tables.
      - **Raw retention:** Raw observations and diagnostic events expire after 90 days by backend arrival time. Usage `day` may be a clustering or sort attribute, never the expiration key; historical backfills therefore arrive in fresh buckets. Durable gold never expires.
      - **Canonical views:** State views combine gold with all retained raw observations and deduplicate by natural key using `storage_model.py`. They never prune raw by a compaction watermark or `collected_at`. Diagnostic views deduplicate by `event_id` before counting observations or resolving latest state; diagnostic freshness is defined below.
      - **Gold layout:** Gold layout follows each table's replacement unit: durable usage and price tables are organized by usage day where the platform supports efficient per-day replacement; sessions and curation tables are keyed by natural key. Collection never writes gold.
      - **Tombstones:** Curation deletes append tombstones. Compaction retains them in gold; canonical views hide winning tombstones, preventing older retained raw values from resurrecting deleted assignments.
    - **Append-and-compact compaction contract:** Scheduled compaction is the only routine writer of gold; atomic restore is the only other. Every Append-and-compact backend implements these rules, using a platform-native scheduler that `bassoon init` installs or updates idempotently. The default cadence is nightly.
      - **Atomic pass:** Commit gold changes and progress together. Passes must be safe to rerun and prevent conflicting progress from overlapping runs; use a singleton conflict row or equivalent native overlap control.
      - **Pinned read point:** All gold, raw, count, and deduplication reads observe one consistent point in time; later arrivals enter the next pass. A platform without point-in-time reads must document an equivalent guarantee before it qualifies.
      - **Candidacy by counts:** `compaction_ledger` records incorporated counts per `(source_id, domain, usage day, arrival bucket)`, with a sentinel day for domains without usage days. A bucket qualifies when its visible count exceeds its processed count. Never compare counts across buckets or gate progress by timestamps: bucket expiry must not mask new arrivals, and client clock order does not establish backend visibility order.
      - **Recompute from gold plus raw:** Choose winners from existing gold plus retained raw using the shared ordering. Never rebuild gold from raw alone: raw expires, and gold may hold keys that no longer exist in raw. Compaction preserves usage keys absent from a batch.
      - **Scope and health:** Raw diagnostic tables are never compacted. `bassoon doctor` reports overdue raw buckets before expiration becomes a history-loss risk, and compaction must continue to succeed within the 90-day retention window. Replayed rows can cause harmless extra work.

- **BigQuery (Append-and-compact implementation):** Each platform-neutral concept above maps to BigQuery as follows.
  - Publication uses Parquet load jobs with `WRITE_APPEND`, one concurrent job per raw table. This reuses the snapshot Arrow-to-Parquet path; duplicate appends are resolved by canonical views. Storage Write API deliberately not adopted; revisit only for sub-minute collection or synchronous read-after-write needs, behind the existing adapter interface.
  - Arrival bucket: raw tables use ingestion-time (arrival-day) partitions with a 90-day partition expiration. Historical usage `day` is a clustering key where applicable. Initialization clears expiration on durable tables, including expiration inherited from dataset defaults.
  - Gold layout: usage and price tables partition by usage `day`, the atomic-replacement unit under compaction. Sessions and curation tables are unpartitioned and keep natural keys. Partition keys follow lifecycle axis: arrival for expiring raw data, usage day for durable gold data.
  - Scheduler: `bassoon init` installs or updates one Scheduled Query at 02:00 UTC from the packaged `sql/bigquery/compaction.sql` through the Data Transfer API (`google-cloud-bigquery-datatransfer`, imported lazily in the provisioning path only; `types-protobuf` is a dev-only dependency for Pyrefly).
  - Atomic pass and pinned read point: the script runs in one transaction, binds its cutoff to that transaction's `CURRENT_TIMESTAMP()`, and reads raw and gold with `FOR SYSTEM_TIME AS OF` that cutoff. A singleton conflict row guards against overlapping scripts.
  - Views: `compaction_backlog` exposes overdue arrival buckets to `bassoon doctor`.

- **Initialization and schema updates:** `bassoon init` provisions idempotently. Missing or incomplete `schema_marker` blocks ordinary backend open. Retrying interrupted non-atomic init validates and preserves managed tables, completes missing objects, then marks readiness. Matching version/hash uses metadata preflight with no schema jobs. Older supported schemas use only registered, hash-gated migrations recorded in `schema_migrations`; newer versions or unexpected hashes fail closed. The migration registry is currently empty, with no migration SQL files; baseline DDL is never a fallback migration. The hash covers installed and scheduled SQL, including compaction. Changing these assets is a schema change; changing released table shapes requires a version bump and registered migration because idempotent create statements do not reshape tables. Before public contracts exist, prerelease changes may replace the baseline and reset disposable resources without adding legacy migrations; schema preflight still rejects mismatched hashes.

- **Collection and row identity:** Both architectures use a UUID `run_id` for each collection, validated during normalization and stored as canonical text. Compaction generates its own run IDs. Each normalized observation has an `event_id`, retained across retries and used as the final tie-break; natural keys define logical identity. `schema_marker` and `schema_migrations` have no `event_id`.

- **Observation ordering and tie-breaks:** Freshness sorts by `collected_at DESC`. For identical timestamps: tags and notes favor a surviving `upsert` over `delete`; diagnostic issues favor resolution over the raised event; `daily_stats` favors the larger `total_tokens`; `price_versions` favors a non-null, larger output-token rate; `collection_ledger` outcomes favor the later `finished_at`. `event_id DESC` is the final deterministic tie-break. This policy resolves ties; it does not correct clock skew between collection environments. Canonical views, compaction, snapshots, and restore share the policy defined in `storage_model.py`; never reimplement it ad hoc in one dialect without parity coverage.

- **Backend source-column invariant:** Every persisted backend base table, including facts, curation, audit, and history tables, MUST contain a non-null `source_id`. Every source-scoped current-state table's natural key MUST include `source_id`. Tags are global and exclude it from their natural keys and curation joins; their non-null `source_id` is mutation provenance. Notes use `(source_id, client, session_id)`, matching their exact target session, and their session joins MUST include `source_id`. Append-only tables may use another globally unique key such as `run_id`, but MUST still store `source_id`. Preserve this invariant in every SQL dialect and in normalization, persistence, snapshot, and restore paths.

- **Append-and-compact publication policy:** Partial publication across tables is tolerated. Retryable backend publication failures, including partial BigQuery publication, use bounded exponential backoff with jitter in `persistence.py`, preserving the batch's `run_id` and row `event_id` values across retries. Publish `collection_ledger` after fact and debug loads succeed: preflight uses completed outcomes to skip historical work. Ledger failure may cause harmless replay. Never gate facts or compaction on ledger presence, or publish completion earlier without separating client outcomes from the preflight completion signal.

- **Diagnostic freshness:** Doctor reads unresolved drift and reconciliation through `open_schema_drift_events` and `open_reconciliation_issues`, filtering by latest `collected_at` within 90 days. DuckDB/MotherDuck physical pruning remains opportunistic; no background expiry worker is required.

- **Same-source concurrency:** DuckDB collection uses a local OS lock; MotherDuck transaction conflicts retry with bounded jitter and a warning. Append-and-compact accepts independent remote appends under the publication contract above. Same-key curation follows the shared observation ordering; do not add application-level leases or fencing to these writes. Snapshot catalog reservations are a separate requirement.

- **Concurrent collection and snapshot safety:** Independent environments can collect into one backend and compete for snapshot publication without sharing a process lock.
  - `source_id` namespaces usage facts from different sources. Remote collection does not require a source lease.
  - All snapshot destinations require compare-and-swap catalog reservations with owner, expiry, and fence checks. If multiple UsageBassoon instances attempt a snapshot, a writer claims every configured destination before capture. Only the holder of the current reservation and fence can publish. Reservations renew through capture, uploads, and publication of each destination. Object-store catalog updates and deletion use provider-specific version preconditions. Local catalog compare-and-swap holds an OS file lock through version check and atomic replacement.
  - Publication is atomic within each destination catalog after all tables and the manifest are complete. Multiple destination catalogs publish sequentially; a failure attempts to roll back published entries, releases owned reservations, and cleans up uploaded objects. There is no transaction across providers.
  - Required graph/models failures abort usage publication and attempt to append a failed client outcome. Pricing failures are tolerated per model and report failures per day; successful results still enter the bundle. `collection_ledger` records complete, partial, or failed outcomes for retry. Completed historical models/pricing work may be skipped later.

- **Session and curation identity:** Sessions and notes both use `(source_id, client, session_id)`; workspace remains session metadata. Notes belong only to the exact source-scoped session and must never be read, edited, deleted, or joined through another source's matching client/session identifiers. Notes additionally persist a deterministic `note_id` derived by `storage_model.note_id_for_session`: UUIDv5 in `NAMESPACE_URL` over the compact, Unicode-preserving JSON array `["usagebassoon", "notes", source_id, client, session_id]`, encoded losslessly as `n_` plus 22 base64url characters. This 24-character handle is stable across edits and deletion/recreation; it is an alternate access handle, never a natural key, compaction deduplication key, or observation tie-breaker. Preserve the derivation and encoding across independent writers. Client and workspace are peer scopes, not a hierarchy. Tags use `(scope, client, workspace, session_id, tag)` globally, so matching targets share tags across sources. Effective session tags combine direct session tags with global client and workspace tags. Tag targets have no surrogate IDs; global session tags assume session IDs are unique within each client. Revisit this assumption if source evidence reveals collisions; schema-shape drift alone cannot establish identity collisions.

- **Curation operations and conflicts:** Operations share the `upsert` and `delete` contract across architectures. Append-and-compact retains append history and uses nullable `op_id` to correlate events from one command; in BigQuery, a rename appends an old-key delete and new-key upsert in one load, sharing `op_id` and keeping separate stable `event_id` values. Direct transactional upsert applies renames transactionally and physically deletes removed assignments in DuckDB/MotherDuck. Curation conflicts follow the shared observation ordering; no application-level serialization is added. Views and compaction use the winning row's fields. `created_at` belongs to the assignment lifetime: edits and renames preserve it, explicit deletion followed by re-add starts a fresh lifetime. `updated_at` tracks the latest meaningful mutation; unchanged add/set calls are no-ops. For tags, `source_id` records the source emitting the winning mutation; for notes, it identifies the target session's namespace.

- **Daily facts:** `tokscale graph` supplies candidate dates only. For each candidate day, date-filtered `tokscale models` supplies `daily_stats` at `(source_id, day, client, session_id, model)`. Completed historical targets skip by default; the current day refreshes.

- **Token calculation invariants:**
  - "reasoning" tokens are a component of the total token count such that total_tokens = input + cache_read + cache_write + reasoning + output tokens (fixture-verified against tokscale 4.15.1).
  - "reasoning" tokens are considered output tokens for pricing purposes (fixture-verified against tokscale 4.15.1).

- **Data ingest pipeline:** `bassoon collect`:
  1. Resolve tokscale (`TOKSCALE_BIN`, else `tokscale` on PATH, else `bunx tokscale@latest`). Record version from graph payload meta.
  2. Run `graph`, use its contribution dates to select daily models work, run date-filtered `models` per required day, then fetch prices for each model used on those days and `report --no-summarize`.
  3. Validate against the schema contract with pydantic strict mode. Invalid required graph/models fields *abort* usage publication; report/pricing validation failures follow the per-day/per-model tolerance above. Unknown fields, type changes, and changed cardinalities produce **drift events** — appended to ephemeral `schema_drift_events` by source, command domain, Tokscale version, and drift key, surfaced in output, and surfaced on the next `bassoon doctor` until a complete clean validation resolves them.
  4. Normalize to Arrow tables, compute derived columns, and assign the stable collection and observation IDs described above.
  5. Publish through the selected persistence architecture, following its publication and retry rules above.
  6. Collection does not trigger archival. Independent snapshot jobs check scheduled/weekly obligations through `SnapshotArchiver` without waiting for collection or requiring tokscale.

- **Backend SQL management:** Each backend installs native DDL and views under `sql/<dialect>/`; MotherDuck shares the DuckDB assets. CI checks structural parity with SQLGlot and behavior with synthetic replay and native backend tests.

- **CI workflows:** `CI Local` (`.github/workflows/ci-local.yml`) runs on pull requests targeting `main` and manual dispatch. `CI` (`.github/workflows/ci.yml`) runs on pushes to `main` and `dev`, repeating local checks and adding protected BigQuery, MotherDuck, and GCS integration tests through the `ci-live` environment. `Release` (`.github/workflows/release.yml`) requires a successful `CI` push run on `main` for the exact tagged commit.

- **Backend SQL CI: `.github/workflows/ci-local.yml` and `.github/workflows/ci.yml`** — structural and synthetic parity checks:
  1. `SQLGlot` parses both dialects' DDL/views.
  2. Compares shared logical table schemas and report view ASTs; backend-specific raw tables, canonical ingestion views, and compaction SQL deliberately differ.
  3. **Structural replay tests:** Use SQLGlot to compare shared logical table columns and view definitions across dialects, then replay transpiled SQL with shared synthetic rows in DuckDB. Keep these checks in sync as tables and views change. Transpiler parity is *structural*, not semantic.
  4. **Synthetic replay tests:** Run the shared rows through each backend's native SQL and compare every shared view to observe dialect-specific behavior. Update these tests when a view or its input tables change. For tables unused by views, use structural and focused backend tests.

- **Ingest semantics:** Daily models define session/model usage state; reports add session metadata. `session_model_stats` calculates all-time totals over `daily_stats`. Curation deletion follows the persistence architecture: retained gold tombstones for Append-and-compact, physical deletion for current Direct transactional upsert implementations.

- **Snapshot semantics:** Snapshots are portable, normalized-state, whole-backend archives, not raw-payload replay points. They preserve every source and observation ID. Backend-native dumps must not bypass canonical Arrow and archiver-owned Parquet serialization.
  - Backend reads, incremental Parquet writing, uploads, verified downloads, and transactional restore use bounded batches/files. Keep the DuckDB/MotherDuck transaction open through streaming; BigQuery physical reads use one captured timestamp. Temporary disk failure must leave destination application data unchanged.
  - `manifest.json` and `COMPLETE` are immutable. Manifest objects use snapshot-directory-relative names, row counts, schemas, sizes, and content hashes; provider generations never define portable identity. Completion evidence binds the immutable manifest digest. `state.json` travels with the snapshot and stores mutable pins/lifecycle metadata under compare-and-swap.
  - Valid immutable snapshot directories are independently recoverable regardless of missing, damaged, unpublished, or retired mutable state. Mutable metadata governs archive management, never emergency reading. `catalog.json` is a derived index. `control.json` independently owns reservations, fences, archive identity, and authoritative destination retention policy, so index repair cannot erase a live claim. Missing indexes can be discovered read-only; explicit repair validates published snapshots, preserves corrupt index evidence, and writes under the current reservation. Routine management excludes unpublished/retired directories and orphaned same-archive projections; emergency recovery excludes only invalid immutable contents. Retain retirement records only through unfinished deletion. Delete exact observed immutable revisions, delete portable state last, verify absence, then remove the authoritative record through fenced CAS; preserve the monotonic archive fence. Automatic publication retries pending cleanup without blocking independent work; recovery reservations never perform cleanup. Repair preserves corrupt index evidence and verifies candidates before separate abandoned-stage cleanup.
  - Claim every participating destination in deterministic order before capture. Renew through all capture/upload/publication phases. Publication, pinning, and retirement authorize transitions through the same control-document CAS as owner/fence checks. Catalogs and sidecars are projections. Downloads attempt reservations but must support verified read-only recovery when mutation authority is unavailable. A stale owner cannot publish or retire another owner's snapshot and stops that attempt until the next cadence opportunity. Cleanup uses owned identities and exact observed provider versions. Never classify active capture as abandoned from directory age alone.
  - `latest` sorts globally by capture time, grouping copies of an identity before older recovery points. Automatic selection warns on unavailable/corrupt copies and reports the actual selected URI/time. Exact ID/directory/manifest selections fail explicitly. Explicit archive roots allow fallback within that root and override configured locations. Fallback finishes before destination writes and never follows a destination transaction failure.
  - Manual `bassoon snapshot` bypasses cadence and pins by default; `--no-pin` opts out. Pins do not rewrite content hashes, are exempt from rotation, and survive copying/index reconstruction. No unpin command: `snapshot delete` displays selected copies/pin/weekly roles and requires typing `DELETE`. Retired copy cleanup is retryable; explicit URIs restrict the scope.
  - Scheduled captures use the configured interval (default twelve hours) and unpinned retention count (default three). Local automatic snapshots default to disabled. Enabled destinations capture the current UTC week unless their provider-specific `snapshots.local.disable_weekly` or `snapshots.gcs.disable_weekly` is true. Retain four successful weekly slots; never fabricate missing weeks or rotate an old point after failed publication. A capture can fulfill both obligations and survives while either class retains it. Manual captures do not reset automatic clocks. Separate native snapshot jobs and a dedicated worker loop service snapshot deadlines even while collection is blocked or fails; `snapshot --automatic` is due-only and unpinned; stopped hosts cannot execute captures. Weekly health derives available target slots, oldest recovery point, and coverage gaps from present snapshots; do not maintain a historical snapshot tally.
  - Shared destination policy is authoritative. Ordinary publication must reject conflicting destructive settings; only an explicit `snapshot policy` operation reconciles the ceiling. Different instances may request different cadence intervals or destination subsets without redefining shared deletion policy.
  - Capture canonical gold plus retained raw state, including uncompacted observations, deduplicated ledger outcomes, and diagnostic event streams. Exclude physical schema/compaction metadata and restore receipts. Deleted curation assignments and original agent files/tokscale payloads are not archived.
  - Restore requires a fresh, initialized, empty, quiescent destination. Check all base tables, including populated unexpected tables, before disabling maintenance; repeat emptiness checks inside the native atomic restore transaction. Schema/bootstrap/control metadata and restore receipts are explicit exceptions. Stop collectors, native schedules, workers, cron, and external writers. The CLI validates first, checks emptiness, warns about matching source evidence, then confirms with default No before changing maintenance or data.
  - `init --restore` provisions recovery without collecting or starting backup jobs, checks emptiness, and disables applicable maintenance. Restore independently disables maintenance and rejects pending/running compaction; inspect transfer runs and project-wide jobs rather than assuming disablement proves quiescence. BigQuery recovery requires `bigquery.jobs.listAll` for cross-instance job visibility and fails closed when inspection is unavailable. Maintenance remains disabled on success/failure; plain `init` enables it after user verification. Keep source IDs of continuing collection environments; never infer identity from hardware or remap archived IDs. Source-matching advice is optional and must not block verified immutable recovery.
  - A snapshot must restore across every supported backend and relocate across local/GCS roots without rewriting contents or identity. Restore validates original bytes, applies registered forward transformations on separate files, validates current schemas/semantics, then publishes atomically. BigQuery loads owned, expiring stages and commits durable gold, diagnostic raw, audit outcomes, and a completion receipt together. DuckDB/MotherDuck insert verified Arrow batches and the receipt in one transaction. Receipt lookup resolves lost acknowledgements. Stage cleanup must verify ownership and terminal job status, and immediately discard disposable stages without waiting for expiry. Use stable digest-bound logical restore IDs and unique attempt IDs; names alone never authorize deletion. All maintenance RPCs and polling share an operation deadline; report known maintenance state on success, failure, and committed retries without hiding the original error.

- **Source audit aggregation:** Query the installed dialect-specific `audit_sources` view; snapshot audits reuse its packaged DuckDB definition; snapshot audits query verified Parquet with memory-limited, disk-backed DuckDB. Retain one Python summary per source, not one identity per historical run.

- **Explicit archive access:** Resolve explicit paths/URIs independently of publication enablement, construct only needed adapters, and preserve authentication settings for disabled providers. CLI recovery access to a location outside enabled archive roots requires a separate Y/n confirmation; restoration still requires its default-No stopped-writers confirmation. Inspection and recovery discover immutable directories even when mutable metadata is damaged. Portable sidecars must not regress after takeover, and projection failure must be reported before relocation. Newly relocated complete directories remain discoverable when authority records cover only a subset. An absent restore receipt proves failure only after owned jobs are terminal; preserve staging and report unknown completion otherwise. Discard rejected candidates' private files before fallback.

- **Provider construction:** `config.py` parses, validates, and persists configuration without constructing storage adapters. `backends/base.py` and `buckets/base.py` define contracts and shared abstractions; provider selection, credentials, settings application, and lazy adapter construction belong in each family's `factory.py`. Application backend opens use `backends/factory.py`; normal opens require schema preflight and close safely on failure, while explicit initialization skips preflight without implicitly issuing DDL or changing maintenance. Registries are explicit constructor mappings, not plugin discovery. Collection, diagnostics, snapshot orchestration, and recovery operate through provider contracts.

- **Snapshot buckets:** Every provider implements bounded file upload/download, version-aware metadata reads, compare-and-swap, exact-version deletion, listing, and lifecycle warnings. URI-scheme registration belongs in `buckets/factory.py`; archiving accepts arbitrary destination URI sequences, protocol adapter sequences, and a generic bucket factory. Shared orchestration and reading must not assume GCS is the only remote provider or treat unregistered remote schemes as local paths. `ScopedSnapshotBucket` and provider-neutral URI helpers belong in `buckets/base.py`; scoped adapters reuse any provider's authenticated operations for nested archive roots. Versions identify physical revisions, including identical-content recreations, separately from portable content hashes. Local publication flushes/syncs temporary files, atomically publishes them, and syncs containing directories where supported; bounded stable lock stripes coordinate replacement and deletion. GCS resolves revisions at the current location; copied generations need not match the producing location. Portable hashes remain the integrity gate.

- **Public compatibility contracts:** Application SemVer identifies the producer; `storage_model.DATA_SCHEMA_VERSION` identifies portable data meanings/schemas; `snapshot.format.FORMAT_VERSION` identifies packaging; `schema_assets.SCHEMA_VERSION` plus hash identifies the physical SQL installation. Record all four responsibilities in manifests. An incompatible public persistence or archive interface requires a major application release; compatible additions/fixes do not automatically require one. A major release does not justify abandoning backup recovery.
  - Every publicly released format/data contract retains a tested recovery path. Register immutable format readers, historical Arrow contracts, contiguous forward file transformations, and semantic validators in `snapshot/format.py`. Independently validate every intermediate schema and required field in isolated candidate directories. Expose verified raw provenance even when compatibility is unsupported. Preserve source/event identity and original archives. Removed direct readers need a documented tested conversion path; historical released packages are an emergency fallback.
  - Physical backend SQL migrations remain separate in `schema_assets.py`: registered native assets and previous/target hashes for every dialect, contiguous steps, ledger recording, and no baseline-DDL fallback. Automatic upgrades preserve deliberate maintenance pauses. Prerelease refactors replace the baseline and reset disposable test resources; do not invent legacy conversions before public contracts exist.

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

- **Storage backend abstraction:** `StorageBackend` in `backends/base.py` defines publication, queries, consistent snapshot reads, and atomic restore using Arrow. Implementations:
  - `duckdb_local.py` — local file; `register(arrow_table)` is zero-copy
  - `motherduck.py` — identical code path, `md:` connection string
  - `bigquery.py` — `google-cloud-bigquery`; Append-and-compact publication and restore. The mapping above defines provisioning, partitioning, and compaction; active transactions are exposed to `bassoon doctor`.

  Derived columns (`total_tokens`, `session_label`) are computed **in the normalizer**; calculated costs remain dialect-paired views. The pipeline is:
  ```
  tokscale JSON → pydantic contract validation (strict, drift events)
              → pydantic models (typed objects)
              → Arrow normalizer (batch, derived columns, canonical schema)
              → StorageBackend (dialect-specific executor)
              → pandas/Polars via `query()`, or Arrow via `query_arrow()`
  ```

- **Notable SQL semantics:**
  - `last_active` is tokscale session metadata; `first_seen_at` and `last_seen_at` track UsageBassoon observations.
  - `collected_at` orders observations; shared tie-break policies and natural keys are in `storage_model.py`.

- **General storage model:** Durable logical state comprises `sessions`, `daily_stats`, `price_versions`, `tags`, and `notes`. `collection_ledger` outcomes are permanent; physical replay appends are deduplicated in canonical views. Diagnostic streams have a 90-day visibility window, with physical pruning as described above. Append-and-compact progress uses per-arrival-bucket counts; BigQuery implements buckets with ingestion-day partitions. Schema and compaction metadata are excluded from snapshots.

### Canonical Ingest Commands

These are the `tokscale` commands used to generate ingest data. Each command is authoritative for its data domain. Graph supplies candidate dates only; it is not reconciled with daily models totals:

- `tokscale models --json --group-by client,session,model --since <YYYY-MM-DD> --until <YYYY-MM-DD>` — authoritative for daily statistics with session-level granularity.
- `tokscale report --json --no-summarize --since <YYYY-MM-DD> --until <YYYY-MM-DD>` — authoritative for session metadata.
- `tokscale graph` — authoritative for candidate dates; no activity table is persisted.
- `tokscale pricing <model-id> --json` — authoritative for the rate observed while processing a daily usage fact.

### Implemented SQL views (per-dialect: `sql/{duckdb,bigquery}/views.sql`)

The DuckDB/MotherDuck and BigQuery assets implement shared canonical, collection planning, diagnostic, and report views; BigQuery additionally exposes `compaction_backlog`:

- `daily_cost` applies the observed source/day/model rates to daily token facts. It returns `NULL` cost when a nonzero token category has no matching rate; reasoning uses the output rate.
- `session_model_stats` aggregates daily facts across time at source/client/session/model grain. Its cost is `NULL` unless every contributing daily fact has a known cost. `session_model_stats_current` is an alias with identical rows and no current-time filter.
- `report_daily_usage` exposes daily facts with workspace metadata. `report_session_models` exposes session/model totals with workspace and last-active metadata. These are the filterable report inputs; CLI reporting applies source, client, model, workspace, and effective-tag filters before aggregating.
- `report_summary` and `report_models` provide global session and model aggregates. They omit source/client/workspace dimensions, and their `SUM(cost_usd)` ignores `NULL` inputs, so totals may be partial when pricing is incomplete.
- `session_tags` resolves global client, workspace, and session tags while preserving tag scope. `tagged_sessions` joins those effective tags to session metadata.
- `noted_sessions` joins notes to their exact source/client/session and includes `note_id`. `session_notes` exposes active notes with `note_id`, the complete session key, text, and creation/modification/collection dates; curation listing and UUID lookups use this shared view.

The report source views retain the dimensions needed for filtering; the pre-aggregated summary views do not. Consumers that need to distinguish incomplete pricing should use the detailed views or explicitly check cost completeness before aggregating.

### CLI

The installed command is `bassoon`. The commands below are implemented.

| Command | Purpose |
|---|---|
| `bassoon init [--restore]` | repair missing source identity, provision schema, configure normal/recovery maintenance |
| `bassoon collect` | one delta-ingest cycle (designed for cron) |
| `bassoon query <relation>` | bounded query of a supported relation; raw output with sharing warning |
| `bassoon report summary/models/sessions/daily/graph` | terminal reports; `--sanitize/--obfuscate` for sharing |
| `bassoon tag add/rename/remove` | global user curation with source provenance |
| `bassoon note set/edit/remove` | source-scoped session notes; edit/remove additionally accept `--id` |
| `bassoon note list` | recent notes in 16-row pages with ten-word previews; `--page`, `--source-id`, `--no-pager`, and `--json` |
| `bassoon note describe <note-id> [<note-id> ...]` | note identity and creation/modification dates in YAML; `--json` for JSON |
| `bassoon restore` | restore a snapshot into an initialized, empty backend |
| `bassoon snapshot [list/inspect/audit/pin/delete/copy/repair/policy]` | pinned manual capture or protected archive management |
| `bassoon export <relation> <path>` | export a supported table/view to parquet/csv/json; obfuscated by default; `--raw` for raw data |
| `bassoon audit runs/sources/restores/snapshot(s)` | run/source evidence, owned restore stages, or shared snapshot integrity audit |
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

### Python API

pandas is the default; Polars is supported through the optional extra. `connect()` returns a `StorageBackend` that the caller must close:

```python
import usagebassoon

df = usagebassoon.query("SELECT * FROM report_models LIMIT 1000")  # pandas
df = usagebassoon.query("SELECT * FROM daily_cost LIMIT 1000", engine="polars")
tbl = usagebassoon.query_arrow("SELECT * FROM report_session_models LIMIT 1000")
backend = usagebassoon.connect()      # StorageBackend; caller owns its lifetime
backend.close()
```

### Default Configuration

```toml
# Platform-specific config path; override with USAGEBASSOON_CONFIG.
source_id = "018f2d70-0000-4000-8000-000000000000" # UUID source namespace; generated by `bassoon init`
[backend]
provider = "duckdb" # duckdb (default) | motherduck | bigquery
# [backend.duckdb]
# database = "/path/to/usagebassoon.duckdb" # defaults to the platform data directory

[tokscale]
# bin omitted: TOKSCALE_BIN, then tokscale on PATH, then bunx tokscale@latest.
env = []
timeout = "180s"
max_stdout_bytes = 67108864
max_stderr_bytes = 8388608

# [backend.bigquery] # only when backend.provider = "bigquery"
# project = "my-project" # required; no default
# dataset = "my-dataset" # required; no default
# location = "US"
# credentials: GOOGLE_APPLICATION_CREDENTIALS, or `gcloud auth application-default login`
# credentials_file = "/path/to/service-account.json" # optional; default unset
# maximum_bytes_billed = 1073741824
# timeout = "120s"

# [backend.motherduck] # only when backend.provider = "motherduck"
# database = "my-database" # required; no default

# [snapshots.gcs] # optional GCS snapshot destination
# enable = false # set true to enable with uri and project
# uri = "gs://my-bucket/usagebassoon/snapshots"
# project = "my-project" # required when GCS is configured
# credentials_file = "/path/to/service-account.json" # optional; default unset
# timeout = "60s"
# disable_weekly = false # destination-specific weekly recovery policy

[collection.schedule]
interval = "15m" # default interval; scheduling is not installed by default

[collection]
max_retries = 3
retry_initial_seconds = 1.0

[logging]
disable = false
directory = "~/.local/state/usagebassoon/logs"
max_files = 5
max_bytes = 5242880

[snapshots]
max_snapshots = 3 # unpinned scheduled/manual retention; weekly slots and pins are independent

[snapshots.local]
enable = false
disable_weekly = false
# path = "/path/to/local/snapshots" # defaults to platform snapshot directory

[snapshots.schedule]
interval = "12h" # independent automatic cadence
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
