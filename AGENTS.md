<!--
SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
SPDX-License-Identifier: AGPL-3.0-only
-->

# UsageBassoon — Agent Context

## Purpose

A Python CLI and library (pipx-installable, import usagebassoon) that persists `tokscale` JSON token usage statistics to a persistent data store into DuckDB or MotherDuck. This allows users to aggregate token usage across agentic environments.

## Repository Structure
```
usagebassoon/
├── pyproject.toml            # hatchling; pipx-installable; Python >= 3.11
├── src/usagebassoon/
│   ├── cli/                  # typer app; one module per CLI command
│   ├── parsers/              # one module per payload kind
│   │   ├── models.py         # per session×model rows → session_model_stats
│   │   ├── report.py         # session metadata       → sessions (no LLM summary fields)
│   │   ├── graph.py          # daily contributions    → daily_stats/daily_activity/run_metrics
│   │   └── pricing.py        # rates + resolution     → pricing_snapshots + row stamps
│   ├── contracts/            # JSON schema contracts per payload kind
│   ├── backends/
│   │   ├── base.py           # StorageBackend protocol
│   │   ├── duckdb_local.py
│   │   ├── motherduck.py
│   │   └── bigquery.py
│   ├── collector.py          # tokscale subprocess + retry
│   ├── arrow_port.py         # normalizer: models → Arrow, derived columns
│   ├── drift.py              # schema_drift detection + reporting
│   ├── snapshots.py          # local/GCS rotating snapshots + restore
│   ├── merge.py              # staging + delta append + current-view logic + session_label
│   ├── reconcile.py          # cross-payload consistency checks
│   ├── curation.py           # tags + notes
│   ├── obfuscate.py          # export-time pseudonymization
│   ├── report_term.py        # rich tables + plotext charts
│   ├── api.py                # usagebassoon.query/connect (pandas default, polars opt-in)
│   ├── sql/                  # ddl + views, loaded as package data
│   │   ├── duckdb/{ddl.sql, views.sql}    # also serves motherduck
│   │   └── bigquery/{ddl.sql, views.sql}
├── tests/
│   ├── fixtures/             # sanitized golden captures: models, report, graph, pricing
│   └── test_*.py             # incl. fixture-derived invariants
└── .github/workflows/        # ci (ruff, pyrefly, pytest), dialect-parity, release to PyPI
```

## Architecture

- **Dependencies**: `duckdb`, `pandas`, `pydantic`, `pyarrow`, `typer`, `rich`, `plotext`, `polars` (optional).
- **Dev Environment**: `uv`, `hatchling`, `twine`, `pyrefly`, `ruff`, `pytest`, `just`, `pre-commit`.

```mermaid
flowchart TD
    subgraph ENV[Ephemeral container / VM / laptop]
        CRON[cron or systemd timer] --> COLLECT[bassoon collect]
        COLLECT -->|subprocess| TS[tokscale CLI — all JSON on stdout<br/>pricing &lt;model&gt; --json · graph<br/>models --json --group-by client,session,model<br/>report --json --no-summarize]
        TS --> NORM[validate against schema contract<br/>pydantic strict + drift log]
        NORM --> ARROW[normalize → Arrow tables<br/>derived columns computed here<br/>total_tokens · session_label · price stamps]
    end

    subgraph BACKEND[StorageBackend protocol — Arrow in, Arrow out]
        ARROW --> DELTA[delta engine<br/>compare vs current views<br/>append new keys + changed rows only]
        DELTA --> SQLW[bassoon query / report / doctors]:::noop
        DELTA --> CUR[(versioned append-mostly tables<br/>per-dialect DDL + views)]
    end

    subgraph IMPLS[Backend implementations]
        CUR --> LCK[duckdb — local .duckdb file]
        CUR --> MDK[motherduck — ATTACH 'md:...']
        CUR --> BQ[bigquery — dataset + MERGE DML]
    end

    CUR --> SNAP[bassoon snapshot<br/>Parquet → GCS · rotating · max_snapshots]
    subgraph SNAPS[Optional snapshot locations]
      LOC[(Local snapshot archive)]
      GS[(GCS snapshot archive)]
    end
    SNAP --> LOC
    SNAP --> GS
    SNAPS -.->|bassoon restore| CUR
```

## Commands

Run all project tasks via `just` from the repository root. Use `just --list` to inspect available recipes.

- USE `just install-dev` to synchronize all uv dependency groups.
- USE `just test` to run the test suite; pass pytest arguments with `just test <args>`.
- USE `just coverage` to run test suite with terminal coverage reporting.
- USE `just lint` to perform Ruff linting and formatting checks.
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
- NO version string is ever hard-coded in source; `hatch-vcs` manages version numbering from git tags (`v0.1.0` → `0.1.0`).

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
- User-curated organization: per-session and per-client tags, free-text notes
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

- **tokscale-resolved pricing only:** Rejected deriving price data from external APIs like Models.dev or LiteLLM because (a) usagebassoon wraps tokscale, and storing rates tokscale did *not* use breaks data fidelity between `cost_usd` and the embedded prices; (b) it would silently ignore downstream users' `custom-pricing.json` tokscale overrides; (c) corrective pricing under any rate card is a query-time recomputation users can do themselves.

- **No at-rest obfuscation of stored data:** Session data is stored raw at-rest and obfuscated at export-time. The CLI command `bassoon export` will *default* to obfuscating potentially personal information including session IDs, workspace names and paths, project names, and anything similar. For example, a project name may be pseudonymized as "project-alpha". Downstream users may optionally export their raw data as JSON via the flag `--raw-json`.

- **Library + CLI:** Everything the CLI does is importable Python.

- **Raw tokscale data semantics:** Raw tokscale JSON output from every `bassoon collect` is a *cumulative* state and will be used to compare against the database's current state. A collect run stages the incoming Arrow batch and upserts the data; **never** deletes.

- **Data ingest pipeline:** `bassoon collect` (target runtime < 10s):
  1. Resolve tokscale (`TOKSCALE_BIN`, else `tokscale` on PATH, else `bunx tokscale@latest`). Record version from graph payload meta.
  2. Run the extraction set: `pricing` per distinct model first (both calls then observe the same LiteLLM ~1h disk-cache state), then `models`, `graph`, `report --no-summarize`. All emit JSON on stdout.
  3. Validate against the schema contract with pydantic strict mode. Required-field absence **fails the run** with a clear error; *unknown* fields or changed cardinalities are **drift events** — recorded in `schema_drift`, surfaced in output, surfaced again on the next `bassoon doctor`, and the run continues (tolerant reader) so collection is never blocked by additive changes. (Be careful! this can cause migration issues if a hotfix is released!)
  4. Reconcile models ↔ report session overlap and cost drift; graph totals ↔ models grand totals per token type. Mismatches go to `ingest_runs` and are surfaced by `bassoon doctor`.
  5. Normalize to Arrow tables; compute derived columns. Stage each fact table in one batch.
  6. Stage the Arrow batch, match rows by natural key, update changed existing rows, insert new rows, and leave absent rows untouched. Use a transactional upsert/MERGE; **never** delete.
  7. Optionally: if `snapshots.interval` has elapsed, run `bassoon snapshot`.

- **Database management:** Locally, data will be managed and stored by DuckDB. Remotely, data will be managed and stored by either MotherDuck or GCP BigQuery. DDL and SQL views will be written natively to their respective dialect (e.g. `sql/duckdb/{ddl,views}.sql` and `sql/bigquery/{ddl,views}.sql`). `SQLGlot` will be used during CI to prevent structural drift. Dedicated pytests will be used during CI to prevent semantic drift against the golden fixtures.

- **Database CI: `.github/workflows/dialect-parity.yml`** — on every PR that touches `sql/` or `tests/fixtures/`:
  1. `SQLGlot` parses both dialects' DDL/views.
  2. Transpiles `bigquery/*` → duckdb dialect, asserts AST-equivalence against the duckdb tree (and vice-versa for the view sets).
  3. A **replay test** runs the same golden-fixture dataset through both dialects in DuckDB (translated BigQuery SQL) and asserts identical result sets. Transpiler parity is *structural*, not semantic.

- **Ingest semantics:** tokscale reports *cumulative* totals per session in `models`; dedupe keeps the latest snapshot per key and values are never summed across rows. `graph` contributions are authoritative daily totals at `(day, client, model)` and are upserted; repeated cumulative observations are never summed. Tags and notes are owned by the user and are never touched by merge — they persist across `collect` runs and are exported by `bassoon export` like any other table.

- **Snapshot semantics:** Snapshots are an optional layer that contain normalized tables that provide portability, restore, and seed of the current database state. They are not raw-payload replay points:
  - `bassoon snapshot` writes every table to Parquet locally at `~/.usagebassoon/snapshots/` (UTC timestamp in the filename) or remotely in GCS `gs://<uri>/<UTC-date>-<short-sha>/`, plus a `manifest.json` (table list, row counts, tokscale contract version, DuckDB/BigQuery schema hashes).
  - Rotation: after each snapshot, delete the oldest if more than `max_snapshots` exist.
  - `bassoon restore --from-snapshot [latest|date|run]` hydrates a fresh environment (new container, new VM, local↔cloud migration) — reads Parquet → Arrow → backend append path unchanged.
  - Snapshots are the substrate for `bassoon export` portability too.

- **Schema contracts:** Each tokscale payload kind has a versioned contract — the expected field names, types, and cardinalities, pinned against a tokscale version. The contract lives in `src/usagebassoon/contracts/{models,graph,pricing,report}.json`, generated from golden fixtures and asserted in tests. Deviation produces `schema_drift` rows and a user-facing warning and asks for a bug report:

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
  - `bigquery.py` — `google-cloud-bigquery`; `merge()` via `MERGE` DML on a staging table; Arrow → load job → stage; query → `.to_arrow()`

  Derived columns (`total_tokens`, `session_label`, per-row pricing stamps) are computed **in the normalizer**, before the protocol layer — so dialect differences in computed-column DDL never leak into data. The pipeline is:
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

- **General storage model:** Usage facts use current-state upserts. Existing natural keys are overwritten in place; new natural keys are inserted; rows absent from later snapshots are **never** deleted. Only audit, pricing-history, drift, reconciliation, and snapshot artifacts are append-only. Current-state tables do not retain prior versions of session facts.

### Canonical Ingest Commands

These are the `tokscale` commands used to generate ingest data. Each command is authoritative for their given data domain. Cross-payload reconciliation runs at ingest. Mismatches are surfaced to request bug reporting, never silently resolved:

- `tokscale models --json --group-by client,session,model --merge-worktrees` — authoritative for token/cost metrics.
- `tokscale report --json --no-summarize` — authoritative for session metadata.
- `tokscale graph` — authoritative for daily statistics granularity.
- `tokscale pricing <model-id> --json` — authoritative for current point-in-time rates.

### Shipped views (per-dialect: `sql/{duckdb,bigquery}/views.sql`)

- `daily_cost` — cost and tokens per day
- `cost_by_model` — lifetime cost/tokens per model and client
- `cost_by_workspace` — cost and duration per project
- `cache_efficiency` — cache_read hit ratios per model
- `burn_rate` — trailing 7/30-day daily averages
- `session_leaderboard` — top sessions by cost
  (`lag()` over `collected_at`), no longer depending on detection
- `price_drift` — rate changes per model over time
- `throughput` — ms_per_1k_tokens distributions per model
- `tagged_sessions`, `cost_by_tag`, `tokens_by_tag` — curation views

### Proposed CLI

| Command | Purpose |
|---|---|
| `bassoon init` | create config + per-backend DDL + views |
| `bassoon collect` | one delta-ingest cycle (designed for cron) |
| `bassoon query "<sql>"` | ad-hoc SQL → rich table; `--format csv\|json\|parquet`; `--sanitize` |
| `bassoon report` | terminal summary; `--sanitize` |
| `bassoon report --save out.txt` | render incl. charts to text |
| `bassoon tag` / `note` | user curation |
| `bassoon restore` | recreate normalized state/views from a snapshot and optionally re-collect current tokscale state |
| `bassoon snapshot` / `bassoon restore` | write/read rotating GCS Parquet snapshots |
| `bassoon export` | dump any table/view to parquet/csv; `--sanitize` |
| `bassoon runs` | ingest audit log incl. run_metrics |
| `bassoon doctor` | credentials, connectivity, reconciliation, unresolved schema_drift, with issue link |

### Proposed Python API

`pandas` is the *default*; `polars` is first-class:

```python
import usagebassoon

df  = usagebassoon.query("SELECT * FROM cost_by_model")                   # pandas
df  = usagebassoon.query("SELECT * FROM daily_cost", engine="polars")     # polars
tbl = usagebassoon.query_arrow("SELECT * FROM sessions_current")          # raw Arrow
con = usagebassoon.connect()          # duckdb conn, or ibis-style BigQuery session
```

### Proposed Configuration Schema

`~/.config/usagebassoon/config.toml` (user-global):

```toml
backend = "duckdb"            # duckdb | motherduck | bigquery
database = "usagebassoon"     # duckdb: file path · motherduck: db name · bigquery: dataset

[tokscale]
bin = "bunx tokscale@latest"

[bigquery]                    # only when backend = "bigquery"
project = "my-project"
location = "us-central1"
# credentials: GOOGLE_APPLICATION_CREDENTIALS, or `gcloud auth application-default login`

[snapshots]                   # optional; absent = feature off
gcs_uri = "gs://my-bucket/usagebassoon/snapshots"
max_snapshots = 10            # rotating retention
interval = "12h"              # taken during collect when elapsed
```
