<!--
SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
SPDX-License-Identifier: AGPL-3.0-only
-->

<h1 align="center">UsageBassoon</h1>
<p align="center"><strong>Persistent AI token usage statistics no matter where your agents live</strong></p>

<p align="center">
  <a href="https://github.com/israelflores8789/usagebassoon/actions/workflows/ci.yml"><img src="https://github.com/israelflores8789/usagebassoon/actions/workflows/ci.yml/badge.svg" alt="CI status"></a>
  <a href="https://pypi.org/project/usagebassoon/"><img src="https://img.shields.io/pypi/v/usagebassoon.svg" alt="PyPI"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-%E2%89%A53.12-blue.svg" alt="Python >=3.12"></a>
  <a href="https://github.com/israelflores8789/usagebassoon/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0--only-blue.svg" alt="License - AGPL-3.0-only"></a>
</p>

UsageBassoon turns [tokscale](https://github.com/junhoyeo/tokscale)'s stateless JSON output into durable, queryable token-usage history. It is designed for ephemeral containers, rotating VMs, laptops, and scheduled collection jobs.

It supports local DuckDB, MotherDuck, and BigQuery storage, with optional local or Google Cloud Storage snapshots. Everything is available through the `bassoon` CLI and an importable Python API.

> [!NOTE]
> **Data disclaimer.** UsageBassoon is a personal, local-first telemetry tool. It reads token-usage statistics from the user's local tokscale environment and persists them to a local DuckDB database controlled by the user, the user's own MotherDuck account, or the user's own BigQuery project and optional GCS bucket when configured. UsageBassoon does not transmit telemetry to a UsageBassoon-operated service, aggregate user telemetry, sell telemetry, or provide a cross-user analytics platform.

> [!WARNING]
> UsageBassoon stores raw operational data at rest. Session IDs, workspace and project names, paths, notes, tags, and collector-host metadata may be present in the configured database or snapshots. Treat those locations as private, and review every artifact before sharing it.

## Overview

- Collects daily, per-session, and per-model token statistics, costs, pricing versions, session metadata, and collection-host metadata.
- Merges new and changed facts idempotently without deleting history.
- Provides terminal reports, bounded relation queries, user-managed tags and notes, diagnostics, exports, and portable snapshots.
- Keeps cloud storage optional: a local DuckDB installation needs no cloud account.

## Table of Contents

- [Start](#start)
- [Getting Started](#getting-started)
- [Reports at a glance](#reports-at-a-glance)
- [Config.toml](#configtoml)
- [Automated Scheduling](#automated-scheduling)
- [Source identity and curation](#source-identity-and-curation)
- [Terminal Reports](#terminal-reports)
- [Python API](#python-api)
- [Privacy and sharing](#privacy-and-sharing)
- [Google Cloud permissions](#google-cloud-permissions)
- [Snapshots](#snapshots)
- [License & Disclaimers](#license--disclaimers)

## Start

```bash
bassoon init
```

This creates `~/.config/usagebassoon/config.toml` when absent, generates a stable `source_id`, and initializes a local DuckDB warehouse. Use `--config` to select another configuration file. Existing configuration is never overwritten.

## Getting Started

🚀 UsageBassoon requires Python 3.12 or newer and a working tokscale installation. Install the optional extras when you plan to use BigQuery, GCS snapshots, or Polars:

```bash
pipx install "usagebassoon[full]"
# or, from a checkout:
uv tool install ".[full]"
```

Install tokscale separately using its trusted upstream instructions. UsageBassoon executes the configured tokscale command; it does not install, audit, or verify that executable.

> [!WARNING]
> You are responsible for obtaining tokscale from a trusted source. Verify the exact version with `tokscale --version` and verify a publisher-provided checksum or signature when one is available. Review `[tokscale].bin` or `TOKSCALE_BIN` before running collection, especially when it invokes a package runner such as `bunx`, `npx`, or `deno`.

Initialize the local store, collect one run, and inspect the result:

```bash
bassoon init
bassoon collect
bassoon report summary
bassoon doctor
```

Use `bassoon init` after configuring a remote backend as well; it creates the configured schema and does not overwrite an existing configuration file. Run `bassoon --help` or `bassoon <command> --help` for the complete command reference.

## Reports at a glance

Terminal reports are one of UsageBassoon's main advantages: costs, tokens, cache efficiency, sessions, models, and daily trends are readable directly in a shell. The examples below come from the packaged deterministic fixtures, so `--test` does not need a configured backend.

### Summary

```text
$ bassoon report summary --test
UsageBassoon Summary

 Metric       Value
 ━━━━━━━━━━━━━━━━━━
 Sessions        81
 Cost (USD) $109.48

                  Models

 Model            Total Tokens Cost (USD)
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 gemini-3.7-flash       293.4M     $63.13
 gemini-3.8-flash       270.9M     $42.58
 gpt-5.6-terra            9.3M      $3.60
 gpt-5.6-luna             4.7M      $0.18
```

### Daily usage

```text
$ bassoon report daily --test
                       Daily Token Usage

 Date        Input Output Cache R Cache ×  Total   Cost Cost/1M
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 2026-09-10 391.5K  30.5K    6.2M  15.94×   6.7M  $1.12  $0.168
 2026-09-09 353.9K  72.2K    6.9M  19.48×   7.3M  $2.65  $0.362
 2026-09-08   5.1M 253.8K   29.4M   5.74×  34.7M  $6.99  $0.201
 2026-09-07  14.3M 406.1K   93.5M   6.53× 108.3M $19.27  $0.178
 2026-09-06   9.1M 546.5K  131.2M  14.44× 140.9M $18.71  $0.133
 2026-09-05   2.7M 142.4K   25.8M   9.42×  28.6M  $4.52  $0.158
 2026-09-04   3.1M 226.7K   22.5M   7.26×  25.8M  $4.85  $0.188
 2026-09-03  17.1M 696.2K   63.7M   3.72×  81.5M $20.23  $0.248
 2026-09-02   1.3M  50.7K    6.5M   4.89×   7.9M  $1.69  $0.212
 2026-09-01   2.7M  40.2K   13.1M   4.92×  15.8M  $3.13  $0.198
 2026-08-31   3.2M  98.2K   13.2M   4.14×  16.5M  $3.76  $0.227
 2026-08-30   5.8M 220.0K   29.2M   5.08×  35.2M  $7.33  $0.208
 2026-08-29   7.6M 128.1K   33.8M   4.47×  41.5M  $8.69  $0.209
 2026-08-27   1.3M  44.3K    4.1M   3.22×   5.4M  $1.41  $0.264
 2026-08-26   2.1M  56.5K    7.2M   3.52×   9.3M  $2.29  $0.246
 2026-08-24  56.1K   5.3K   52.6K   0.94× 114.1K  $0.07  $0.579
```

### Session usage

```text
$ bassoon report sessions --test
                                       Session Token Usage

 Session    Client  Model           Input Output Cache R Cache ×  Total  Cost Cost/1M Last Active
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 roll…ad621 codex   gpt-5…-terra    25.1K   2.0K  178.2K   7.10× 205.3K $0.11  $0.535 09-10 03:47
 roll…1be99 codex   gpt-5.6-luna    45.8K   4.5K  229.9K   5.02× 280.2K $0.02  $0.068 09-10 02:55
 roll…960fa codex   gpt-5.6-luna+1 374.8K  28.5K    6.2M  16.54×   6.6M $1.01  $0.154 09-10 00:00
 roll…650cb codex   gpt-5.6-luna+1 299.8K  67.7K    6.5M  21.79×   6.9M $2.63  $0.381 09-09 21:31
 ses_…3cb44 ope…ode ge…3.8-flash     1.8M  82.1K   12.2M   6.61×  14.1M $2.60  $0.185 09-08 03:52
 ses_…5b87f ope…ode ge…3.7-flash+1 806.4K  44.7K    4.5M   5.54×   5.3M $1.11  $0.208 09-08 03:21
 ses_…49afc ope…ode ge…3.7-flash+1   2.7M 169.4K   15.4M   5.74×  18.3M $3.81  $0.208 09-08 02:42
 ses_…9e714 ope…ode ge…3.7-flash+1   2.4M  48.8K    8.7M   3.60×  11.2M $2.65  $0.237 09-07 22:59
 ses_…f2c46 ope…ode ge…3.7-flash     3.4M 130.3K   27.7M   8.19×  31.2M $5.11  $0.163 09-07 05:24
 ses_…5a4d4 ope…ode ge…3.7-flash     1.8M  14.5K   14.2M   8.06×  16.0M $2.44  $0.153 09-07 04:12
 ses_…0fed7 ope…ode ge…3.7-flash     4.7M 128.9K   23.2M   4.93×  28.0M $5.75  $0.205 09-07 03:45
 ses_…aeea9 ope…ode ge…3.8-flash    63.3K   1.4K  170.2K   2.69× 234.9K $0.07  $0.279 09-07 01:23
 ses_…2cbea ope…ode ge…3.8-flash     3.0M 120.0K   34.9M  11.64×  38.0M $5.31  $0.140 09-07 00:44
 ses_…7c07c ope…ode ge…3.8-flash     2.9M 172.0K   55.7M  19.43×  58.7M $6.97  $0.119 09-06 22:56
 ses_…c7cee ope…ode ge…3.8-flash     1.5M  60.4K   16.8M  11.40×  18.4M $2.60  $0.141 09-06 05:10
 ses_…1232c ope…ode ge…3.8-flash     1.6M  67.6K   13.9M   8.83×  15.5M $2.47  $0.159 09-06 03:59
```

### Cost graph

```text
$ bassoon report graph --test
                       Cost (USD): 2026-09-01 to 2026-09-10
      ┌────────────────────────────────────────────────────────────────────────┐
$20.23┤               ███████                                                  │
      │               ███████              ██████████████                      │
      │               ███████              ██████████████                      │
$15.17┤               ███████              ██████████████                      │
      │               ███████              ██████████████                      │
      │               ███████              ██████████████                      │
      │               ███████              ██████████████                      │
$10.12┤               ███████              ██████████████                      │
      │               ███████              ██████████████                      │
      │               ███████              █████████████████████               │
 $5.06┤               ██████████████████████████████████████████               │
      │ ██████        ██████████████████████████████████████████ ██████        │
      │ ██████ ██████ ██████████████████████████████████████████ ██████ ██████ │
 $0.00┤ ██████ ██████ ██████████████████████████████████████████ ██████ ██████ │
      └────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┬────┘
           01     02     03     04     05     06     07     08     09     10
```

## Config.toml

> [!NOTE]
> UsageBassoon reads `~/.config/usagebassoon/config.toml` by default. Override by setting the `USAGEBASSOON_CONFIG` environment variable.

```toml
# If you use `Even Better TOML` in VSCode, or similar, you can include the schema for linting in your IDE.
#:schema https://raw.githubusercontent.com/israelflores8789/usagebassoon/main/config.schema.json

source_id = "018f2d70-0000-4000-8000-000000000000"  # Required; typically generated with `bassoon init`. Set manually
                                                    # for identical environments (e.g. respawning a crashed container).
backend = "duckdb"                                  # Required; one of `duckdb`, `motherduck`, or `bigquery`.
database = "usagebassoon.duckdb"                    # Required; DuckDB file path, MotherDuck database name, or
                                                    # BigQuery dataset ID.

[tokscale]                                          # Optional; `bin` overrides `TOKSCALE_BIN` when set.
bin = "bunx tokscale@latest"                        # Command prefix with runner arguments.
                                                    # Examples: `npx tokscale@latest`,
                                                    #           `bunx tokscale@latest`, or
                                                    #           `deno x npm:tokscale@latest`.
env = ["YOUR_ENV_VAR"]                              # Optional; additional env-vars for the tokscale subprocess.
timeout_seconds = 300                               # Optional; max duration of one tokscale call.
max_stdout_bytes = 67108864                         # Optional; max stdout captured from tokscale for one command.
max_stderr_bytes = 8388608                          # Optional; this and the above prevent memory-leaks and abuse.

[bigquery]                                          # Required when backend is `bigquery`.
project = "my-gcp-project"                          # Required; Google Cloud project ID.
location = "US"                                     # Optional dataset and job location; defaults to `US`.
credentials_file = "path/to/gcp-sa-secret.json"     # Optional; default uses Application Default Credentials.
maximum_bytes_billed = 1073741824                   # Optional; per-job maximum bytes billed for BigQuery queries.

[gcs]                                               # Optional; configure GCS snapshots independently of the data backend.
uri = "gs://my-private-bucket/usagebassoon"         # Required if `gcs` is present; private Google Cloud Storage URI.
project = "my-gcp-project"                          # Required; Google Cloud project ID.
location = "US"                                     # Optional configured bucket location; defaults to `US`.
credentials_file = "path/to/gcp-sa-secret.json"     # Optional; default uses Application Default Credentials.

[schedule]                                          # Optional automated collection scheduling.
interval = "15m"                                    # Minutes or hours between collection cycles (`30m`, `2h`).

[collection]                                        # Optional collection deadline & retry settings.
max_retries = 3                                     # Additional attempts after the first persistence failure.
retry_initial_seconds = 1.0                         # Positive initial delay for exponential backoff.
timeout = "5m"                                      # Optional end-to-end collection deadline (`30m`, `2h`).

[logging]                                           # Optional rotating operational log settings.
directory = "~/.local/state/usagebassoon/logs"      # Log files directory.
max_files = 5                                       # Retained files, including the active file.
max_bytes = 10485760                                # Max size of the active log file before rotation.

[snapshots]                                         # Optional retention and automatic snapshot settings.
file_uri = "file:///var/lib/usagebassoon/snapshots" # Optional local path or `file://` archive;
                                                    # with `gcs.uri`, snapshots go both locally and to GCS.
max_snapshots = 3                                   # Max number of snapshots to retain.
interval = "12h"                                    # Optional positive cadence (`30m`, `12h`, `7d`);
                                                    # enables due-only snapshots after collection when set.
```

The `bigquery` table is only required for the BigQuery backend. BigQuery is *not* required to persist snapshots to Google Cloud Storage, and GCS is *not* required to use BigQuery.

If neither `gcs.uri` nor `snapshots.file_uri` is configured, snapshots use the conventional local archive at `~/.usagebassoon/snapshots/`. If `gcs.uri` and `snapshots.file_uri` are both present, snapshots will archive both locally *and* to GCS.

## Automated Scheduling

`bassoon collect` performs one collection cycle. The `schedule` commands manage repeated collection without changing that boundary.

On Linux, install a systemd user timer; on macOS, install a launchd agent:

```bash
bassoon schedule install --interval 15m
bassoon schedule status --json
bassoon schedule stop
bassoon schedule remove
```

`--interval` persists `[schedule].interval` in `config.toml`. The value must use minutes or hours; `1d` and `30s` are rejected. `bassoon schedule install` and `bassoon schedule worker` both run a tokscale preflight before scheduling.

For interactive use, `bassoon schedule worker --interval 15m` starts a detached self-contained worker and reports its PID and log path. Use `bassoon schedule status`, `bassoon schedule logs`, and `bassoon schedule stop` to manage it.

For containers, run the worker in the foreground so it remains the container's main process:

```bash
bassoon schedule worker --foreground --interval 15m
```

The worker invokes `bassoon collect` at the configured interval and does not call Docker or Podman or control the host. Container deployments should consider MotherDuck or BigQuery for persistence and GCS for snapshots, but these backends are not enforced.

## Source identity and curation

`source_id` separates data collected from different environments, even when their client, workspace, or session names match. Reuse a source ID only when those environments intentionally share a collection namespace.

Tags can target a workspace, client, or one session; notes belong to one session.

```bash
bassoon tag project-alpha --workspace /work/repo
bassoon tag production --client codex
bassoon tag important --client codex --session ses_123
bassoon note "Investigate cache miss" --client codex --session ses_123
```

## Terminal Reports

Reports are terminal-first and personal by default:

- `bassoon report daily` shows the newest daily usage and cost rows (16 by default).
- `bassoon report sessions` shows the newest sessions; add `--by-model` for session/model detail.
- `bassoon report graph` renders up to 31 days of daily bars, showing USD cost by default; select token metrics with `--metric`.
- `bassoon report summary` shows the configured summary report.

Token values use one-decimal `K`, `M`, and `T` units; daily USD values use three decimal places and session USD values use cents. All report commands support combined `--client`, `--model`, `--workspace`, `--tag`, and `--source` filters; use `--source local` for the configured source only. `--width 100` is the default bounded layout, `--width max` disables truncation, and `--test` renders deterministic output from the packaged sanitized golden fixtures without reading a backend. Reports are raw by default; use `--sanitize` before sharing or `--save PATH` to write a text artifact.

## Python API

The same configured backend is available from Python. Results are pandas DataFrames by default; request Polars explicitly or use Arrow when you need the raw table:

```python
import usagebassoon

daily = usagebassoon.query("SELECT * FROM daily_cost")
polars_daily = usagebassoon.query("SELECT * FROM daily_cost", engine="polars")
arrow_daily = usagebassoon.query_arrow("SELECT * FROM daily_cost")
```

## Privacy and sharing

> [!WARNING]
> 🔒 Obfuscation reduces exposure; it is not a guarantee that an artifact is safe for every audience. Review output for sensitive values before uploading it anywhere.

The commands have deliberately different sharing behavior:

| Command | Default output | Sharing guidance |
| --- | --- | --- |
| `bassoon report ...` | Raw personal report | Add `--sanitize` before sharing. |
| `bassoon export ...` / `usagebassoon export ...` | Obfuscated export; notes are redacted | Safe defaults still require review. Add `--raw` only for an intentional private backup or data-management export. `--obfuscate` is an alias for the default behavior. |
| `bassoon doctor` | Sanitized diagnostic paths and credentials | Prefer this for issue reports, but review it: diagnostics may expose host metadata such as OS, OS version, architecture, CPU, memory, and shell. Add `--raw` only for private troubleshooting. |
| `bassoon query ...` | Raw relation data | It always warns on stderr and may contain session IDs, workspaces, tags, notes, paths, and host metadata. Do not share it publicly. |
| `bassoon snapshot` | Raw restoration archive | Keep local and GCS snapshots private; they are not shareable exports. |

`bassoon export` pseudonymizes fields such as session IDs, workspaces, tags, and host identifiers consistently within one output, and redacts notes, embedded filesystem paths, and common credential forms. Collector system metadata remains raw by design. No command can infer the sensitivity of your downstream environment, so inspect sanitized output before sharing it.

## Google Cloud permissions

Use a dedicated service account or user identity scoped to your own project, dataset, and bucket. Avoid broad project-owner permissions. The permissions below describe the operations performed by UsageBassoon; grant only the subset required by the backend and commands you use.

For GCS snapshots, grant these permissions at the archive bucket (or a narrower custom-role scope):

- `storage.buckets.get` — bucket metadata and lifecycle checks used by diagnostics.
- `storage.objects.get` — read manifests, Parquet tables, and object metadata.
- `storage.objects.list` — discover catalog and retained snapshots.
- `storage.objects.create` — publish snapshot tables, manifests, and catalogs.
- `storage.objects.delete` — rotate old snapshots and clean up staged objects.

A restore-only identity needs only the bucket metadata and object read/list permissions. A writer/retention identity also needs create and delete. `roles/storage.objectAdmin` is the usual predefined role for object operations; add a narrowly scoped bucket-metadata permission if `storage.buckets.get` is not otherwise granted. Prefer a custom role when you need tighter control. Use the `STANDARD` storage class for an active snapshot archive: UsageBassoon writes, lists, restores, and rotates snapshots, so colder archival classes are a poor default. See Google's [Cloud Storage IAM permissions](https://docs.cloud.google.com/iam/docs/roles-permissions/storage) and [storage classes](https://docs.cloud.google.com/storage/docs/storage-classes).

For the BigQuery backend, the identity generally needs:

- Project-level `bigquery.jobs.create` to run schema, query, load, and merge jobs.
- Project-level `bigquery.datasets.create` only when `bassoon init` should create the dataset.
- Dataset-level `bigquery.datasets.get` to validate the configured dataset and location.
- Dataset-level `bigquery.tables.create`, `bigquery.tables.get`, `bigquery.tables.getData`, `bigquery.tables.updateData`, and `bigquery.tables.delete` to initialize the schema, read data, merge facts, and remove temporary staging tables.
- BigQuery Storage Read API permissions `bigquery.readsessions.create`, `bigquery.readsessions.getData`, and `bigquery.readsessions.update` to return query results as Arrow.

The common predefined-role arrangement is `roles/bigquery.jobUser` on the project and `roles/bigquery.dataEditor` on the dataset, with dataset-creation and Storage Read API permissions added only when required by your organization. Verify the effective permissions in your project because predefined roles can change. See Google's [BigQuery IAM documentation](https://docs.cloud.google.com/iam/docs/roles-permissions/bigquery) and [dataset access controls](https://docs.cloud.google.com/bigquery/docs/access-control).

## Snapshots

`bassoon snapshot` writes a catalog-published Parquet restoration archive and `bassoon restore --from-snapshot latest` restores only complete published snapshots into an initialized empty warehouse. Local archives rotate under `~/.usagebassoon/snapshots/` by default. Configure `[snapshots] max_snapshots = 3` and an optional positive interval such as `12h`; an interval also enables due-only automatic snapshots after collection. Set `gcs.uri = "gs://bucket/private/usagebassoon-snapshots"` to use Google Cloud Storage (install `usagebassoon[gcs]`); set both `gcs.uri` and `snapshots.file_uri` to publish the same complete snapshot to both destinations. Snapshot object names are confined to the selected archive and SHA-256 is verified before manifest or Parquet data is parsed. GCS archives use generation-conditional catalog publication and all snapshot archives contain raw private data. SHA-256 detects corruption or accidental replacement but does not authenticate an actor able to rewrite both catalog and objects; use restrictive local permissions and least-privilege GCS IAM so writers can publish and retain snapshots while restore-only identities can read without modifying the archive.

## License & Disclaimers

UsageBassoon is copyright © 2026 Israel Flores-Arbolay and licensed under the GNU Affero General Public License v3.0 (AGPL-3.0-only). See LICENSE for the full text.
