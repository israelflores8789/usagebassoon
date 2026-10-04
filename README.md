<!--
SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
SPDX-License-Identifier: AGPL-3.0-only
-->

<h1 align="center">UsageBassoon</h1>

<p align="center">
  <img src="media/usagebassoon_readme_banner-800px.png" alt="Banner" width=400 />
</p>

<p align="center"><strong>Persistent AI token usage statistics no matter where your agents live</strong></p>

<p align="center">
  <a href="https://github.com/israelflores8789/usagebassoon/actions/workflows/ci.yml"><img src="https://github.com/israelflores8789/usagebassoon/actions/workflows/ci.yml/badge.svg" alt="CI status"></a>
  <a href="https://pypi.org/project/usagebassoon/"><img src="https://img.shields.io/pypi/v/usagebassoon.svg" alt="PyPI"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-%E2%89%A53.12-blue.svg" alt="Python >=3.12"></a>
  <a href="https://github.com/israelflores8789/usagebassoon/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0--only-blue.svg" alt="License - AGPL-3.0-only"></a>
</p>

UsageBassoon turns [`tokscale`](https://github.com/junhoyeo/tokscale)'s stateless JSON output into durable, queryable token-usage history. It is designed for ephemeral containers, rotating VMs, laptops, anywhere you want to track and save your token usage history. It can be run as a scheduled job in macOS, Linux, and container environments.

UsageBassoon supports local DuckDB, MotherDuck, and BigQuery storage, with optional local or Google Cloud Storage snapshots. We recommend BigQuery for persisting token data across environments due to GCP's generous free-tier and easy integration with Google Colab.

Get started with `bassoon --help` or import the Python API with `import usagebassoon`.

## Overview

- Collects daily, per-session, and per-model token statistics, costs, pricing versions, session metadata, and collection-host metadata.
- Publishes observations idempotently: retries and overlapping collections for the same source do not double-count usage. Separate sources retain separate histories.
- Provides terminal reports, bounded relation queries, user-managed tags and notes, diagnostics, exports, and portable snapshots.
- Keeps cloud storage optional: a local DuckDB installation needs no cloud account.

## Table of Contents

- [Getting Started](#getting-started)
- [Terminal Reports](#terminal-reports)
- [Config.toml](#configtoml)
- [Choosing a Backend](#choosing-a-backend)
- [How Persistence Works](#how-persistence-works)
- [Automated Scheduling](#automated-scheduling)
- [Tags and Notes](#tags-and-notes)
- [Python API](#python-api)
- [Privacy and sharing](#privacy-and-sharing)
- [Snapshots](#snapshots)
- [MotherDuck setup and permissions](#motherduck-setup-and-permissions)
- [BigQuery setup and permissions](#bigquery-setup-and-permissions)
- [Google Cloud Storage setup and permissions](#google-cloud-storage-setup-and-permissions)
- [Why AGPL?](#why-agpl)
- [License & Disclaimers](#license--disclaimers)

## Getting Started

🚀 UsageBassoon requires Python 3.12 or newer and a working [`tokscale`](https://github.com/junhoyeo/tokscale) installation.

```bash
pipx install "usagebassoon"            # minimal install

pipx install "usagebassoon[bigquery]"  # use with BigQuery
pipx install "usagebassoon[gcs]"       # use with Google Cloud Storage
pipx install "usagebassoon[polars]"    # use polars dataframes
pipx install "usagebassoon[full]"      # full installation

uv tool install ".[full]"              # install from a checkout
```

Then, initialize UsageBassoon.

```bash
bassoon init
```

This creates a configuration file at the [platform-specific default path](#configtoml) when absent, generates a stable `source_id` that is unique to your environment, and initializes a local DuckDB storage backend by default. Init is safe to repeat. Ordinary commands require an initialized backend and perform a schema preflight. Newer or incompatible schemas fail with an explicit error.

> [!TIP]
> Use `bassoon init` after for setting up new environments with an existing `config.toml` as well, especially if using a remote backend. It performs important setup including creating the configured schema, setting `source_id`, and does *not* overwrite your existing configuration file.

Try it out!

```bash
bassoon collect          # your first token usage collection
bassoon report summary   # see the results!
bassoon doctor           # troubleshooting
```

Run `bassoon --help` or `bassoon <command> --help` for the complete command reference.

### Don't forget Tokscale!

```bash
# UsageBassoon uses bun by default
bunx tokscale@4.15.1 --version
```

> [!IMPORTANT]
> Version 4.15.1 is officially supported. Verify the exact version with `tokscale --version` and verify your installation against [`tokscale`'s](https://github.com/junhoyeo/tokscale/releases) official checksums.

> [!NOTE]
> You can set `[tokscale].bin` in `config.toml` or the `TOKSCALE_BIN` environment variable to choose the command; the configuration value takes precedence. With neither set, UsageBassoon uses a local `tokscale` executable when available, otherwise `bunx tokscale@latest`. Pin the supported release with `bin = "bunx tokscale@4.15.1"`.

### Source identity

`source_id` is a unique identifier (UUID) generated when `bassoon init` creates `config.toml` and separates data collected from different environments, even when their client, workspace, or session names match.

> [!TIP]
> If you set `source_id` manually, you can reuse it for ephemeral environments that you want to namespace token usage. For example, if you have a container that should be considered the same as a *unique* previous container build for token statistics purposes.

## Terminal Reports

- `bassoon report summary` shows summary statistics.
- `bassoon report models` shows token, cost, and timing statistics by model and client.
- `bassoon report daily` shows the newest daily usage and cost rows (16 by default).
- `bassoon report sessions` shows the newest sessions.
- `bassoon report graph` renders up to 31 days of daily bars.

All report commands support combined `--client`, `--model`, `--workspace`, `--tag`, and `--source` filters; use `--source local` for the configured source only.

All report commands support inclusive `--since YYYY-MM-DD` and `--until YYYY-MM-DD` bounds on usage days and can also be combined with all other filters. `report sessions`, including with `--by-model`, filters by the last active timestamp or `--sort created-at`.

`--width 100` is the default bounded layout, but `--width max` disables truncation.

Daily, models, and sessions reports also support mutually exclusive `--json` and `--csv` flags. These output the same filtered and grouped results as an array of JSON objects or a CSV table,

> [!IMPORTANT]
> Reports are raw by default. Use `--sanitize` before sharing or `--save <path>` to write a text artifact.

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

### Models

```text
$ bassoon report models --test
                                  Model Token Usage

 Model            Client    Input Output Cache R Cache ×  Total  ms/1K Cost/1M   Cost
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 gemini-3.7-flash opencode  52.4M   1.6M  239.5M   4.57× 293.4M    110  $0.215 $63.13
 gemini-3.8-flash opencode  25.2M   1.4M  244.3M   9.70× 270.9M     60  $0.157 $42.58
 gpt-5.6-terra    codex    436.8K  80.6K    8.8M  20.12×   9.3M    304  $0.387  $3.60
 gpt-5.6-luna     codex    308.7K  22.1K    4.4M  14.09×   4.7M    105  $0.037  $0.18
```

`ms/1K` is calculated from the total measured milliseconds and timed tokens for each model and client. `Cache ×` is cache-read tokens divided by input tokens. Output includes reasoning tokens.

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
$ bassoon report sessions --test    # add --by-model for per-model detail
                                    # add --with-duration to see session execution time
                                    # add --with-performance to see reasoning latency
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
$ bassoon report graph --test       # shows USD cost by default. use `--metric` for token metrics.
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

### Daily activity

> [!TIP]
> The heatmap supports truecolor and 256-color terminals. If neither are detected, it falls back to ASCII shading characters. You can toggle ASCII shading characters yourself with `--use-ascii`. If the color is contradictory to your theme, use `--color` and pass an ANSI color name to change the heatmap color. Note, you may need to set `COLORTERM=truecolor` in your shell!

```text
$ bassoon report activity --test --use-ascii
                      Daily Activity

    total (tokens) | 2026-05-14 to 2026-09-10 | linear

    May 2026    Jun         Jul         Aug            Sep
Sun                                              ░░ ▒▒ ██
Mon                                              ░░ ░░ ▒█
Tue                                                 ░░ ▒▒
Wed                                              ░░ ░░ ░░
Thu                                              ░░ ▓▓ ░░
Fri                                                 ▒▒
Sat                                           ░░ ▒▒ ▒▒

Less    ░░ ▒▒ ░█ ▓▓ ▒█ ██ More
Intensity is relative to this selection. Unfilled squares show zero recorded usage.
```

## Config.toml

> [!NOTE]
> UsageBassoon reads by default:
> - Linux: `~/.config/usagebassoon/config.toml`
> - macOS: `~/Library/Application Support/UsageBassoon/config.toml`
> - Windows: `%LOCALAPPDATA%\UsageBassoon\config.toml`
>
> Override by setting the `USAGEBASSOON_CONFIG` environment variable.

```toml
# If you use `Even Better TOML` in VSCode, or similar, you can include the schema for linting in your IDE.
#:schema https://raw.githubusercontent.com/israelflores8789/usagebassoon/main/config.schema.json

# Defaults are shown unless otherwise stated.

source_id = "018f2d70-0000-4000-8000-000000000000"  # Required; typically generated with `bassoon init`. Set manually
                                                    # for identical environments (e.g. respawning a crashed container).
backend = "duckdb"                                  # Required; one of `duckdb`, `motherduck`, or `bigquery`.
spinner = "pong"                                    # Optional; yaspin animation name, e.g. `pong`, `dots`, or `line`.
local_database = "path/to/your/database.duckdb"     # Optional; local DuckDB file path.
                                                    # Default: Linux: ~/.local/share/usagebassoon/usagebassoon.duckdb
                                                    #          macOS: ~/Library/Application Support/UsageBassoon/usagebassoon.duckdb
                                                    #          Windows: %LOCALAPPDATA%\UsageBassoon\usagebassoon.duckdb

[tokscale]                                          # Optional; `bin` overrides `TOKSCALE_BIN` when set.
bin = "bunx tokscale@latest"                        # Command prefix with runner arguments.
                                                    # Examples: `npx tokscale@latest`,
                                                    #           `bunx tokscale@latest`, or
                                                    #           `deno x npm:tokscale@latest`.
env = ["YOUR_ENV_VAR"]                              # Optional; additional env-vars for the tokscale subprocess.
timeout = "180s"                                    # Optional; max duration of one tokscale subprocess call.
max_stdout_bytes = 67108864                         # Optional; max stdout captured from tokscale for one command.
max_stderr_bytes = 8388608                          # Optional; this and the above prevent memory-leaks and abuse.

[bigquery]                                          # Required when backend is `bigquery`.
project = "my-gcp-project"                          # Required; Google Cloud project ID.
dataset = "usagebassoon"                            # Required; BigQuery dataset ID in the project above.
location = "US"                                     # Optional dataset and job location; defaults to `US`.
credentials_file = "path/to/gcp-sa-secret.json"     # Optional; default uses Application Default Credentials.
maximum_bytes_billed = 1073741824                   # Optional; per-job maximum bytes billed for BigQuery queries.
timeout = "120s"                                    # Optional; max wait for one BigQuery job or Storage Read request.

[motherduck]                                        # Required when backend is `motherduck`.
database = "usagebassoon"                           # Required; MotherDuck database name without the `md:` prefix.
                                                    # Don't forget to set your MOTHERDUCK_TOKEN environment variable!

[gcs]                                               # Optional; configure GCS snapshots independently of the data backend.
uri = "gs://my-private-bucket/usagebassoon"         # Required if `gcs` is present; private Google Cloud Storage URI.
project = "my-gcp-project"                          # Required; Google Cloud project ID.
credentials_file = "path/to/gcp-sa-secret.json"     # Optional; default uses Application Default Credentials.
timeout = "60s"                                     # Optional; max wait for one GCS request, not the whole snapshot.

[schedule]                                          # Optional automated collection scheduling.
interval = "15m"                                    # Minutes or hours between collection cycles; must exceed tokscale.timeout.

[collection]                                        # Optional persistence retry settings.
max_retries = 3                                     # Additional attempts after the first persistence failure.
retry_initial_seconds = 1.0                         # Positive initial delay for exponential backoff.

[logging]                                           # Optional rotating operational log settings.
max_files = 5                                       # Retained files, including the active file.
max_bytes = 5242880                                 # Max size of the active log file before rotation (5 MiB).
directory = "path/to/your/logs/"                    # Log files directory.
                                                    # Default: Linux: ~/.local/state/usagebassoon/logs/
                                                    #          macOS: ~/Library/Logs/UsageBassoon/
                                                    #          Windows: %LOCALAPPDATA%\UsageBassoon\Logs\

[snapshots]                                         # Optional retention and automatic snapshot settings.
max_snapshots = 3                                   # Max number of snapshots to retain.
interval = "12h"                                    # Optional positive cadence (m, h, d).
file_uri = "file:///path/to/your/snapshots/"        # Optional local path or `file://` archive.
                                                    # with `gcs.uri`, snapshots go both locally and to GCS.
                                                    # Default: Linux: ~/.local/share/usagebassoon/snapshots/
                                                    #          macOS: ~/Library/Application Support/UsageBassoon/snapshots/
                                                    #          Windows: %LOCALAPPDATA%\UsageBassoon\snapshots\
```

> [!TIP]
> On Linux, default path locations follow `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, and `XDG_STATE_HOME` when those variables are set. UsageBassoon also supports XDG overrides on macOS.

BigQuery is *not* required to persist snapshots to Google Cloud Storage, and GCS is *not* required to use BigQuery.

If neither `gcs.uri` nor `snapshots.file_uri` is configured, snapshots use the platform-specific local archive documented above. If only `gcs.uri` is configured, snapshots go to GCS; if both are configured, snapshots go to both destinations. On Linux, the default archive follows `XDG_DATA_HOME` when set; the default is `~/.local/share/usagebassoon/snapshots/`.

## Choosing a Backend

| | Local DuckDB | MotherDuck | BigQuery |
|:--|:--|:--|:--|
| Setup | `bassoon init`; no cloud account | MotherDuck account and `MOTHERDUCK_TOKEN` | Google Cloud project, authentication, APIs, and IAM grants |
| Best for | One local environment | Shared history across environments | Shared history on Google Cloud |
| Concurrent collectors | One per local user environment; a second is rejected | Transaction conflicts retried with bounded backoff | Independent appends; duplicates deduplicated by canonical views |
| Ongoing operations | Local database storage | Account and backend access | Nightly compaction must remain operational |

Snapshot destinations are independent of your storage backend: any backend can archive to local disk, Google Cloud Storage, or both. Complete snapshots can restore into any initialized, empty backend with all destination writers stopped.

## How Persistence Works

**DuckDB and MotherDuck** use direct transactional upserts. Each normalized collection batch applies its changes in one transaction, and current state follows the shared observation ordering. Local DuckDB holds an OS lock for the user environment during collection; retryable MotherDuck transaction conflicts use bounded backoff.

**BigQuery** uses append-and-compact persistence. Collection and curation append observations, and canonical views combine durable gold tables with retained raw observations, deduplicating by logical identity. Accepted loads are queryable without waiting for compaction. Independent remote collectors, including ones sharing a `source_id`, can publish concurrently. Publication across tables can be partial; bounded retries reuse the same observation identities, and collection completion is recorded after fact and diagnostic loads succeed.

- `bassoon init` installs a Scheduled Query at 02:00 UTC and can be rerun to create a missing schedule or update its query and cadence. Verify that the schedule remains enabled and its runs succeed in BigQuery.
- Raw BigQuery observations use arrival-day partitions with 90-day expiration, including historical backfills. Compaction preserves existing gold plus retained raw data; durable gold never expires. `bassoon doctor` warns about overdue compaction buckets.
- Collection audit history (`bassoon audit`) is retained permanently. Doctor shows unresolved schema-drift and reconciliation diagnostics from the last 90 days; BigQuery expires raw diagnostics, while DuckDB and MotherDuck prune them opportunistically.
- Restore requires an initialized, empty destination and all destination writers stopped until completion. BigQuery append loads cannot be excluded by the restore transaction.

## Automated Scheduling

`bassoon collect` performs one collection cycle. The `schedule` commands manage repeated collection on macOS (launchd), Linux (systemd), and container environments (worker script). Windows Task Scheduler integration is not supported at this time.

```bash
bassoon schedule install --interval 15m
bassoon schedule status
bassoon schedule stop
bassoon schedule remove
```

`--interval` persists to `[schedule].interval` in `config.toml`. The value must use minutes or hours.

#### Interactive Use
```bash
bassoon schedule worker --interval 15m
```

Starts a detached self-contained worker and reports its PID and log path.  Use `bassoon schedule status`, `bassoon schedule logs`, and `bassoon schedule stop` to manage it.

Workers keep the configuration loaded at startup for their entire lifetime, including `source_id`, storage backend, logging, and collection interval. *Configuration files are __never__ hot-swapped*. For a detached worker, run `bassoon schedule stop` before editing the file, then run `bassoon schedule worker` to apply the new settings. Restart a foreground worker through its container or process supervisor.

Native schedules launch a new `bassoon collect` process for each cycle and load the file for that invocation; rerun `bassoon schedule install` to update a native schedule's interval.

#### Container Environments
Run the worker in the foreground so it remains the container's main process:

```bash
bassoon schedule worker --foreground --interval 15m
```

> [!TIP]
> When using UsageBassoon in a container environment, consider a remote data warehouse like MotherDuck or BigQuery and remote object storage like GCS if you want snapshot archives.

## Tags and Notes

You can group token usage statistics together by `tag`ging all agentic sessions in a project workspace directory, all sessions for an agentic client (e.g. Codex), or for individual sessions, and you can generate reports across environments based on your tags!

Tags apply globally to matching client, workspace, and session targets across sources; their `source_id` records the source of the latest mutation. Notes belong to one exact session identified by `(source_id, client, session_id)`; note commands use the source configured in `config.toml`. Renames and note edits preserve creation time; deleting and later re-adding starts a new lifetime. The update time records the latest meaningful change, and unchanged add/set calls are no-ops.

```bash
bassoon tag add project-alpha --workspace /work/repo
bassoon tag add production --client codex
bassoon tag add important --client codex --session ses_123
```

Use `bassoon tag add`, `bassoon tag rename`, and `bassoon tag remove` to manage tags, and `bassoon note set`, `bassoon note edit`, and `bassoon note remove` for notes. `note edit` opens an existing note in `VISUAL` or `EDITOR`. Run any command with `--help` for its full arguments.

You can use `note` to annotate individual agentic sessions to remember things like why token usage was so high, key things about a session important to a project, or add debugging notes, etc!

```bash
bassoon note set "Investigate cache miss" --client codex --session ses_123
```

## Python API

The same configured storage backend is available from Python. For library use, install UsageBassoon in your Python environment with `pip install usagebassoon`; `pipx` installs the CLI in an isolated environment. Results are `pandas` DataFrames by default. Install the optional Polars extra with `pip install "usagebassoon[polars]"`, or use Arrow when you need the raw table:

```python
import usagebassoon

daily = usagebassoon.query("SELECT * FROM daily_cost LIMIT 1000")
polars_daily = usagebassoon.query("SELECT * FROM daily_cost LIMIT 1000", engine="polars")
arrow_daily = usagebassoon.query_arrow("SELECT * FROM daily_cost LIMIT 1000")
```

## Privacy and sharing

> [!WARNING]
> UsageBassoon stores raw operational data at rest. Session IDs, workspace and project names, paths, notes, tags, and collector-host metadata may be present in the configured database or snapshots. You should always treat your data as private, and **always** review every UsageBassoon artifact before sharing it.

Your token usage data can contain private information including session IDs, workspace paths, cost information, etc, and UsageBassoon takes that seriously. Some commands are obfuscated by default while others offer a `--sanitize` flag. **Always** use `bassoon doctor` when submitting a bug report, and **always** sanitize your token usage data before sharing it publicly!

> [!IMPORTANT]
> 🔒 Obfuscation reduces exposure, but it is *not* a guarantee that an artifact is safe for every audience. Review any `bassoon` output for sensitive values before uploading it anywhere.

The commands have deliberately different sharing behavior:

| Command            | Default output                             | Sharing guidance                                             |
|--------------------|--------------------------------------------|--------------------------------------------------------------|
| `bassoon doctor`   | Sanitized diagnostic paths and credentials | Prefer this for issue reports, but *review* it: diagnostics may expose host metadata such as OS, OS version, architecture, CPU, memory, and shell. Add `--raw` only for private troubleshooting. |
| `bassoon report`   | Raw personal report                        | Add `--sanitize` before sharing.                             |
| `bassoon export`   | Obfuscated export; notes are redacted      | Safe defaults still require review. Add `--raw` only for an intentional private backup or data-management export. |
| `bassoon query`    | Raw relation data                          | It always warns on stderr and may contain session IDs, workspaces, tags, notes, paths, and host metadata. Do not share it publicly. |
| `bassoon snapshot` | Raw restoration archive                    | Keep local and GCS snapshots private; they are not shareable exports. |

`bassoon export` pseudonymizes fields such as session IDs, workspaces, tags, and host identifiers consistently within one output, and redacts notes, embedded filesystem paths, and common credential forms. System metadata remains raw in all cases. No command can infer the sensitivity of your downstream environment, so inspect sanitized output before sharing it.

## Snapshots

`bassoon snapshot` creates a private, whole-backend Parquet archive and pins it by default. It preserves all sources, including uncompacted BigQuery observations. A manual request bypasses automatic cadence; use `--no-pin` to make it eligible for rotation. Pins do not change immutable manifests or data hashes.

```bash
bassoon snapshot
bassoon snapshot list                     # YAML, newest capture first; --json available
bassoon snapshot inspect --from-snapshot SNAPSHOT_ID
bassoon snapshot audit --from-snapshot SNAPSHOT_ID
bassoon snapshot pin SNAPSHOT_ID
bassoon snapshot copy /new/archive --from-snapshot SNAPSHOT_ID
bassoon snapshot delete SNAPSHOT_ID        # displays all selected copies; type DELETE
```

`bassoon audit snapshot` and `bassoon audit snapshots` are aliases for `bassoon snapshot audit`. Integrity auditing downloads and checks every table without opening a destination backend. `inspect` shows producer version, data-schema version, archive-format version, backend schema version/hash, capture time, and cadence. `inspect` and listing expose verified provenance even when this release cannot restore a newer format or data contract, and separately report compatibility. Listing is discovery, not a full integrity audit. Snapshot commands emit structured YAML or JSON on stdout; notices and warnings go to stderr.

Local manual snapshots use the platform data directory by default. Set `snapshots.file_uri` or `gcs.uri` to choose archive destinations; GCS requires `usagebassoon[gcs]`. Both can be configured for redundant publication. `snapshots.interval` enables automatic user-cadence captures, and `snapshots.max_snapshots` limits unpinned scheduled/manual recovery points (default three). Explicitly configured destinations also capture the current UTC calendar-week slot and retain four successful weekly slots. Set `[snapshots].disable_weekly_snapshots = true` or `[gcs].disable_weekly_snapshots = true` to opt out for that destination. Pins are exempt from both retention classes and consume storage until explicitly deleted. Manual captures do not advance automatic cadence clocks.

Snapshot cadence is independent of collection. `bassoon schedule install` creates separate systemd/launchd snapshot jobs, and the container worker services snapshots separately even while collection is blocked or fails. For cron, invoke `bassoon snapshot --automatic` independently of `bassoon collect`; automatic invocations capture only due obligations and never pin. Weekly obligations are checked at least hourly while these jobs are operational. A stopped host cannot capture missing weeks: the next invocation serves the current slot. A capture can fulfill scheduled and weekly obligations together; failed publication does not evict older recovery points. Four weekly slots provide roughly three to four weeks of coverage. Use pins for longer-lived recovery points, and stop rotation during an incident. Instances sharing an archive obey its recorded retention policy; reconcile intentional changes with `bassoon snapshot policy --max-snapshots N` and update their configuration to match.

> [!TIP]
> Recovery requires immutable `manifest.json`, its valid `COMPLETE` digest, and every referenced Parquet file. Copy the complete snapshot directory, including portable pin metadata in `state.json`, with its ID unchanged between local and GCS locations to preserve both data and retention settings. Preserve `catalog.json` when copying a whole archive; a missing catalog can be discovered from complete directories and rebuilt with `bassoon snapshot repair`. Incomplete contents are never restorable. Routine management excludes retired or unpublished snapshots, but emergency restore can read surviving valid immutable contents regardless of missing, damaged, or retired mutable metadata. Pin metadata travels in `state.json`; missing or unreadable state never silently authorizes rotation.

> [!NOTE]
> Snapshots preserve logical state at capture time, not every revision, original agent session files, or original tokscale payloads. Deleted tags and notes are absent from snapshot state. Archives contain original private values and should remain private.

## Restoring your data

Stop all destination writers before recovery: collectors in every environment, systemd/launchd schedules (`bassoon schedule stop`), container workers, cron jobs, and external writers. Keep them stopped through verification. Restore requires an initialized, empty destination and rejects populated unexpected base tables too. Keep the damaged backend and a separate copy of the recovery archive until recovery is verified.

Create a separate recovery configuration pointing to a fresh storage destination and the existing archive. Changing destinations does not change a continuing collection environment's identity: preserve its original `source_id` to avoid collecting the same history into a second namespace. Restore preserves every archived source ID; the recovery configuration's ID does not filter or rewrite the archive. If the ID is lost, inspect `bassoon audit sources --from-snapshot /path/to/SNAPSHOT_ID` or a GCS URI. `bassoon audit runs --from-snapshot …` also works without a working backend or source ID. Host metadata is evidence for identifying a source, not proof of identity.

```bash
bassoon init --restore --config recovery.toml
bassoon snapshot list --config recovery.toml
bassoon snapshot audit --config recovery.toml --from-snapshot SNAPSHOT_ID
bassoon restore --config recovery.toml --from-snapshot SNAPSHOT_ID
bassoon doctor --config recovery.toml
```

`init --restore` repairs a missing source ID, provisions the destination without collecting, checks emptiness, and disables applicable scheduled maintenance. Ordinary `init` also repairs a missing ID while preserving an existing valid ID and configuration settings. GCS configuration alone never installs BigQuery maintenance.

`--from-snapshot` accepts `latest`, an exact snapshot ID, a snapshot directory, its manifest path, a GCS directory/manifest URI, or an archive root. Explicit locations override configured archive roots and use configured GCS credentials or ambient authentication. `latest` chooses the newest capture across all configured locations; an archive root chooses within that root. Corrupt or unavailable automatic candidates produce warnings and fallback to another copy of the same snapshot, then an older capture. Exact IDs/directories fail explicitly on error. The selected URI and capture time are reported. Fallback finishes before destination writes; a destination failure never selects older data.

Restore discovers and validates immutable contents independently of `catalog.json`, `control.json`, and `state.json`; damaged mutable metadata produces warnings and never blocks emergency reading. Restore checks destination emptiness, asks for confirmation (default No), disables applicable maintenance, and checks emptiness again inside the atomic restore transaction. BigQuery prints and logs “Disabling scheduled compaction...” and briefly waits for active maintenance to finish. Applicable maintenance remains disabled on success or failure, and its observed state is reported. Completion receipts bind the immutable snapshot digest and distinguish a committed restore from a lost acknowledgement. Failed BigQuery attempts use disposable, owned stages: retries drain owned jobs and clean surviving stages immediately instead of waiting for expiry. If completion cannot be determined, inspect or retry recovery before resuming writers; unrelated destination tables are never removed. All restore paths stream verified Parquet through Arrow batches or native Parquet staging instead of buffering the whole archive in memory.

Verify table counts, source IDs, date coverage, token components, pricing, tags, and notes against the archive. `doctor` checks operational health, not equality with your chosen recovery point. Then run `bassoon init --config recovery.toml` to resume applicable maintenance, redirect continuing collectors to the recovered destination while preserving their IDs, and restart their schedules. Dispose of the damaged destination at your discretion after verification.

New releases retain registered forward recovery paths for publicly released archive contracts. Application, portable data, archive packaging, and physical SQL installation versions serve distinct purposes. Physical backend changes do not automatically make a portable snapshot incompatible. Preserve original archives throughout recovery; a compatible historical released package is the documented emergency fallback when needed.

## MotherDuck setup and permissions

- Set `MOTHERDUCK_TOKEN` to a read/write token for the identity that owns the configured `[motherduck].database`. Initialization, collection, curation, and restore need a writable database.
- **Builder** is required to create and manage service accounts and their tokens through the MotherDuck UI; **Admin** also includes these permissions. MotherDuck's Admin REST API requires an Admin user's read/write token. See [service-account setup](https://motherduck.com/docs/key-tasks/service-accounts-guide/create-and-configure-service-accounts/).
- Builder is not a blanket requirement for using UsageBassoon with a personal database: MotherDuck's **Explorer** role can create databases and run SQL. Organization roles do not grant write access to another identity's database. See [MotherDuck roles and access control](https://motherduck.com/docs/concepts/roles-and-access-control/).

## BigQuery setup and permissions

> [!IMPORTANT]
> Use the most restrictive permissions and least-privilege IAM roles for BigQuery. Use a dedicated service account or user identity scoped to your project and dataset, and avoid broad project-owner permissions.

### BigQuery setup checklist

1. Enable the BigQuery, BigQuery Storage, and BigQuery Data Transfer APIs. UsageBassoon does not enable services itself.
2. Pre-create an empty dataset in the location configured in `config.toml`, or give the setup identity permission to create it. Pre-creating it avoids granting dataset creation to your everyday identity.
3. Configure a dedicated service account or your own identity through Application Default Credentials, or set `credentials_file` explicitly.
4. Grant the permissions below, configure `[bigquery]`, and run `bassoon init` with the setup identity.
5. Run `bassoon collect` and `bassoon doctor` to check access and compaction backlog. Verify the Scheduled Query is enabled and runs successfully in BigQuery; doctor does not inspect the schedule configuration.

### Permissions UsageBassoon may use

Each permission notes where it is granted and which identity needs it. Setup means the identity that runs `bassoon init` or applies a registered schema upgrade. Runtime means collection, curation, reports, queries, exports, and snapshots against an already matching schema. Grant only the permissions required by the commands the identity uses.

- **BigQuery, project scope**
  - `bigquery.jobs.create`: run queries, loads, and schema statements. Setup, runtime, restore, and scheduled compaction.
  - `bigquery.readsessions.create`, `bigquery.readsessions.getData`, `bigquery.readsessions.update`: permissions supplied by the Storage Read API role for Arrow result reads. Runtime reads and snapshot capture.
  - `bigquery.transfers.get`: find an existing compaction schedule. Setup.
  - `bigquery.transfers.update`: broader authorization to create or update the compaction schedule. Setup, optional when using Google's ownership-based path described below.
  - `bigquery.datasets.create`: create the dataset, only if you want `bassoon init` to do it. Setup, optional.
  - `bigquery.jobs.listAll`: let `bassoon doctor` inspect active transactions; without it doctor warns and continues. Doctor, optional.
- **BigQuery, dataset scope**
  - `bigquery.datasets.get`: check dataset metadata. Setup, runtime, and scheduled compaction.
  - `bigquery.tables.get`, `bigquery.tables.getData`: inspect table metadata and read schema state, reports, diagnostics, and snapshots. Setup, runtime, restore, and scheduled compaction as applicable.
  - `bigquery.tables.updateData`: append collection and curation data, seed compaction control data, and write restored or compacted state. Setup, runtime writes, restore, and scheduled compaction.
  - `bigquery.tables.create`, `bigquery.tables.update`: create tables and views, set schema labels and retention settings, replace view definitions, and apply registered schema migrations. Setup.
  - `bigquery.tables.list`: discover existing relations during init and check destination contents before restore. Setup and restore.
  - `bigquery.tables.create`, `bigquery.tables.delete`: create and remove temporary restore staging tables. Restore; table creation is also used during setup as noted above.
- **Service account scope**
  - `iam.serviceAccounts.actAs`: attach the service account that runs the compaction schedule, granted on that specific account. Setup when assigning a service account; also relevant to accessing an existing account-backed schedule for query changes.

### Roles that provide them

- `roles/bigquery.jobUser` on the project: `bigquery.jobs.create`.
- `roles/bigquery.readSessionUser` on the project: the three `bigquery.readsessions.*` permissions listed above. This role can only be granted at project level or above.
- `roles/bigquery.dataEditor` on the dataset: `bigquery.datasets.get` and every `bigquery.tables.*` permission listed above, including create, update, updateData, list, and delete. Grant it on the dataset rather than the project; project-level grants also permit dataset creation.
- `roles/bigquery.dataViewer` on the dataset instead of `dataEditor` for a read-only identity used for reports, queries, exports, and snapshot capture. It supplies dataset metadata and table read/list permissions; the identity still needs `jobUser` and `readSessionUser` on the project.
- A custom project role containing `bigquery.transfers.get` for setup using Google's ownership-based path, or containing both `bigquery.transfers.get` and `bigquery.transfers.update` for broader transfer management. Avoid granting project-wide BigQuery Admin merely to manage the schedule.
- `roles/iam.serviceAccountUser` on the schedule's specific service account: `iam.serviceAccounts.actAs`. If the same service account both runs `bassoon init` and is assigned to the new schedule, it needs this grant on itself.
- `roles/bigquery.resourceViewer` on the project, or a custom role containing only `bigquery.jobs.listAll`, for doctor's optional transaction check.

### Least-privilege notes

- Separate setup and runtime identities where useful. Collection needs no schema or schedule provisioning; apply registered migrations with the setup identity first.
- Schedule ownership can replace `bigquery.transfers.update`. Editing query text still requires ownership or access to the associated service account. See [scheduled-query permissions](https://docs.cloud.google.com/bigquery/docs/scheduling-queries#required_permissions).
- New schedules use the initializing service account when identifiable, otherwise the initializing user. Rerunning init preserves the existing execution identity.
- The compaction identity needs job creation and dataset read/write access, without transfer-management or Storage Read permissions.

See Google's [BigQuery IAM roles and permissions](https://docs.cloud.google.com/iam/docs/roles-permissions/bigquery), [dataset access controls](https://docs.cloud.google.com/bigquery/docs/access-control), and [job-metadata permissions](https://docs.cloud.google.com/bigquery/docs/information-schema-jobs#required_permissions).

## Google Cloud Storage setup and permissions

Google Cloud Storage (GCS) is an optional snapshot destination, independent of the storage backend. Use it with DuckDB, MotherDuck, or BigQuery; GCS snapshots do not require BigQuery or its IAM roles.

### Setup checklist

1. Install the GCS extra with `pipx install "usagebassoon[gcs]"`, or include `gcs` alongside your other required extras.
2. Create a private snapshot bucket yourself; UsageBassoon never creates buckets.
3. Configure `[gcs].uri` and `[gcs].project` in `config.toml`. Use Application Default Credentials or set `[gcs].credentials_file` explicitly.
4. Grant the bucket permissions below to the identity used for snapshots. Keep snapshots private: they contain raw restoration data.
5. Run `bassoon snapshot` to publish an archive and `bassoon doctor` to inspect bucket lifecycle rules. Configure `[snapshots].interval` for due-only automatic snapshots after collection.

### Permissions UsageBassoon may use

Grant only the permissions needed for archive writes, retention, restore, or diagnostic inspection.

- **Cloud Storage, snapshot bucket scope**
  - `storage.objects.create`, `storage.objects.delete`, `storage.objects.get`, `storage.objects.list`: publish, read, and rotate snapshot archives and their catalogs. Runtime archive writes and retention.
  - `storage.objects.get`, `storage.objects.list`: read archives for restore without writing or rotation. Restore-only archive access; the destination backend still needs restore permissions.
  - `storage.buckets.get`: let `bassoon doctor` read lifecycle rules. Doctor only; without it doctor reports that lifecycle inspection is unavailable.

### Roles that provide them

- `roles/storage.objectUser` on the snapshot bucket for a snapshot writer: object create, get, list, and delete.
- `roles/storage.objectViewer` on the snapshot bucket for restore-only archive access: object get and list.
- `roles/storage.bucketViewer` on the snapshot bucket for identities that run doctor's lifecycle checks: `storage.buckets.get`.

### Least-privilege notes

- Scope grants to the snapshot bucket. `storage.buckets.get` is only needed for doctor's lifecycle check.
- Use `STANDARD` for active archives to avoid colder classes' retrieval and early-deletion costs. See [storage classes](https://docs.cloud.google.com/storage/docs/storage-classes).

See Google's [Cloud Storage IAM roles and permissions](https://docs.cloud.google.com/iam/docs/roles-permissions/storage).

## Why AGPL?

UsageBassoon exists to record statistics about *your* token usage—data you generated with your own activity and money. We believe that data *belongs to you*, and the ability to persist and inspect your own usage history should never sit behind a paywall or a proprietary service.

> Any agentic user should always be able to inspect their token usage history for FREE and answer questions like:
>
> **What did I spend, on which models, and was it worth it?**

The license mirrors that belief. AGPLv3 means that anyone who modifies UsageBassoon and offers it as a network service *must* make their modified source available to its users. That means improvements to the access-layer, the actual mechanic of storing and viewing your token history, remains freely available to everyone that depends on it, and SaaS-based commercial licensing is restricted to genuine value added on top of that access, like dashboards, hosting, and team reporting.

## License & Disclaimers

> [!NOTE]
> **Data disclaimer.** UsageBassoon is a personal, local-first token usage statistics tool. It reads token-usage statistics from your local `tokscale` environment and persists them to either a configurable local DuckDB database or a remote data analytics warehouse. UsageBassoon does not and will **never** collect, aggregate, or sell your token usage data.

---

UsageBassoon is copyright © 2026 Israel Flores-Arbolay and licensed under the GNU Affero General Public License v3.0 (AGPL-3.0-only). See LICENSE for the full text.
