<!--
SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
SPDX-License-Identifier: AGPL-3.0-only
-->

<h1 align="center">UsageBassoon</h1>
<p align="center"><strong>Persistent AI token usage statistics no matter where your agents live</strong></p>

## Start

```bash
bassoon init
```

This creates `~/.config/usagebassoon/config.toml` when absent, generates a stable `source_id`, and initializes a local DuckDB warehouse. Use `--config` to select another configuration file. Existing configuration is never overwritten.

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

[bigquery]                                          # Required when backend is `bigquery`.
project = "my-gcp-project"                          # Google Cloud project ID.
location = "US"                                     # Optional dataset and job location; defaults to `US`.
credentials_file = "path/to/gcp-sa-secret.json"     # Optional; default uses Application Default Credentials.

[collection]                                        # Optional scheduled collection & retry settings.
max_retries = 3                                     # Additional attempts after the first persistence failure.
retry_initial_seconds = 1.0                         # Positive initial delay for exponential backoff.

[logging]                                           # Optional rotating operational log settings.
directory = "~/.local/state/usagebassoon/logs"      # Log files directory.
max_files = 5                                       # Retained files, including the active file.
max_bytes = 10485760                                # Max size of the active log file before rotation.

[snapshots]                                         # Optional retention and automatic snapshot settings.
gcs_uri = "gs://my-private-bucket/usagebassoon"     # Optional local `file://` or private `gs://` archive.
max_snapshots = 3                                   # Max number of snapshots to retain.
interval = "12h"                                    # Optional positive cadence (`30m`, `12h`, `7d`);
                                                    # enables due-only snapshots after collection when set.
```

The `bigquery` table is only required for the BigQuery backend. BigQuery is *not* required to persist snapshots to Google Cloud Storage, and GCS is *not* required to use BigQuery. If `snapshots.gcs_uri` is omitted, snapshots use the conventional local archive at `~/.usagebassoon/snapshots/`.

## Source identity and curation

`source_id` separates data collected from different environments, even when their client, workspace, or session names match. Reuse a source ID only when those environments intentionally share a collection namespace.

Tags can target a workspace, client, or one session; notes belong to one session.

```bash
bassoon tag project-alpha --workspace /work/repo
bassoon tag production --client codex
bassoon tag important --client codex --session ses_123
bassoon note "Investigate cache miss" --client codex --session ses_123
```

## Privacy and sharing

Reports are raw by default for personal terminal use; run `bassoon report --sanitize` before sharing one. `bassoon doctor` is the shareable diagnostics command and sanitizes configuration locations and credentials by default. Its `--raw` mode prints a warning not to paste raw output into public GitHub issues.

`bassoon query` intentionally returns raw values, accepts only one read-only SELECT or WITH query, and always warns on stderr not to share its output publicly. Use `bassoon doctor` for issue-ready diagnostics instead.

`bassoon export` obfuscates potentially identifying fields and redacts notes by default. It announces this behavior on stderr; use `--raw` only for intentional personal backup or data-management exports. Snapshots are raw restoration artifacts and should be kept private.

## Snapshots

`bassoon snapshot` writes a catalog-published Parquet restoration archive and `bassoon restore --from-snapshot latest` restores only complete published snapshots into an initialized empty warehouse. Local archives rotate under `~/.usagebassoon/snapshots/` by default. Configure `[snapshots] max_snapshots = 3` and an optional positive interval such as `12h`; an interval also enables due-only automatic snapshots after collection. Set `gcs_uri = "gs://bucket/private/usagebassoon-snapshots"` to use Google Cloud Storage (install `usagebassoon[gcs]`); the archive uses generation-conditional catalog publication and stores raw private data.

## License & Disclaimers

UsageBassoon is copyright © 2026 Israel Flores-Arbolay and licensed under the GNU Affero General Public License v3.0 (AGPL-3.0-only). See LICENSE for the full text.
