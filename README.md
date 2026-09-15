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

## License & Disclaimers

UsageBassoon is copyright © 2026 Israel Flores-Arbolay and licensed under the GNU Affero General Public License v3.0 (AGPL-3.0-only). See LICENSE for the full text.
