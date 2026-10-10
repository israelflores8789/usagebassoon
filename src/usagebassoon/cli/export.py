# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""export.py — Share-safe relation exports with an explicit raw escape hatch."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal

import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import typer
from rich.table import Table

from usagebassoon.backends.base import close_backend
from usagebassoon.backends.factory import open_backend
from usagebassoon.cli._output import output_console
from usagebassoon.cli.spinner import spinner
from usagebassoon.config import ConfigurationError, ConfigurationManager
from usagebassoon.privacy import sanitize_table
from usagebassoon.sql_safety import PUBLIC_RELATIONS
from usagebassoon.storage_model import EVENT_KEYS, STATE_KEYS

ExportFormat = Literal["csv", "json", "parquet"]
_CANONICAL_EXPORTS = frozenset(STATE_KEYS | EVENT_KEYS)
_EXPORTABLE_RELATIONS = _CANONICAL_EXPORTS | PUBLIC_RELATIONS
_RELATION_DESCRIPTIONS = {
    "sessions": "Session metadata, timestamps, workspaces, and observed activity.",
    "daily_stats": "Daily token usage per source, session, and model.",
    "price_versions": "Model token prices observed on each collection date.",
    "tags": "User tags assigned to workspaces, clients, or sessions.",
    "notes": "User notes attached to source-specific sessions.",
    "collection_ledger": "Collection outcomes, acquisition counts, and host metadata.",
    "schema_drift_events": "Payload schema changes and their resolution state.",
    "reconciliation_issues": "Token discrepancies and their resolution state.",
    "daily_cost": "Daily token usage with costs and pricing provenance.",
    "session_model_stats": "All-time token usage and costs per session and model.",
    "report_daily_usage": "Daily usage and costs with workspace attribution.",
    "report_session_models": "Session model totals with workspace and activity.",
    "report_summary": "Overall session count and total usage cost.",
    "report_summary_models": "Overall token usage and cost totals per model.",
    "report_models": "Daily model usage facts with workspace and cost attribution.",
    "session_tags": "Effective session tags inherited from all assignment scopes.",
    "tagged_sessions": "Session metadata joined with effective tags.",
    "session_notes": "Current session notes with identifiers and mutation timestamps.",
    "noted_sessions": "Session metadata joined with user notes.",
}


def _json_default(value: object) -> str:
    """Serialize date-like values for JSON files."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def export(
    target: Annotated[
        str | None,
        typer.Argument(help="A supported target; discover names with --list."),
    ] = None,
    output: Annotated[
        Path | None, typer.Argument(help="Destination file path.")
    ] = None,
    format: Annotated[
        ExportFormat,
        typer.Option("--format", help="File format: csv, json, or parquet."),
    ] = "parquet",
    raw: Annotated[
        bool,
        typer.Option(
            "--raw",
            help="Export original values; unsafe to share without review.",
        ),
    ] = False,
    sanitize: Annotated[
        bool,
        typer.Option(
            "--sanitize",
            "--obfuscate",
            help="Obfuscate output (the default; aliases are equivalent).",
        ),
    ] = True,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    list_kind: Annotated[
        Literal["tables", "views"] | None,
        typer.Option("--list", help="List supported export targets and descriptions."),
    ] = None,
) -> None:
    """Export a relation, obfuscating share-sensitive fields by default."""
    del sanitize
    if list_kind is not None:
        if target is not None or output is not None:
            raise typer.BadParameter(
                "--list cannot be combined with target or output arguments",
                param_hint="--list",
            )
        names = _CANONICAL_EXPORTS if list_kind == "tables" else PUBLIC_RELATIONS
        table = Table(title=f"Exportable {list_kind}")
        table.add_column("Target", no_wrap=True)
        table.add_column("Description")
        for name in sorted(names):
            table.add_row(name, _RELATION_DESCRIPTIONS[name])
        console = output_console()
        console.print(table)
        if list_kind == "tables":
            console.print("Table targets export canonical current state.")
        return
    if target is None:
        raise typer.BadParameter(
            "target is required; use --list tables or --list views to discover targets",
            param_hint="target",
        )
    if output is None:
        raise typer.BadParameter("output is required", param_hint="output")
    if target not in _EXPORTABLE_RELATIONS:
        allowed = ", ".join(sorted(_EXPORTABLE_RELATIONS))
        raise typer.BadParameter(
            f"target must be one of: {allowed}",
            param_hint="target",
        )
    if not raw:
        output_console(stderr=True).print(
            "Export output is obfuscated by default. Use --raw to export original "
            "values for personal backup or data-management use.",
            style="yellow",
        )
    try:
        configuration = ConfigurationManager(config).load()
        with spinner(configuration):
            backend = open_backend(configuration)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    try:
        with spinner(configuration):
            relation = "current_" + target if target in _CANONICAL_EXPORTS else target
            result = backend.query(f"SELECT * FROM {relation}")
    finally:
        close_backend(backend, context="exporting data")
    if not raw:
        result = sanitize_table(result)
    if format == "csv":
        pacsv.write_csv(result, output)
    elif format == "json":
        output.write_text(json.dumps(result.to_pylist(), default=_json_default) + "\n")
    else:
        pq.write_table(result, output)
