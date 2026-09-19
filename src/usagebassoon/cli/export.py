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
from rich.console import Console

from usagebassoon.backends.base import close_backend
from usagebassoon.config import ConfigurationError, ConfigurationManager, open_backend
from usagebassoon.privacy import sanitize_table

ExportFormat = Literal["csv", "json", "parquet"]
_EXPORTABLE_RELATIONS = frozenset(
    {
        "daily_activity",
        "daily_stats",
        "daily_cost",
        "daily_processed_state",
        "ingest_runs",
        "notes",
        "noted_sessions",
        "price_versions",
        "reconciliation_issues",
        "run_metrics",
        "schema_drift",
        "session_model_stats",
        "session_model_stats_current",
        "session_tags",
        "sessions",
        "tagged_sessions",
        "tags",
    }
)


def _json_default(value: object) -> str:
    """Serialize date-like values for JSON files."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def export(
    target: Annotated[str, typer.Argument(help="A supported table or view name.")],
    output: Annotated[Path, typer.Argument(help="Destination file path.")],
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
) -> None:
    """Export a relation, obfuscating share-sensitive fields by default."""
    del sanitize
    if target not in _EXPORTABLE_RELATIONS:
        allowed = ", ".join(sorted(_EXPORTABLE_RELATIONS))
        raise typer.BadParameter(
            f"target must be one of: {allowed}",
            param_hint="target",
        )
    if not raw:
        Console(stderr=True).print(
            "Export output is obfuscated by default. Use --raw to export original "
            "values for personal backup or data-management use.",
            style="yellow",
        )
    try:
        configuration = ConfigurationManager(config).load()
        backend = open_backend(configuration)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    try:
        result = backend.query(f"SELECT * FROM {target}")
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
