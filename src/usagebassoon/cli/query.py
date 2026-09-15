# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""query.py — Raw, read-only SQL command with a sharing-safety warning."""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal

import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import typer
from rich.console import Console
from rich.table import Table

from usagebassoon.config import ConfigurationError, ConfigurationManager, open_backend
from usagebassoon.sql_safety import dialect_for_backend, validate_read_only_sql

QueryFormat = Literal["table", "csv", "json", "parquet"]

_RAW_QUERY_WARNING = (
    "WARNING: bassoon query returns raw data, which may include session IDs, "
    "workspace names, tags, notes, paths, and host metadata. Do not share "
    "results publicly or paste them into GitHub issues. Use bassoon doctor "
    "for shareable diagnostics."
)


def _json_default(value: object) -> str:
    """Serialize dates and Arrow-adapted scalars for JSON output."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _render_table(rows: list[dict[str, object]], columns: list[str]) -> None:
    """Render raw query rows with Rich.

    Args:
        rows: Materialized query result rows.
        columns: Result columns in backend order.
    """
    rendered = Table(show_header=True)
    for column in columns:
        rendered.add_column(column)
    for row in rows:
        rendered.add_row(*(str(row.get(column, "")) for column in columns))
    Console().print(rendered)


def query(
    sql: Annotated[str, typer.Argument(help="One read-only SELECT or WITH query.")],
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    format: Annotated[
        QueryFormat,
        typer.Option("--format", help="Output format: table, csv, json, or parquet."),
    ] = "table",
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            help="Write csv, json, or parquet output to this path.",
        ),
    ] = None,
) -> None:
    """Run raw read-only SQL and warn before emitting its result."""
    Console(stderr=True).print(_RAW_QUERY_WARNING, style="yellow")
    try:
        configuration = ConfigurationManager(config).load()
        validate_read_only_sql(sql, dialect=dialect_for_backend(configuration.backend))
        backend = open_backend(configuration)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="sql") from error
    try:
        result = backend.query(sql)
    finally:
        backend.close()
    if format == "table":
        if output is not None:
            raise typer.BadParameter("--output requires csv, json, or parquet")
        _render_table(result.to_pylist(), result.column_names)
    elif format == "csv":
        if output is None:
            pacsv.write_csv(result, sys.stdout.buffer)
        else:
            pacsv.write_csv(result, output)
    elif format == "json":
        payload = json.dumps(result.to_pylist(), default=_json_default) + "\n"
        if output is None:
            typer.echo(payload, nl=False)
        else:
            output.write_text(payload)
    else:
        if output is None:
            raise typer.BadParameter("--output is required for parquet format")
        pq.write_table(result, output)
