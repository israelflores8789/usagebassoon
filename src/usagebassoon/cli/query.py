# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""query.py — Allowlisted relation-query command with a sharing-safety warning."""

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
from usagebassoon.display import sanitize_display
from usagebassoon.sql_safety import (
    MAX_PUBLIC_QUERY_LIMIT,
    PUBLIC_RELATIONS,
    build_relation_query,
    dialect_for_backend,
    validate_read_only_sql,
)

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
        rendered.add_column(sanitize_display(column))
    for row in rows:
        rendered.add_row(*(sanitize_display(row.get(column, "")) for column in columns))
    Console(markup=False, highlight=False).print(rendered)


def _filters(values: tuple[str, ...]) -> dict[str, str]:
    """Parse repeated ``column=value`` CLI filters into bound parameters."""
    filters: dict[str, str] = {}
    for value in values:
        column, separator, parameter = value.partition("=")
        if not separator or not column or column in filters:
            raise ValueError("--filter must be unique and use column=value")
        filters[column] = parameter
    return filters


def query(
    relation: Annotated[
        str,
        typer.Argument(
            help=(
                "Supported UsageBassoon relation: "
                + ", ".join(sorted(PUBLIC_RELATIONS))
            )
        ),
    ],
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    filters: Annotated[
        list[str] | None,
        typer.Option("--filter", help="Equality filter in column=value form."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            "--limit",
            min=1,
            max=MAX_PUBLIC_QUERY_LIMIT,
            help="Maximum result rows.",
        ),
    ] = 1_000,
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
    """Run one bounded allowlisted relation query and warn before output."""
    Console(stderr=True).print(_RAW_QUERY_WARNING, style="yellow")
    try:
        configuration = ConfigurationManager(config).load()
        sql, parameters = build_relation_query(
            relation,
            filters=_filters(tuple(filters or ())),
            limit=limit,
        )
        validate_read_only_sql(sql, dialect=dialect_for_backend(configuration.backend))
        backend = open_backend(configuration)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="relation") from error
    try:
        result = backend.query(sql, parameters)
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
