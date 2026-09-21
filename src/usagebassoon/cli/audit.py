# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""audit.py — Typer command for recent collection audit history."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from usagebassoon.cli._output import output_console
from usagebassoon.cli._utils import configured_backend


def audit(
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, help="Maximum audit records to show."),
    ] = 20,
) -> None:
    """Show recent ingestion audit records."""
    _, backend = configured_backend(config)
    try:
        data = backend.query(
            "SELECT run_id, started_at, finished_at, status, rows_in, rows_inserted, "
            "rows_updated, drift_events FROM ingest_runs "
            f"ORDER BY finished_at DESC LIMIT {limit}"
        )
    finally:
        backend.close()
    table = Table(title="Recent ingest audit records")
    for column in data.column_names:
        table.add_column(column)
    for row in data.to_pylist():
        table.add_row(*(str(row[column]) for column in data.column_names))
    output_console().print(table)
