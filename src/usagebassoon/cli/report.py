# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""report.py — Typer command for personal-use terminal usage summaries."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from usagebassoon.backends.base import close_backend
from usagebassoon.cli._utils import configured_backend
from usagebassoon.display import sanitize_display
from usagebassoon.privacy import sanitize_table


def report(
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    save: Annotated[
        Path | None,
        typer.Option("--save", help="Write the report as text."),
    ] = None,
    sanitize: Annotated[
        bool,
        typer.Option(
            "--sanitize",
            "--obfuscate",
            help="Obfuscate report labels for sharing.",
        ),
    ] = False,
) -> None:
    """Render a personal-use warehouse summary."""
    _, backend = configured_backend(config)
    try:
        sessions = backend.query("SELECT * FROM report_summary")
        models = backend.query("SELECT * FROM report_models")
    finally:
        close_backend(backend, context="rendering a report")
    if sanitize:
        models = sanitize_table(models)
    summary = sessions.to_pylist()[0]
    table = Table(title="UsageBassoon report")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Sessions", str(summary["sessions"]))
    table.add_row("Cost (USD)", f"{summary['cost_usd']:.6f}")
    model_table = Table(title="Models")
    for column in models.column_names:
        model_table.add_column(sanitize_display(column))
    for row in models.to_pylist():
        model_table.add_row(
            *(sanitize_display(row[column]) for column in models.column_names)
        )
    console = Console(record=save is not None, markup=False, highlight=False)
    console.print(table)
    console.print(model_table)
    if save is None and not sanitize:
        console.print(
            "Sharing this report? Re-run with --sanitize to obfuscate identifiers "
            "and free text.",
            style="yellow",
        )
    if save is not None:
        save.write_text(console.export_text())
