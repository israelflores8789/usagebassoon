# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""doctor.py — Typer command for read-only UsageBassoon health diagnostics."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.text import Text

from usagebassoon.config import ConfigurationError, ConfigurationManager, open_backend
from usagebassoon.drift import DoctorReport, run_doctor


def _print_report(report: DoctorReport) -> None:
    """Render a structured doctor report with Rich."""
    console = Console()
    styles = {"ok": "green", "warning": "yellow", "error": "red"}
    for check in report.checks:
        line = Text(f"{check.status.upper()} ", style=styles[check.status])
        line.append(f"{check.name}: {check.message}")
        console.print(line)
        for detail in check.details:
            console.print(f"  - {detail}", markup=False)
    console.print(f"\nOverall status: {report.status}")


def doctor(
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Use this configuration file instead of the environment/default path.",
        ),
    ] = None,
    strict: Annotated[
        bool,
        typer.Option("--strict", help="Return failure when warnings are present, too."),
    ] = False,
    limit: Annotated[
        int,
        typer.Option(
            "--limit",
            min=1,
            help="Maximum unresolved drift and recent run records to display.",
        ),
    ] = 20,
) -> None:
    """Check the configured backend, schema, drift, and ingest health."""
    manager = ConfigurationManager(config)
    opened = None
    config_error: str | None = None
    connection_error: str | None = None
    configuration = None
    try:
        configuration = manager.load()
        try:
            opened = open_backend(configuration)
        except Exception as error:
            connection_error = str(error)
    except ConfigurationError as error:
        config_error = str(error)

    report = run_doctor(
        opened,
        backend_name=configuration.backend if configuration else "",
        database=configuration.database if configuration else None,
        config_path=str(manager.path),
        config_error=config_error,
        connection_error=connection_error,
        snapshot_enabled=(configuration.snapshots is not None)
        if configuration
        else None,
        limit=limit,
    )
    try:
        _print_report(report)
    finally:
        if opened is not None:
            opened.close()
    if report.exit_code(strict=strict):
        raise typer.Exit(code=1)
