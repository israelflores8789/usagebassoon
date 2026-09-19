# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""doctor.py — Typer command for read-only UsageBassoon health diagnostics."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.text import Text

from usagebassoon.cli._utils import snapshot_store
from usagebassoon.config import (
    ConfigurationError,
    ConfigurationManager,
    UsageBassoonConfig,
    open_backend,
)
from usagebassoon.display import sanitize_display
from usagebassoon.drift import DoctorReport, run_doctor
from usagebassoon.privacy import sanitize_doctor_text


def _print_report(
    report: DoctorReport,
    *,
    raw: bool,
    config_path: str | None,
    database: str | None,
) -> None:
    """Render a structured doctor report with Rich."""
    console = Console(markup=False, highlight=False)
    styles = {"ok": "green", "warning": "yellow", "error": "red"}
    for check in report.checks:
        line = Text(f"{check.status.upper()} ", style=styles[check.status])
        message = (
            check.message
            if raw
            else sanitize_doctor_text(
                check.message,
                config_path=config_path,
                database=database,
            )
        )
        line.append(f"{sanitize_display(check.name)}: {sanitize_display(message)}")
        console.print(line)
        for detail in check.details:
            text = (
                detail
                if raw
                else sanitize_doctor_text(
                    detail,
                    config_path=config_path,
                    database=database,
                )
            )
            console.print(f"  - {sanitize_display(text)}", markup=False)
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
    raw: Annotated[
        bool,
        typer.Option(
            "--raw",
            help="Show original diagnostic locations and connection details.",
        ),
    ] = False,
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
        snapshot_warnings=(
            _snapshot_warnings(configuration)
            if configuration
            and configuration.snapshots
            and configuration.snapshots.gcs_uri
            else ()
        ),
        limit=limit,
    )
    try:
        if raw:
            Console(stderr=True).print(
                "WARNING: raw doctor output may contain identifiers, paths, host "
                "metadata, and other private information. Do not paste it into a "
                "public GitHub issue. Use bassoon doctor without --raw for shareable "
                "diagnostics.",
                style="yellow",
            )
        _print_report(
            report,
            raw=raw,
            config_path=str(manager.path),
            database=configuration.database if configuration else None,
        )
    finally:
        if opened is not None:
            opened.close()
    if report.exit_code(strict=strict):
        raise typer.Exit(code=1)


def _snapshot_warnings(configuration: UsageBassoonConfig) -> tuple[str, ...]:
    """Inspect configured GCS lifecycle rules without failing doctor outright."""
    try:
        return snapshot_store(configuration).lifecycle_warnings()
    except Exception as error:
        return (f"GCS lifecycle inspection unavailable: {error}",)
