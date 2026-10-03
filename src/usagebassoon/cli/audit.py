# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""audit.py — Source/run audits and aliases for snapshot integrity auditing."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from usagebassoon.audit import audit_runs, audit_sources
from usagebassoon.backends.base import close_backend
from usagebassoon.cli._output import output_console, render_records
from usagebassoon.cli._utils import configured_backend
from usagebassoon.cli.snapshot import audit_snapshot, notice, read_archiver
from usagebassoon.display import sanitize_display
from usagebassoon.storage_model import SNAPSHOT_TABLES

audit_app = typer.Typer(
    no_args_is_help=True, help="Audit collection history, sources, and snapshots."
)


def _audit(
    *,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ],
    selection: str | None,
    sources: bool,
    limit: int,
    json_output: bool,
    csv_output: bool,
) -> None:
    """Share backend-independent input and structured output for run/source audits."""
    if json_output and csv_output:
        raise typer.BadParameter("--json and --csv are mutually exclusive")
    try:
        if selection is not None:
            tables = SNAPSHOT_TABLES if sources else ("collection_ledger",)
            with read_archiver(config).reader.prepare(
                selection, tables=tables, warning=notice
            ) as prepared:
                rows = (
                    audit_sources(prepared=prepared)
                    if sources
                    else audit_runs(prepared=prepared, limit=limit)
                )
        else:
            _, backend = configured_backend(config)
            try:
                rows = (
                    audit_sources(backend)
                    if sources
                    else audit_runs(backend, limit=limit)
                )
            finally:
                close_backend(backend, context="auditing collection history")
        if not sources and not json_output and not csv_output:
            table = Table(title="Recent collection audit records")
            columns = ("run_id", "source_id", "started_at", "finished_at", "status")
            for column in columns:
                table.add_column(column)
            for row in rows:
                table.add_row(
                    *(sanitize_display(str(row.get(column))) for column in columns)
                )
            output_console().print(table)
            return
        render_records(
            rows,
            columns=tuple(rows[0]) if rows else ("source_id",),
            format="json" if json_output else "csv" if csv_output else "yaml",
            save=None,
        )
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error


@audit_app.command(name="runs")
def runs(
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    selection: Annotated[
        str | None,
        typer.Option(
            "--from-snapshot",
            help="Read an archive without opening the destination backend.",
        ),
    ] = None,
    limit: Annotated[int, typer.Option("--limit", min=1)] = 20,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit JSON instead of YAML.")
    ] = False,
    csv_output: Annotated[bool, typer.Option("--csv")] = False,
) -> None:
    """Show recent complete run records from backend views or an archive."""
    _audit(
        config=config,
        selection=selection,
        sources=False,
        limit=limit,
        json_output=json_output,
        csv_output=csv_output,
    )


@audit_app.command(name="sources")
def sources(
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    selection: Annotated[
        str | None,
        typer.Option(
            "--from-snapshot",
            help="Read an archive without opening the destination backend.",
        ),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit JSON instead of YAML.")
    ] = False,
    csv_output: Annotated[bool, typer.Option("--csv")] = False,
) -> None:
    """Identify recorded sources, last activity, and latest host metadata."""
    _audit(
        config=config,
        selection=selection,
        sources=True,
        limit=20,
        json_output=json_output,
        csv_output=csv_output,
    )


audit_app.command(name="snapshot")(audit_snapshot)
audit_app.command(name="snapshots")(audit_snapshot)


@audit_app.command(name="restores")
def restores(
    config: Annotated[Path | None, typer.Option("--config")] = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Inspect owned interrupted restore stages and their expiration metadata."""
    _, backend = configured_backend(config)
    try:
        rows = backend.restore_stages()
        render_records(
            rows, columns=(), format="json" if json_output else "yaml", save=None
        )
    finally:
        close_backend(backend, context="inspecting restore stages")
