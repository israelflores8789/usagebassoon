# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""restore.py — Typer command for restoring a snapshot into an empty store."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.cli._utils import configured_backend, snapshot_archiver
from usagebassoon.cli.spinner import spinner


def restore(
    snapshot: Annotated[
        str,
        typer.Option("--from-snapshot", help="Snapshot stamp or latest."),
    ] = "latest",
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Restore a raw snapshot into an empty configured warehouse."""
    typer.echo(
        "Warning: stop all UsageBassoon instances and writers before restoring. "
        "Concurrent writes can mix new data with the snapshot and prevent a "
        "consistent restore.",
        err=True,
    )
    typer.confirm(
        "Have all writers stopped, and do you wish to proceed?",
        default=False,
        abort=True,
    )
    configuration, backend = configured_backend(config)
    try:
        with spinner(configuration):
            try:
                restored = snapshot_archiver(configuration).restore(backend, snapshot)
            except (RuntimeError, ValueError) as error:
                raise typer.BadParameter(str(error)) from error
    finally:
        backend.close()
    details = ", ".join(f"{table}={count}" for table, count in restored.items())
    typer.echo(f"Restored {details}")
