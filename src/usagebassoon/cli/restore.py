# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""restore.py — Typer command for restoring a snapshot into an empty store."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.audit import source_identity_warning
from usagebassoon.backends.base import close_backend
from usagebassoon.cli._utils import configured_backend, snapshot_archiver
from usagebassoon.cli.snapshot import notice
from usagebassoon.cli.spinner import spinner
from usagebassoon.snapshot.restore import restore_operation_id, restore_prepared


def restore(
    snapshot: Annotated[
        str,
        typer.Option("--from-snapshot", help="Snapshot stamp or latest."),
    ] = "latest",
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    cleanup_stages: Annotated[
        bool,
        typer.Option(
            "--cleanup-stages",
            help="Remove expired owned restore stages before checking emptiness.",
        ),
    ] = False,
) -> None:
    """Restore a raw snapshot into an empty configured warehouse."""
    typer.echo(
        "Warning: stop all UsageBassoon instances and writers before restoring. "
        "Concurrent writes can mix new data with the snapshot and prevent a "
        "consistent restore.",
        err=True,
    )
    configuration, backend = configured_backend(config)
    try:
        with spinner(configuration):
            try:
                with snapshot_archiver(configuration).reader.prepare(
                    snapshot, warning=notice
                ) as prepared:
                    if cleanup_stages:
                        backend.cleanup_restore_stages()
                    if not backend.restore_committed(restore_operation_id(prepared)):
                        backend.check_restore_empty()
                    identity_warning = source_identity_warning(
                        configuration.source_id, prepared
                    )
                    if identity_warning:
                        notice(identity_warning)
                    typer.confirm(
                        "Have all writers stopped, and do you wish to proceed?",
                        default=False,
                        abort=True,
                    )
                    restored = restore_prepared(
                        backend, prepared, notice=notice, preflight_checked=True
                    )
            except typer.Abort:
                raise
            except (RuntimeError, ValueError) as error:
                raise typer.BadParameter(str(error)) from error
    finally:
        close_backend(backend, context="restoring a snapshot")
    details = ", ".join(f"{table}={count}" for table, count in restored.items())
    typer.echo(f"Restored {details}")
