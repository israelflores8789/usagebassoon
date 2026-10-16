# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""restore.py — Typer command for restoring a snapshot into an empty store."""

from __future__ import annotations

import logging
from contextlib import ExitStack
from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.audit import source_identity_warning
from usagebassoon.backends.base import close_backend
from usagebassoon.cli._utils import configured_backend, snapshot_archiver
from usagebassoon.cli.snapshot import confirm_location, notice
from usagebassoon.cli.spinner import spinner
from usagebassoon.snapshot.restore import (
    restore_operation_id,
    restore_prepared,
)


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
            help="Drain owned restore jobs and remove disposable stages.",
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
        try:
            with ExitStack() as stack:
                preparation_notices: list[str] = []
                try:
                    with spinner(configuration):
                        archiver = snapshot_archiver(configuration)
                        prepared = stack.enter_context(
                            archiver.reader.prepare(
                                snapshot, warning=preparation_notices.append
                            )
                        )
                finally:
                    for message in preparation_notices:
                        notice(message)
                notice(
                    f"Snapshot: {prepared.candidate.uri} "
                    f"(captured {prepared.candidate.captured_at})"
                )
                if configuration.backend == "bigquery" and configuration.bigquery:
                    destination = (
                        f"{configuration.bigquery.project}."
                        f"{configuration.bigquery.dataset}"
                    )
                elif configuration.backend == "motherduck" and configuration.motherduck:
                    destination = configuration.motherduck.database
                else:
                    destination = str(configuration.local_database)
                notice(f"Destination: {configuration.backend} {destination}")
                confirm_location(archiver, prepared.candidate.uri)
                try:
                    identity_warning = source_identity_warning(
                        configuration.source_id, prepared
                    )
                except Exception:
                    logging.getLogger("usagebassoon").warning(
                        "Restore source-identity advice is unavailable",
                        exc_info=True,
                    )
                    identity_warning = (
                        "Source matching is unavailable; "
                        "restore preserves archived source IDs."
                    )
                if identity_warning:
                    notice(identity_warning)
                typer.confirm(
                    "Have all writers stopped, and do you wish to proceed?",
                    default=False,
                    abort=True,
                )
                with spinner(configuration):
                    if cleanup_stages:
                        backend.cleanup_restore_stages()
                    if not backend.restore_committed(restore_operation_id(prepared)):
                        backend.check_restore_empty()
                    restored = restore_prepared(
                        backend, prepared, notice=notice, preflight_checked=True
                    )
        except typer.Abort:
            raise
        except (OSError, RuntimeError, ValueError) as error:
            raise typer.BadParameter(str(error)) from error
        except Exception as error:
            logging.getLogger("usagebassoon").exception("Snapshot restore failed")
            raise typer.BadParameter(str(error)) from error
    finally:
        close_backend(backend, context="restoring a snapshot")
    details = ", ".join(f"{table}={count}" for table, count in restored.items())
    typer.echo(f"Restored {details}")
