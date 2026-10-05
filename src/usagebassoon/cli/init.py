# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""init.py — Typer command to initialize UsageBassoon configuration and schema."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.backends.base import close_backend
from usagebassoon.backends.factory import open_backend
from usagebassoon.cli.snapshot import notice
from usagebassoon.cli.spinner import spinner
from usagebassoon.config import (
    ConfigurationError,
    ConfigurationManager,
    write_initial_config,
)


def init(
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help=(
                "Create or use this configuration file instead of the "
                "environment/default path."
            ),
        ),
    ] = None,
    restore: Annotated[
        bool,
        typer.Option(
            "--restore",
            help="Initialize an empty destination with maintenance disabled.",
        ),
    ] = False,
) -> None:
    """Create configuration when absent, then apply the configured schema."""
    manager = ConfigurationManager(config)
    try:
        created = write_initial_config(manager.path)
        configuration = manager.load()
        with spinner(configuration):
            backend = open_backend(configuration, initialize=True)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    schedule = None
    try:
        with spinner(configuration):
            backend.apply_ddl()
            if restore:
                backend.check_restore_empty()
            schedule = backend.configure_maintenance(enabled=not restore)
            if restore:
                backend.prepare_recovery(notice=notice)
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    finally:
        close_backend(backend, context="initializing the schema")
    if schedule is not None:
        state = "disabled for recovery" if restore else "scheduled at 02:00 UTC"
        typer.echo(f"Nightly compaction {state}: {schedule}")
    if restore:
        typer.echo(
            "Recovery destination is empty. Keep collectors and schedules "
            "stopped until restore and verification finish.",
            err=True,
        )
    action = "Created" if created else "Using existing"
    typer.echo(f"{action} configuration at {manager.path}.")
    typer.echo(f"Initialized {configuration.backend} schema.")
