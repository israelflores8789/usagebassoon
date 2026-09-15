# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""init.py — Typer command to initialize UsageBassoon configuration and schema."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.config import (
    ConfigurationError,
    ConfigurationManager,
    open_backend,
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
) -> None:
    """Create configuration when absent, then apply the configured schema."""
    manager = ConfigurationManager(config)
    try:
        created = write_initial_config(manager.path)
        configuration = manager.load()
        backend = open_backend(configuration)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    try:
        backend.apply_ddl()
    finally:
        backend.close()
    action = "Created" if created else "Using existing"
    typer.echo(f"{action} configuration at {manager.path}.")
    typer.echo(f"Initialized {configuration.backend} schema.")
