# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""init.py — Typer command to initialize UsageBassoon configuration and schema."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.backends.base import close_backend
from usagebassoon.cli.spinner import spinner
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
        with spinner(configuration):
            backend = open_backend(configuration, initialize=True)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    schedule = None
    try:
        with spinner(configuration):
            backend.apply_ddl()
            if configuration.backend == "bigquery":
                from usagebassoon.backends.bigquery import BigQueryBackend
                from usagebassoon.backends.bigquery_compaction import install_compaction

                if not isinstance(backend, BigQueryBackend):
                    raise RuntimeError(
                        "configured BigQuery backend has an invalid type"
                    )
                schedule = install_compaction(backend)
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    finally:
        close_backend(backend, context="initializing the schema")
    if schedule is not None:
        typer.echo(f"Nightly compaction scheduled at 02:00 UTC: {schedule}")
    action = "Created" if created else "Using existing"
    typer.echo(f"{action} configuration at {manager.path}.")
    typer.echo(f"Initialized {configuration.backend} schema.")
