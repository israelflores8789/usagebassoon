# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""collect.py — Typer command for one tokscale collection and merge cycle."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.collector import collect as collect_run
from usagebassoon.config import ConfigurationError, ConfigurationManager


def collect(
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Collect one cumulative tokscale state and merge it into storage."""
    try:
        configuration = ConfigurationManager(config).load()
        run_id, summary = collect_run(configuration)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    typer.echo(
        f"Collected run {run_id}: {summary.inserted} inserted, "
        f"{summary.updated} updated."
    )
