# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""collect.py — Typer command for one tokscale collection and merge cycle."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.cli.spinner import spinner
from usagebassoon.collection_lock import CollectionBusy
from usagebassoon.config import ConfigurationError, ConfigurationManager
from usagebassoon.logger import LOGGER_NAME
from usagebassoon.orchestrator import collect as collect_run

_LOG = logging.getLogger(LOGGER_NAME)
collect_app = typer.Typer(invoke_without_command=True)


@collect_app.callback()
def collect(
    ctx: typer.Context,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Collect daily tokscale observations and publish them to storage."""
    ctx.obj = config
    if ctx.invoked_subcommand is None:
        _run_collection(config)


@collect_app.command()
def refresh(
    ctx: typer.Context,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    since: Annotated[
        str | None, typer.Option("--since", help="Inclusive YYYY-MM-DD start.")
    ] = None,
    until: Annotated[
        str | None, typer.Option("--until", help="Inclusive YYYY-MM-DD end.")
    ] = None,
) -> None:
    """Refresh this source's usage; default to 30 days ending today (UTC).

    Retain absent usage keys and existing historical prices. Use refresh after
    imports, scope changes, or collector corrections beyond the overlap window.
    """
    dates: list[date | None] = []
    for value, option in ((since, "--since"), (until, "--until")):
        if value is None:
            dates.append(None)
            continue
        try:
            parsed = date.fromisoformat(value)
            if parsed.isoformat() != value:
                raise ValueError("date must use YYYY-MM-DD")
        except ValueError as error:
            raise typer.BadParameter(
                "expected YYYY-MM-DD", param_hint=option
            ) from error
        dates.append(parsed)
    parent_config = ctx.obj
    selected_config = config or (
        parent_config if isinstance(parent_config, Path) else None
    )
    _run_collection(selected_config, refresh=True, since=dates[0], until=dates[1])


def _run_collection(
    config: Path | None,
    *,
    refresh: bool = False,
    since: date | None = None,
    until: date | None = None,
) -> None:
    """Run ordinary collection or refresh with shared error and result rendering."""
    try:
        configuration = ConfigurationManager(config).load()
        with spinner(configuration):
            run_id, summary = (
                collect_run(configuration, refresh=True, since=since, until=until)
                if refresh
                else collect_run(configuration)
            )
    except CollectionBusy as error:
        typer.echo(f"Collection failed: {error}", err=True)
        raise typer.Exit(code=1) from error
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        if isinstance(error, (OSError, RuntimeError)):
            typer.echo(
                "Warning: collection did not complete. Resolve the reported failure "
                "and run bassoon collect again to retry missing data.",
                err=True,
            )
        hint = (
            "--since/--until"
            if refresh and isinstance(error, ValueError)
            else "--config"
        )
        raise typer.BadParameter(str(error), param_hint=hint) from error
    except Exception as error:
        _LOG.exception("unexpected collection command failure")
        typer.echo(
            "Collection failed unexpectedly; see the operational log.",
            err=True,
        )
        raise typer.Exit(code=1) from error
    if not run_id:
        typer.echo("Collection skipped; see the operational log.", err=True)
    else:
        typer.echo(
            f"Collected run {run_id}: {summary.inserted} inserted, "
            f"{summary.updated} updated."
        )
        if summary.incomplete_targets:
            typer.echo(
                "Warning: collection is incomplete; failed targets were recorded "
                "for retry. Run bassoon collect again to retry missing data. "
                "See bassoon doctor and the operational log for details.",
                err=True,
            )
