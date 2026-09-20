# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""schedule.py — Typer commands for native and container scheduling."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, NoReturn

import typer

from usagebassoon.collector import preflight_tokscale
from usagebassoon.config import (
    ConfigurationError,
    ConfigurationManager,
    update_schedule_interval,
)
from usagebassoon.scheduling import (
    SchedulingError,
    human_status,
    install_native_schedule,
    native_schedule_status,
    remove_native_schedule,
    run_worker,
    schedule_logs,
    scheduler_availability,
    start_native_schedule,
    start_worker,
    status_json,
    stop_native_schedule,
    stop_worker,
    worker_status,
)

schedule_app = typer.Typer(
    help="Install and manage systemd, launchd, or container collection schedules."
)


def _schedule_error(error: Exception) -> NoReturn:
    """Render one schedule error and exit with a shell-friendly status."""
    typer.echo(f"Schedule operation failed: {error}", err=True)
    raise typer.Exit(code=1) from error


@schedule_app.command()
def install(
    interval: Annotated[
        str | None,
        typer.Option(
            "--interval",
            "-i",
            help="Persist the collection interval in config.toml before installation.",
        ),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    no_linger: Annotated[
        bool,
        typer.Option(
            "--no-linger",
            help="Linux only: do not attempt to enable systemd user lingering.",
        ),
    ] = False,
) -> None:
    """Install and enable the native collection schedule."""
    manager = ConfigurationManager(config)
    try:
        configuration = manager.load(schedule_interval=interval)
        if interval is not None:
            availability = scheduler_availability()
            if not availability.available:
                raise SchedulingError(availability.detail)
            preflight_tokscale(configuration)
            update_schedule_interval(manager.path, configuration.schedule.interval)
            configuration = manager.load()
        status = install_native_schedule(configuration, no_linger=no_linger)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        _schedule_error(error)
    typer.echo(f"Installed {status.provider} schedule.")
    typer.echo(f"Interval: {status.interval}")
    typer.echo(f"Artifact: {status.artifact}")
    for detail in status.details:
        if not (
            detail.startswith("could not enable")
            or detail.startswith("loginctl was not found")
        ):
            continue
        typer.echo(f"Warning: {detail}", err=True)


@schedule_app.command()
def status(
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Render machine-readable JSON output."),
    ] = False,
) -> None:
    """Show the installed native scheduler state without changing it."""
    try:
        configuration = ConfigurationManager(config).load()
        result = native_schedule_status(configuration)
        worker = worker_status(configuration)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        _schedule_error(error)
    typer.echo(status_json(result, worker) if as_json else human_status(result, worker))


@schedule_app.command()
def start(
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Start an installed native schedule after tokscale preflight."""
    try:
        result = start_native_schedule(ConfigurationManager(config).load())
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        _schedule_error(error)
    typer.echo(f"Started {result.provider} schedule.")


@schedule_app.command()
def stop(
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Stop native scheduling and any detached UsageBassoon worker."""
    worker_was_running = False
    try:
        configuration = ConfigurationManager(config).load()
        worker_was_running = worker_status(configuration).running
        stop_worker(configuration)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        _schedule_error(error)
    try:
        stop_native_schedule()
    except (OSError, RuntimeError, ValueError) as error:
        if not worker_was_running:
            _schedule_error(error)
        typer.echo(f"Warning: native scheduler was not stopped: {error}", err=True)
    typer.echo("Stopped collection schedule.")


@schedule_app.command()
def logs(
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Show recent worker or native scheduler output."""
    try:
        configuration = ConfigurationManager(config).load()
        typer.echo(schedule_logs(configuration))
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        _schedule_error(error)


@schedule_app.command()
def remove() -> None:
    """Stop a native schedule and remove its generated artifacts."""
    try:
        remove_native_schedule()
    except (OSError, RuntimeError, ValueError) as error:
        _schedule_error(error)
    typer.echo("Removed collection schedule artifacts.")


@schedule_app.command()
def worker(
    interval: Annotated[
        str | None,
        typer.Option(
            "--interval",
            "-i",
            help="Persist the collection interval in config.toml before starting.",
        ),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
    foreground: Annotated[
        bool,
        typer.Option(
            "--foreground",
            help="Run in the current process; use this as the container main process.",
        ),
    ] = False,
) -> None:
    """Start the self-contained worker for interactive or container use."""
    try:
        if foreground:
            run_worker(config, schedule_interval=interval)
            return
        result = start_worker(config, schedule_interval=interval)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        _schedule_error(error)
    if not result.running:
        _schedule_error(
            RuntimeError(
                f"schedule worker exited during startup; inspect {result.log_path}"
            )
        )
    typer.echo(
        f"UsageBassoon schedule worker is running in the background (pid {result.pid})."
    )
    typer.echo(f"Interval: {result.interval}")
    typer.echo(f"Logs: {result.log_path}")
