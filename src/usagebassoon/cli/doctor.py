# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""Typer command for read-only UsageBassoon health diagnostics."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, cast

import typer
from rich.console import Console
from rich.text import Text

from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.backends.motherduck import MotherDuckBackend
from usagebassoon.drift import DoctorReport, run_doctor

DEFAULT_CONFIG = Path("~/.config/usagebassoon/config.toml")
SUPPORTED_BACKENDS = frozenset({"duckdb", "motherduck", "bigquery"})


@dataclass(frozen=True, slots=True)
class _DoctorSettings:
    """Configuration values needed to construct a diagnostic backend."""

    backend: str | None = None
    database: str | None = None
    project: str | None = None
    location: str = "US"
    snapshots_configured: bool = False


def _table(value: object | None, name: str) -> dict[str, object]:
    """Return a TOML table or an empty mapping when it is absent."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"[{name}] must be a TOML table")
    return cast(dict[str, object], value)


def _read_settings(path: Path) -> tuple[_DoctorSettings, str | None, bool]:
    """Read the proposed UsageBassoon TOML configuration.

    Args:
        path: Configuration file to inspect.

    Returns:
        Settings, a parse/shape error if present, and whether the file exists.
    """
    expanded = path.expanduser()
    if not expanded.exists():
        return _DoctorSettings(), None, False
    try:
        payload = cast(dict[str, object], tomllib.loads(expanded.read_text()))
        snapshots = _table(payload.get("snapshots"), "snapshots")
        backend = payload.get("backend")
        database = payload.get("database")
        project = _table(payload.get("bigquery"), "bigquery").get("project")
        location = _table(payload.get("bigquery"), "bigquery").get("location", "US")
        for name, value in (
            ("backend", backend),
            ("database", database),
            ("bigquery.project", project),
            ("bigquery.location", location),
        ):
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{name} must be a string")
        return (
            _DoctorSettings(
                backend=cast(str | None, backend),
                database=cast(str | None, database),
                project=cast(str | None, project),
                location=cast(str, location),
                snapshots_configured=bool(snapshots),
            ),
            None,
            True,
        )
    except (OSError, tomllib.TOMLDecodeError, TypeError, ValueError) as error:
        return _DoctorSettings(), f"could not read configuration: {error}", True


def _open_backend(
    backend_name: str,
    database: str,
    project: str | None,
    location: str,
) -> StorageBackend:
    """Construct the configured backend without applying or changing schema."""
    if backend_name == "duckdb":
        return DuckDBBackend(database)
    if backend_name == "motherduck":
        return MotherDuckBackend(database)
    if backend_name == "bigquery":
        if not project:
            raise ValueError("bigquery.project is required for the BigQuery backend")
        from usagebassoon.backends.bigquery import BigQueryBackend

        return BigQueryBackend(project, database, location=location)
    raise ValueError(f"unsupported backend {backend_name!r}")


def _print_report(report: DoctorReport) -> None:
    """Render a structured doctor report with Rich."""
    console = Console()
    styles = {"ok": "green", "warning": "yellow", "error": "red"}
    for check in report.checks:
        line = Text(f"{check.status.upper()} ", style=styles[check.status])
        line.append(f"{check.name}: {check.message}")
        console.print(line)
        for detail in check.details:
            console.print(f"  - {detail}", markup=False)
    console.print(f"\nOverall status: {report.status}")


def doctor(
    config: Annotated[
        Path,
        typer.Option("--config", help="TOML configuration file to inspect."),
    ] = DEFAULT_CONFIG,
    backend: Annotated[
        str | None,
        typer.Option(
            "--backend",
            help="Override the configured backend: duckdb, motherduck, or bigquery.",
        ),
    ] = None,
    database: Annotated[
        str | None,
        typer.Option(
            "--database",
            help="Override the configured database, dataset, or DuckDB path.",
        ),
    ] = None,
    project: Annotated[
        str | None,
        typer.Option("--project", help="Override the GCP project for BigQuery."),
    ] = None,
    location: Annotated[
        str | None,
        typer.Option("--location", help="Override the BigQuery location."),
    ] = None,
    strict: Annotated[
        bool,
        typer.Option("--strict", help="Return failure when warnings are present, too."),
    ] = False,
    limit: Annotated[
        int,
        typer.Option(
            "--limit",
            min=1,
            help="Maximum unresolved drift and recent run records to display.",
        ),
    ] = 20,
) -> None:
    """Check configuration, connectivity, schema, drift, and ingest health."""
    settings, config_error, config_exists = _read_settings(config)
    backend_name = backend or settings.backend or ""
    database_name = database or settings.database
    project_name = project or settings.project
    location_name = location or settings.location
    if (
        config_error is None
        and not config_exists
        and not (backend_name and database_name)
    ):
        config_error = f"configuration file not found: {config.expanduser()}"
    if config_error is None and backend_name and backend_name not in SUPPORTED_BACKENDS:
        config_error = f"unsupported backend {backend_name!r}"
    opened: StorageBackend | None = None
    connection_error: str | None = None
    if config_error is None and backend_name and database_name:
        try:
            opened = _open_backend(
                backend_name,
                database_name,
                project_name,
                location_name,
            )
        except Exception as error:
            connection_error = str(error)
    report = run_doctor(
        opened,
        backend_name=backend_name,
        database=database_name,
        config_path=str(config.expanduser()) if config_exists else None,
        config_error=config_error,
        connection_error=connection_error,
        snapshot_enabled=settings.snapshots_configured,
        limit=limit,
    )
    try:
        _print_report(report)
    finally:
        if opened is not None:
            opened.close()
    if report.exit_code(strict=strict):
        raise typer.Exit(code=1)
