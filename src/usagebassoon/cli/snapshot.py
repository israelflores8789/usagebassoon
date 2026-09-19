# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""snapshot.py — Typer command for private raw warehouse snapshots."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.backends.base import close_backend
from usagebassoon.cli._utils import configured_backend, snapshot_store


def snapshot(
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Write a raw private restoration snapshot of the configured warehouse."""
    configuration, backend = configured_backend(config)
    try:
        store = snapshot_store(configuration)
        latest = backend.query(
            "SELECT run_id FROM ingest_runs ORDER BY finished_at DESC LIMIT 1"
        ).to_pylist()
        uri = store.write(
            backend,
            run_id=str(latest[0]["run_id"]) if latest else "manual",
        )
    finally:
        close_backend(backend, context="writing a snapshot")
    if uri is None:
        typer.echo(
            "Snapshot skipped: the configured interval is not due or is reserved."
        )
    else:
        destinations = ", ".join(store.destination_uris)
        typer.echo(f"Created private raw snapshot at {destinations}.")
