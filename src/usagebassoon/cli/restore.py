# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""restore.py — Typer command for restoring a snapshot into an empty store."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.cli._utils import configured_backend, snapshot_store
from usagebassoon.snapshots import SNAPSHOT_TABLES


def restore(
    snapshot: Annotated[
        str,
        typer.Option("--from-snapshot", help="Snapshot stamp or latest."),
    ] = "latest",
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Restore a raw snapshot into an empty configured warehouse."""
    configuration, backend = configured_backend(config)
    try:
        populated = []
        for table in SNAPSHOT_TABLES:
            count = backend.query(f"SELECT count(*) AS count FROM {table}").to_pylist()[
                0
            ]["count"]
            if count:
                populated.append(table)
        if populated:
            raise typer.BadParameter(
                "restore requires an empty warehouse; populated tables: "
                + ", ".join(populated)
            )
        restored = snapshot_store(configuration).restore(backend, snapshot)
    finally:
        backend.close()
    details = ", ".join(f"{table}={count}" for table, count in restored.items())
    typer.echo(f"Restored {details}")
