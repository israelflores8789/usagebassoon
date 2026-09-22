# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_utils.py — Shared configured-backend and snapshot helpers for CLI commands."""

from __future__ import annotations

from pathlib import Path

import typer

from usagebassoon.archiver import SnapshotArchiver
from usagebassoon.backends.base import StorageBackend, close_backend
from usagebassoon.config import (
    ConfigurationError,
    ConfigurationManager,
    UsageBassoonConfig,
    open_backend,
)


def configured_backend(
    config: Path | None,
) -> tuple[UsageBassoonConfig, StorageBackend]:
    """Open and initialize the backend selected by one configuration path.

    Args:
        config: Explicit configuration path, when supplied.

    Returns:
        Configuration and initialized backend.
    """
    backend: StorageBackend | None = None
    try:
        configuration = ConfigurationManager(config).load()
        backend = open_backend(configuration)
        backend.apply_ddl()
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        if backend is not None:
            close_backend(backend, context="backend initialization")
        raise typer.BadParameter(str(error), param_hint="--config") from error
    assert backend is not None
    return configuration, backend


def snapshot_archiver(configuration: UsageBassoonConfig) -> SnapshotArchiver:
    """Create the configured snapshot archive destinations.

    Args:
        configuration: Loaded UsageBassoon configuration.

    Returns:
        Snapshot archive with configured retention and destinations.
    """
    return SnapshotArchiver.from_config(configuration)
