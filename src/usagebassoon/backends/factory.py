# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""factory.py — Lazy storage provider construction and schema-readiness checks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from usagebassoon.backends.base import (
    StorageBackend,
    close_backend,
)
from usagebassoon.deadlines import cleanup_budget, operation

if TYPE_CHECKING:
    from usagebassoon.config import UsageBassoonConfig

type BackendFactory = Callable[[UsageBassoonConfig], StorageBackend]


def _duckdb_backend(config: UsageBassoonConfig) -> StorageBackend:
    """Construct local DuckDB from its validated path without issuing DDL."""
    if config.local_database is None:
        raise ValueError("Local DuckDB path is missing")
    from usagebassoon.backends.duckdb_local import DuckDBBackend

    return DuckDBBackend(config.local_database)


def _motherduck_backend(config: UsageBassoonConfig) -> StorageBackend:
    """Construct MotherDuck with its configured database and ambient token."""
    if config.motherduck is None:
        raise ValueError("MotherDuck settings are missing")
    from usagebassoon.backends.motherduck import MotherDuckBackend

    return MotherDuckBackend(
        config.motherduck.database, timeout_seconds=config.motherduck.timeout_seconds
    )


def _bigquery_backend(config: UsageBassoonConfig) -> StorageBackend:
    """Apply explicit or ambient BigQuery authentication and request budgets."""
    if config.bigquery is None:
        raise ValueError("BigQuery settings are missing")
    from usagebassoon.backends.bigquery import BigQueryBackend

    settings = config.bigquery
    return BigQueryBackend(
        settings.project,
        settings.dataset,
        location=settings.location,
        credentials_file=settings.credentials_file,
        maximum_bytes_billed=settings.maximum_bytes_billed,
        timeout_seconds=settings.timeout_seconds,
    )


class StorageBackendRegistry:
    """Construct registered providers and enforce the shared backend-open contract."""

    def __init__(
        self, *, factories: Mapping[str, BackendFactory] | None = None
    ) -> None:
        """Register built-in providers and optional additional constructors."""
        self._factories: dict[str, BackendFactory] = {
            "duckdb": _duckdb_backend,
            "motherduck": _motherduck_backend,
            "bigquery": _bigquery_backend,
        }
        for provider, factory in (factories or {}).items():
            self.register(provider, factory)

    def register(self, provider: str, factory: BackendFactory) -> None:
        """Register a provider constructor without opening a connection."""
        if not provider or provider != provider.strip():
            raise ValueError("backend provider registration requires a nonempty name")
        self._factories[provider] = factory

    def open(
        self, config: UsageBassoonConfig, *, initialize: bool = False
    ) -> StorageBackend:
        """Open a provider and require readiness except for explicit provisioning."""
        factory = self._factories.get(config.backend)
        if factory is None:
            raise ValueError(
                f"unsupported storage backend provider: {config.backend!r}"
            )
        backend: StorageBackend | None = None
        try:
            with operation(config.backend_timeout_seconds):
                backend = factory(config)
                if not initialize:
                    backend.preflight()
                return backend
        except BaseException:
            if backend is not None:
                with cleanup_budget():
                    close_backend(backend, context="failed backend preflight")
            raise


def open_backend(
    config: UsageBassoonConfig,
    *,
    initialize: bool = False,
    registry: StorageBackendRegistry | None = None,
) -> StorageBackend:
    """Open a caller-owned backend through registered provider construction.

    Args:
        config: Validated settings, including provider authentication and limits.
        initialize: Skip schema preflight only for explicit provisioning.
        registry: Optional provider constructors for a library integration.

    Returns:
        An open storage backend owned by the caller.

    Raises:
        ValueError: If the provider is unsupported or its settings are absent.
    """
    return (registry or StorageBackendRegistry()).open(config, initialize=initialize)
