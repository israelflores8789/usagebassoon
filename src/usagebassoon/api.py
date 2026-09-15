# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""api.py — Public Python API for querying UsageBassoon data."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from usagebassoon.backends.base import StorageBackend
from usagebassoon.config import ConfigurationManager, open_backend
from usagebassoon.frames import Engine, query_frame
from usagebassoon.sql_safety import dialect_for_backend, validate_read_only_sql

__all__ = ["connect", "query", "query_arrow"]


def connect(config: str | Path | None = None) -> StorageBackend:
    """Open the configured UsageBassoon storage backend.

    The returned backend owns its connection and must be closed by the caller.
    It is intentionally not initialized here: use ``bassoon init`` (or
    ``backend.apply_ddl()``) to create the schema before querying a new store.

    Args:
        config: Optional configuration file path. When omitted, the
            ``USAGEBASSOON_CONFIG`` environment variable and then the default
            configuration path are used.

    Returns:
        An open storage backend.

    Raises:
        ConfigurationError: If configuration is absent or invalid.
        OSError: If the selected backend cannot be opened.
        RuntimeError: If a remote backend cannot authenticate or connect.
        ValueError: If backend-specific configuration is invalid.
    """
    config_path = Path(config) if config is not None else None
    configuration = ConfigurationManager(config_path).load()
    return open_backend(configuration)


def query_arrow(
    sql: str,
    *,
    config: str | Path | None = None,
) -> pa.Table:
    """Execute read-only SQL and return the result as an Arrow table.

    This function opens and closes a backend for one query. Use ``connect``
    when multiple queries should share one connection.

    Args:
        sql: One read-only SELECT or WITH query in the configured dialect.
        config: Optional configuration file path.

    Returns:
        The query result as a ``pyarrow.Table``.
    """
    config_path = Path(config) if config is not None else None
    configuration = ConfigurationManager(config_path).load()
    validate_read_only_sql(sql, dialect=dialect_for_backend(configuration.backend))
    backend = open_backend(configuration)
    try:
        return backend.query(sql)
    finally:
        backend.close()


def query(
    sql: str,
    *,
    engine: Engine = "pandas",
    config: str | Path | None = None,
) -> object:
    """Execute read-only SQL and return a pandas or Polars DataFrame.

    Args:
        sql: One read-only SELECT or WITH query in the configured dialect.
        engine: Result frame library, either ``"pandas"`` (the default) or
            ``"polars"``.
        config: Optional configuration file path.

    Returns:
        A DataFrame from the requested library.

    Raises:
        ImportError: If Polars is requested but is not installed.
        ValueError: If ``engine`` is unsupported.
    """
    config_path = Path(config) if config is not None else None
    configuration = ConfigurationManager(config_path).load()
    validate_read_only_sql(sql, dialect=dialect_for_backend(configuration.backend))
    backend = open_backend(configuration)
    try:
        return query_frame(backend, sql, engine=engine)
    finally:
        backend.close()
