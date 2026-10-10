# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""frames.py — Single conversion point from Arrow query results to user DataFrames.

pandas and polars share one converter because the backend protocol returns
Arrow; polars is optional and raises a targeted ImportError when absent.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

if TYPE_CHECKING:
    from usagebassoon.backends.base import StorageBackend

Engine = Literal["pandas", "polars"]


def to_frame(table: pa.Table, engine: Engine = "pandas") -> object:
    """Convert an Arrow table to a pandas or polars DataFrame.

    Args:
        table: The Arrow result.
        engine: Target frame library.

    Returns:
        A DataFrame in the requested library.

    Raises:
        ValueError: Unknown engine.
        ImportError: polars requested but not installed.
    """
    if engine == "pandas":
        # Arrow tables have to_pandas(); planar DataFrames pass through
        return table.to_pandas() if hasattr(table, "to_pandas") else table
    if engine == "polars":
        try:
            polars = import_module("polars")
        except ModuleNotFoundError as exc:
            raise ImportError(
                "polars backend requested but polars is not installed; "
                "pip install 'usagebassoon[polars]'"
            ) from exc
        return polars.from_arrow(table)
    raise ValueError(f"unsupported engine {engine!r}")


def query_frame(
    backend: StorageBackend,
    sql: str,
    *,
    engine: Engine = "pandas",
) -> object:
    """Execute SQL on a backend and return the result as a DataFrame.

    Args:
        backend: Configured storage backend.
        sql: Dialect SQL.
        engine: pandas (default) or polars.

    Returns:
        A DataFrame in the requested library.
    """
    return to_frame(backend.query(sql), engine)
