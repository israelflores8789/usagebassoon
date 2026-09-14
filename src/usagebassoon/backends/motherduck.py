# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""motherduck.py — MotherDuck StorageBackend connection and authentication."""

from __future__ import annotations

import os
from urllib.parse import quote

import duckdb

from usagebassoon.backends.duckdb_local import _DuckDBStorage


class MotherDuckBackend(_DuckDBStorage):
    """StorageBackend backed by a MotherDuck database.

    MotherDuck uses the DuckDB SQL schema but has distinct credential and URI
    handling, which is kept here rather than in the local DuckDB module.
    """

    def __init__(self, database: str, *, token: str | None = None) -> None:
        """Connect to a MotherDuck database.

        Args:
            database: MotherDuck database name without the ``md:`` prefix.
            token: Service token, or ``MOTHERDUCK_TOKEN`` when omitted.

        Raises:
            ValueError: If the database name is invalid.
            RuntimeError: If no token is configured.
        """
        if not database or database.startswith("md:"):
            raise ValueError("database must be a non-empty MotherDuck database name")
        resolved_token = token or os.environ.get("MOTHERDUCK_TOKEN")
        if not resolved_token:
            raise RuntimeError(
                "MOTHERDUCK_TOKEN is required for MotherDuck connections"
            )
        self.database = database
        uri = f"md:{database}?motherduck_token={quote(resolved_token, safe='')}"
        super().__init__(duckdb.connect(uri))
