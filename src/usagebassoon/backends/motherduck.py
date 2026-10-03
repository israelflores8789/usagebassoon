# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""motherduck.py — MotherDuck StorageBackend connection and authentication."""

from __future__ import annotations

import os
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import override
from urllib.parse import quote

import duckdb
import pyarrow as pa

from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import _DuckDBStorage


class MotherDuckBackend(_DuckDBStorage):
    """StorageBackend backed by a MotherDuck database.

    MotherDuck uses the DuckDB SQL schema but has distinct credential and URI
    handling, which is kept here rather than in the local DuckDB module.
    """

    @contextmanager
    @override
    def consistent_read(self) -> Generator[StorageBackend]:
        """Pin the remote database before yielding transactional diagnostic reads."""
        with self.transaction():
            # Constant queries can execute locally without starting a remote snapshot.
            self.query("SELECT version FROM schema_marker LIMIT 1")
            yield self

    @property
    @override
    def max_concurrent_queries(self) -> int:
        """Keep independent reads sequential on the owned DuckDB connection."""
        return 1

    @override
    def compaction_backlog(self) -> pa.Table | None:
        """Return None because transactional upserts require no compaction."""
        return None

    @override
    def prepare_recovery(self, *, notice: Callable[[str], None] | None = None) -> None:
        """MotherDuck transactional upserts have no scheduled compaction."""
        del notice

    @override
    def snapshot_provenance(self) -> dict[str, object]:
        """Identify MotherDuck while recording its shared DuckDB SQL contract."""
        return {**super().snapshot_provenance(), "source_backend": "motherduck"}

    @override
    def configure_maintenance(self, *, enabled: bool) -> str | None:
        """Explicitly report that MotherDuck requires no native maintenance."""
        del enabled
        return None

    @override
    def maintenance_status(self) -> tuple[bool, str] | None:
        """MotherDuck transactional upserts need no scheduled maintenance."""
        return None

    @override
    def restore_stages(self) -> list[dict[str, object]]:
        """MotherDuck restores directly in a transaction without staging."""
        return []

    @override
    def cleanup_restore_stages(self) -> None:
        """MotherDuck creates no persistent restore stages."""

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
