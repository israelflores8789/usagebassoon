# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_source_leases.py — Source fencing and consistent warehouse reads."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import override
from uuid import uuid4

import pyarrow as pa
import pytest

from usagebassoon.backends.base import (
    PersistenceBatch,
    SourceLeaseBusy,
    SourceLeaseLost,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.config import UsageBassoonConfig
from usagebassoon.source_leases import source_lease


def test_one_active_collection_per_source_across_connections(tmp_path: Path) -> None:
    """Reject a second same-source claim while another environment collects."""
    database = tmp_path / "warehouse.duckdb"
    source_id = str(uuid4())
    other_source = str(uuid4())
    config = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=source_id,
        backend="duckdb",
        local_database=database,
    )
    logger = logging.getLogger("test-source-leases")
    with source_lease(config, str(uuid4()), logger) as first:
        first.check()
        with pytest.raises(SourceLeaseBusy), source_lease(config, str(uuid4()), logger):
            pytest.fail("a live source was claimed twice")
        other = DuckDBBackend(database)
        try:
            other.ensure_source_lease(other_source)
            token = other.claim_source_lease(other_source, str(uuid4()), str(uuid4()))
            assert token is not None
            other.release_source_lease(token)
        finally:
            other.close()
    with source_lease(config, str(uuid4()), logger) as next_claim:
        assert next_claim.token.fence > first.token.fence


def test_stale_fence_cannot_commit_a_batch(tmp_path: Path) -> None:
    """Reject writes from an owner replaced after warehouse lease expiry."""
    database = tmp_path / "warehouse.duckdb"
    backend = DuckDBBackend(database)
    try:
        backend.apply_ddl()
        source_id = str(uuid4())
        backend.ensure_source_lease(source_id)
        first_run = str(uuid4())
        stale = backend.claim_source_lease(source_id, first_run, str(uuid4()))
        assert stale is not None
        backend.connection.execute(
            "UPDATE source_leases SET lease_expires_at = "
            "CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE source_id = ?",
            [source_id],
        )
        successor = DuckDBBackend(database)
        try:
            current = successor.claim_source_lease(
                source_id, str(uuid4()), str(uuid4())
            )
            assert current is not None
            assert current.fence > stale.fence
            batch = PersistenceBatch(
                run_id=first_run,
                current_state=(),
                append_only={},
                ingest_runs=pa.table({"run_id": [first_run], "source_id": [source_id]}),
                lease=stale,
            )
            with pytest.raises(SourceLeaseLost):
                backend.persist_batch(batch)
            assert backend.query(
                "SELECT COUNT(*) AS n FROM ingest_runs"
            ).to_pylist() == [{"n": 0}]
            backend.release_source_lease(stale)
            assert successor.renew_source_lease(current)
        finally:
            successor.close()
    finally:
        backend.close()


def test_duckdb_snapshot_reads_one_transaction_state(tmp_path: Path) -> None:
    """Keep later table reads at the state established before a concurrent write."""
    database = tmp_path / "warehouse.duckdb"
    writer = DuckDBBackend(database)
    writer.apply_ddl()

    class InterleavedReader(DuckDBBackend):
        """Commit a competing note after the first snapshot table is read."""

        reads = 0

        @override
        def query(
            self, sql: str, parameters: Mapping[str, str] | None = None
        ) -> pa.Table:
            """Interleave one independent commit between snapshot table reads."""
            result = super().query(sql, parameters)
            self.reads += 1
            if self.reads == 1:
                stamp = datetime.now(UTC)
                writer.append(
                    "notes",
                    pa.table(
                        {
                            "source_id": [str(uuid4())],
                            "client": ["codex"],
                            "session_id": ["session"],
                            "note": ["committed between reads"],
                            "created_at": [stamp],
                            "updated_at": [stamp],
                        }
                    ),
                )
            return result

    reader = InterleavedReader(database)
    try:
        snapshot = reader.read_snapshot_tables(("sessions", "notes"))
        assert snapshot.tables["sessions"].num_rows == 0
        assert snapshot.tables["notes"].num_rows == 0
        assert writer.query("SELECT * FROM notes").num_rows == 1
    finally:
        reader.close()
        writer.close()
