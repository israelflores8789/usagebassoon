# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_duckdb.py — Tests for local DuckDB and offline MotherDuck validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.backends.motherduck import MotherDuckBackend
from usagebassoon.ingest import CollectionBundle


def test_local_backend_applies_current_duckdb_schema(tmp_path: Path) -> None:
    from usagebassoon.storage_model import SNAPSHOT_TABLES

    backend = DuckDBBackend(tmp_path / "deep" / "stats.duckdb")
    try:
        backend.apply_ddl()
        backend.apply_ddl()
        backend.preflight()
        tables = set(
            backend.query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE'"
            )
            .column("table_name")
            .to_pylist()
        )
        assert tables == set(SNAPSHOT_TABLES) | {"schema_marker", "schema_migrations"}
        assert "daily_activity" not in tables
        assert "source_leases" not in tables
    finally:
        backend.close()


def test_local_backend_merges_current_state_in_place(
    collection_bundle: CollectionBundle,
) -> None:
    from dataclasses import replace
    from datetime import timedelta
    from uuid import uuid4

    from usagebassoon.normalizer import normalize
    from usagebassoon.persistence import persist_run

    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        first = normalize(collection_bundle)
        persist_run(backend, first)
        assert persist_run(backend, first).inserted == 0
        later = normalize(
            replace(
                collection_bundle,
                run_id=str(uuid4()),
                started_at=collection_bundle.started_at + timedelta(hours=1),
            )
        )
        persist_run(backend, later)
        assert (
            backend.query("SELECT * FROM daily_stats").num_rows
            == first.tables["daily_stats"].num_rows
        )
    finally:
        backend.close()


def test_motherduck_rejects_invalid_database_name() -> None:
    """Assert MotherDuck rejects empty and already-prefixed database names."""
    with pytest.raises(ValueError, match="database name"):
        MotherDuckBackend("")
    with pytest.raises(ValueError, match="database name"):
        MotherDuckBackend("md:usagebassoon")


def test_motherduck_requires_token_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assert a missing MotherDuck token fails without contacting the service."""
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="MOTHERDUCK_TOKEN"):
        MotherDuckBackend("usagebassoon")


def test_local_backend_satisfies_storage_protocol() -> None:
    """Assert the local implementation exposes the Arrow StorageBackend API."""

    def accept(backend: StorageBackend) -> None:
        """Accept a structurally conforming storage backend."""
        assert callable(backend.apply_ddl)

    backend = DuckDBBackend(":memory:")
    try:
        accept(backend)
    finally:
        backend.close()
