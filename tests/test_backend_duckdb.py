# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_duckdb.py — Tests for local DuckDB and offline MotherDuck validation."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.backends.motherduck import MotherDuckBackend


def test_local_backend_applies_current_duckdb_schema(tmp_path: Path) -> None:
    """Assert the local backend applies the DDL currently shipped for DuckDB."""
    backend = DuckDBBackend(tmp_path / "deep" / "stats.duckdb")
    try:
        backend.apply_ddl()
        tables = (
            backend.query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            )
            .column("table_name")
            .to_pylist()
        )
        assert tables == [
            "daily_activity",
            "daily_cost",
            "daily_stats",
            "ingest_runs",
            "ingest_status",
            "noted_sessions",
            "notes",
            "price_versions",
            "reconciliation_issues",
            "report_daily_usage",
            "report_models",
            "report_session_models",
            "report_summary",
            "report_summary_models",
            "schema_drift_events",
            "session_model_stats",
            "session_model_stats_current",
            "session_notes",
            "session_tags",
            "sessions",
            "tagged_sessions",
            "tags",
        ]
        columns = backend.query("DESCRIBE sessions").column("column_name").to_pylist()
        assert columns[-3:] == ["first_seen_at", "last_seen_at", "updated_at"]
        reconciliation_columns = (
            backend.query("DESCRIBE reconciliation_issues")
            .column("column_name")
            .to_pylist()
        )
        assert reconciliation_columns[-2:] == ["resolved", "observation_count"]
        run_columns = (
            backend.query("DESCRIBE ingest_runs").column("column_name").to_pylist()
        )
        assert run_columns[4:11] == [
            "host",
            "os_name",
            "os_version",
            "architecture",
            "cpu_model",
            "cpu_count",
            "memory_bytes",
        ]
    finally:
        backend.close()


def test_local_backend_merges_current_state_in_place() -> None:
    """Assert a changed fact updates its DDL primary-key row rather than appending."""
    backend = DuckDBBackend(":memory:")
    first_updated_at = datetime(2026, 9, 14, tzinfo=UTC)
    second_updated_at = datetime(2026, 9, 15, tzinfo=UTC)
    try:
        backend.apply_ddl()
        first = pa.table(
            {
                "source_id": ["source"],
                "day": [date(2026, 9, 14)],
                "intensity": [1],
                "active_time_ms": [100],
                "updated_at": [first_updated_at],
            }
        )
        changed = first.set_column(2, "intensity", pa.array([2])).set_column(
            4,
            "updated_at",
            pa.array([second_updated_at]),
        )
        first_result = backend.upsert(
            "daily_activity",
            first,
            ("source_id", "day"),
            ("intensity",),
        )
        assert first_result.affected == 1
        unchanged_result = backend.upsert(
            "daily_activity",
            first,
            ("source_id", "day"),
            ("intensity",),
        )
        assert unchanged_result.affected == 0
        assert (
            backend.upsert(
                "daily_activity",
                changed,
                ("source_id", "day"),
                ("intensity",),
            ).updated
            == 1
        )
        assert backend.query(
            "SELECT intensity, updated_at FROM daily_activity"
        ).to_pylist() == [{"intensity": 2, "updated_at": second_updated_at}]
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
