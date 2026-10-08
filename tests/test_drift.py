# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_drift.py — Tests persisted drift queries and read-only health diagnostics."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import override
from unittest.mock import MagicMock
from uuid import uuid4

import pyarrow as pa
import pytest

from tests._observations import observations
from usagebassoon.backends.base import ActiveTransaction, StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.config import UsageBassoonConfig
from usagebassoon.diagnostics import (
    format_checks,
    ingest_issues,
    run_doctor,
    unresolved_schema_drift,
)
from usagebassoon.drift import SchemaDriftState
from usagebassoon.persistence import load_ingest_status

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def test_unresolved_schema_drift_returns_newest_events_first() -> None:
    """Filter resolved events and order remaining diagnostic records by freshness."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        now = datetime.now(UTC)
        backend.append(
            "schema_drift_events",
            observations(
                pa.table(
                    {
                        "source_id": ["source"] * 3,
                        "domain": ["models", "pricing", "report"],
                        "tokscale_ver": ["4.18.0", "4.18.1", "4.18.0"],
                        "drift_key": [
                            "unknown_field:future",
                            "type_change:resolution.price",
                            "unknown_field:extra",
                        ],
                        "drift_kind": ["unknown_field", "type_change", "unknown_field"],
                        "path": ["future", "resolution.price", "extra"],
                        "detail": ["additive", "changed", "additive report"],
                        "contract_tokscale_ver": ["4.18.0"] * 3,
                        "created_at": [now] * 3,
                        "collected_at": [
                            now + timedelta(seconds=2),
                            now,
                            now + timedelta(seconds=1),
                        ],
                        "run_id": [str(uuid4()) for _ in range(3)],
                        "resolved": [True, False, False],
                        "observation_count": [1, 3, 2],
                    }
                )
            ),
        )
        records = unresolved_schema_drift(backend)
        assert [record.domain for record in records] == ["report", "pricing"]
        assert records[1].drift_kind == "type_change"
        assert records[1].observation_count == 3
    finally:
        backend.close()


def test_run_doctor_reports_unresolved_state_without_mutating_backend() -> None:
    """Report drift and run issues while leaving persisted rows unchanged."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        run_id = str(uuid4())
        now = datetime.now(UTC)
        backend.append(
            "schema_drift_events",
            observations(
                pa.table(
                    {
                        "source_id": ["source"],
                        "domain": ["models"],
                        "tokscale_ver": ["4.18.0"],
                        "drift_key": ["unknown_field:futureMetric"],
                        "drift_kind": ["unknown_field"],
                        "path": ["futureMetric"],
                        "detail": ["types int; tolerated"],
                        "contract_tokscale_ver": ["4.18.0"],
                        "created_at": [now],
                        "collected_at": [now],
                        "run_id": [run_id],
                        "resolved": [False],
                        "observation_count": [1],
                    }
                )
            ),
        )
        backend.append(
            "collection_ledger",
            observations(
                pa.table(
                    {
                        "run_id": [run_id],
                        "source_id": ["source"],
                        "started_at": [now],
                        "finished_at": [now],
                        "host": ["pytest"],
                        "tokscale_ver": ["4.18.0"],
                        "status": ["schema_drift"],
                    }
                )
            ),
        )
        before = {
            table: backend.query(f"SELECT * FROM {table} ORDER BY event_id").to_pylist()
            for table in ("schema_drift_events", "collection_ledger")
        }
        report = run_doctor(
            backend,
            backend_name="duckdb",
            database=":memory:",
            snapshot_enabled=False,
        )
        assert before == {
            table: backend.query(f"SELECT * FROM {table} ORDER BY event_id").to_pylist()
            for table in before
        }
        assert report.status == "warning"
        assert not report.errors
        assert {check.name for check in report.warnings} == {
            "schema_drift",
            "collection_runs",
        }
        assert backend.query(
            "SELECT count(*) AS count FROM current_schema_drift_events"
        ).to_pylist() == [{"count": 1}]
        assert format_checks(report.checks)[0].startswith("OK configuration:")
    finally:
        backend.close()


def test_load_collection_status_returns_unresolved_drift_state(tmp_path: Path) -> None:
    """Load persisted event identity and first-detection metadata for rechecks."""
    database = tmp_path / "drift.duckdb"
    backend = DuckDBBackend(database)
    detected_at = datetime.now(UTC)
    detected_run_id = str(uuid4())
    try:
        backend.apply_ddl()
        backend.append(
            "schema_drift_events",
            observations(
                pa.table(
                    {
                        "source_id": [SOURCE_ID],
                        "domain": ["models"],
                        "tokscale_ver": ["4.18.1"],
                        "drift_key": ["unknown_field:future"],
                        "drift_kind": ["unknown_field"],
                        "path": ["future"],
                        "detail": ["types int; tolerated"],
                        "contract_tokscale_ver": ["4.18.0"],
                        "created_at": [detected_at],
                        "collected_at": [detected_at],
                        "run_id": [detected_run_id],
                        "resolved": [False],
                        "observation_count": [4],
                    }
                )
            ),
        )
    finally:
        backend.close()
    config = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=SOURCE_ID,
        backend="duckdb",
        local_database=database,
    )

    statuses, models, prices, reconciliation, schema_drift, _inventory = (
        load_ingest_status(config)
    )

    assert (statuses, models, prices, reconciliation) == ({}, {}, {}, frozenset())
    assert schema_drift == (
        SchemaDriftState(
            domain="models",
            tokscale_ver="4.18.1",
            drift_key="unknown_field:future",
            drift_kind="unknown_field",
            path="future",
            detail="types int; tolerated",
            contract_tokscale_ver="4.18.0",
            created_at=detected_at,
            observation_count=4,
        ),
    )


def test_run_doctor_reports_active_bigquery_transactions() -> None:
    """Surface active warehouse transactions in BigQuery diagnostics."""

    class _TransactionsBackend(DuckDBBackend):
        """DuckDB test double with BigQuery transaction diagnostics."""

        @override
        def active_transactions(self, limit: int) -> tuple[ActiveTransaction, ...]:
            """Return one active transaction while honoring the requested limit."""
            assert limit == 20
            return (ActiveTransaction("job-1", "transaction-1"),)

    backend = _TransactionsBackend(":memory:")
    try:
        backend.apply_ddl()
        report = run_doctor(
            backend,
            backend_name="bigquery",
            database="usagebassoon_it",
            snapshot_enabled=False,
        )
        transaction_check = next(
            check for check in report.checks if check.name == "transactions"
        )
        assert transaction_check.status == "warning"
        assert transaction_check.details == ("job job-1; transaction transaction-1",)
    finally:
        backend.close()


def test_ingest_issues_rejects_non_positive_limit() -> None:
    """Reject invalid diagnostic limits before issuing SQL."""
    backend = MagicMock(spec=StorageBackend)
    with pytest.raises(ValueError, match="limit must be positive"):
        ingest_issues(backend, limit=0)
    backend.query.assert_not_called()


@pytest.mark.parametrize("table", ["schema_drift_events", "reconciliation_issues"])
def test_doctor_hides_stale_events_without_pruning_and_allows_fresh_sightings(
    table: str,
) -> None:
    """Use latest observation freshness even after an idle local warehouse."""
    from usagebassoon.diagnostics import reconciliation_issues
    from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS

    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        now = datetime.now(UTC)
        schema = CANONICAL_TABLE_SCHEMAS[table]
        row: dict[str, object] = {field.name: "synthetic" for field in schema}
        row.update(
            event_id=str(uuid4()),
            created_at=now - timedelta(days=120),
            collected_at=now - timedelta(days=91),
            resolved=False,
            observation_count=1,
        )
        backend.append(table, pa.Table.from_pylist([row], schema=schema))
        read = (
            unresolved_schema_drift
            if table == "schema_drift_events"
            else reconciliation_issues
        )
        assert read(backend) == ()
        assert backend.query(f"SELECT * FROM {table}").num_rows == 1
        row.update(event_id=str(uuid4()), collected_at=now, observation_count=1)
        backend.append(table, pa.Table.from_pylist([row], schema=schema))
        assert len(read(backend)) == 1
        assert read(backend)[0].created_at == now - timedelta(days=120)
        row.update(
            event_id=str(uuid4()),
            collected_at=now + timedelta(seconds=1),
            resolved=True,
            observation_count=0,
        )
        backend.append(table, pa.Table.from_pylist([row], schema=schema))
        assert read(backend) == ()
    finally:
        backend.close()


@pytest.mark.parametrize("workers", [1, 2])
def test_doctor_uses_backend_capabilities_without_provider_selectors(
    workers: int,
) -> None:
    """Honor concurrency and compaction health for an unfamiliar backend name."""
    from collections.abc import Mapping
    from threading import Event, Lock

    import pyarrow as pa

    class _FutureBackend(DuckDBBackend):
        """Exercise the read contract without sharing a DuckDB query cursor."""

        def __init__(self) -> None:
            super().__init__(":memory:")
            self.lock = Lock()
            self.overlap = Event()
            self.inflight = 0
            self.peak = 0

        @property
        @override
        def max_concurrent_queries(self) -> int:
            return workers

        @override
        def query(
            self, sql: str, parameters: Mapping[str, str] | None = None
        ) -> pa.Table:
            del parameters
            if sql == "SELECT 1 AS doctor_ok" or "LIMIT 0" in sql:
                return pa.table({})
            with self.lock:
                self.inflight += 1
                self.peak = max(self.peak, self.inflight)
                if self.inflight == workers:
                    self.overlap.set()
            try:
                assert self.overlap.wait(5), "diagnostic queries did not overlap"
                if "open_schema_drift_events" in sql:
                    raise RuntimeError("drift read failed")
                return pa.table({})
            finally:
                with self.lock:
                    self.inflight -= 1

        @override
        def compaction_backlog(self) -> pa.Table | None:
            return pa.table(
                {
                    "domain": ["sessions", "daily_stats"],
                    "arrival_day": ["recent", "at-risk"],
                    "pending_rows": [1, 1],
                    "age_days": [2, 81],
                }
            )

    backend = _FutureBackend()
    try:
        report = run_doctor(
            backend,
            backend_name="future_backend",
            database="integration",
            snapshot_enabled=False,
            limit=1,
        )
        checks = {check.name: check for check in report.checks}
        assert backend.peak == workers
        assert checks["compaction"].status == "error"
        assert len(checks["compaction"].details) == 1
        assert checks["schema_drift"].status == "error"
        assert checks["collection_runs"].status == "ok"
        assert checks["reconciliation"].status == "ok"
    finally:
        backend.close()


def test_doctor_schema_fallback_identifies_missing_relation_for_any_backend() -> None:
    """Keep precise schema errors when the shared combined probe fails."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        backend.connection.execute("DROP VIEW report_models")
        report = run_doctor(
            backend,
            backend_name="unfamiliar_backend",
            database=":memory:",
            snapshot_enabled=False,
        )
        schema = next(check for check in report.checks if check.name == "schema")
        assert schema.status == "error"
        assert schema.details == ("report_models",)
    finally:
        backend.close()
