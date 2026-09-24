# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_drift.py — Tests persisted drift queries and read-only health diagnostics."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import override
from uuid import uuid4

import pyarrow as pa

from usagebassoon.backends.base import ActiveTransaction
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
    """Load only unresolved events and preserve their diagnostic fields."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        now = datetime.now(UTC)
        backend.append(
            "schema_drift_events",
            pa.table(
                {
                    "source_id": ["source", "source"],
                    "domain": ["models", "pricing"],
                    "tokscale_ver": ["4.15.1", "4.15.2"],
                    "drift_key": [
                        "unknown_field:future",
                        "type_change:resolution.price",
                    ],
                    "drift_kind": ["unknown_field", "type_change"],
                    "path": ["future", "resolution.price"],
                    "detail": ["additive", "changed"],
                    "contract_tokscale_ver": ["4.15.1", "4.15.1"],
                    "created_at": [now, now],
                    "updated_at": [now, now + timedelta(seconds=1)],
                    "detected_run_id": [str(uuid4()), str(uuid4())],
                    "updated_run_id": [str(uuid4()), str(uuid4())],
                    "resolved": [True, False],
                    "observation_count": [1, 3],
                }
            ),
        )
        records = unresolved_schema_drift(backend)
        assert len(records) == 1
        assert records[0].domain == "pricing"
        assert records[0].drift_kind == "type_change"
        assert records[0].observation_count == 3
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
            pa.table(
                {
                    "source_id": ["source"],
                    "domain": ["models"],
                    "tokscale_ver": ["4.15.1"],
                    "drift_key": ["unknown_field:futureMetric"],
                    "drift_kind": ["unknown_field"],
                    "path": ["futureMetric"],
                    "detail": ["types int; tolerated"],
                    "contract_tokscale_ver": ["4.15.1"],
                    "created_at": [now],
                    "updated_at": [now],
                    "detected_run_id": [run_id],
                    "updated_run_id": [run_id],
                    "resolved": [False],
                    "observation_count": [1],
                }
            ),
        )
        backend.append(
            "ingest_runs",
            pa.table(
                {
                    "run_id": [run_id],
                    "source_id": ["source"],
                    "started_at": [now],
                    "finished_at": [now],
                    "host": ["pytest"],
                    "tokscale_ver": ["4.15.1"],
                    "status": ["schema_drift"],
                    "rows_in": [1],
                    "rows_inserted": [0],
                    "rows_updated": [0],
                    "drift_events": [1],
                }
            ),
        )
        report = run_doctor(
            backend,
            backend_name="duckdb",
            database=":memory:",
            snapshot_enabled=False,
        )
        assert report.status == "warning"
        assert not report.errors
        assert {check.name for check in report.warnings} == {
            "schema_drift",
            "ingest_runs",
        }
        assert backend.query(
            "SELECT count(*) AS count FROM schema_drift_events"
        ).to_pylist() == [{"count": 1}]
        assert format_checks(report.checks)[0].startswith("OK configuration:")
    finally:
        backend.close()


def test_load_ingest_status_returns_unresolved_drift_state(tmp_path: Path) -> None:
    """Load persisted event identity and first-detection metadata for rechecks."""
    database = tmp_path / "drift.duckdb"
    backend = DuckDBBackend(database)
    detected_at = datetime.now(UTC)
    detected_run_id = str(uuid4())
    try:
        backend.apply_ddl()
        backend.append(
            "schema_drift_events",
            pa.table(
                {
                    "source_id": [SOURCE_ID],
                    "domain": ["models"],
                    "tokscale_ver": ["4.15.2"],
                    "drift_key": ["unknown_field:future"],
                    "drift_kind": ["unknown_field"],
                    "path": ["future"],
                    "detail": ["types int; tolerated"],
                    "contract_tokscale_ver": ["4.15.1"],
                    "created_at": [detected_at],
                    "updated_at": [detected_at],
                    "detected_run_id": [detected_run_id],
                    "updated_run_id": [detected_run_id],
                    "resolved": [False],
                    "observation_count": [4],
                }
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

    statuses, models, prices, reconciliation, schema_drift = load_ingest_status(config)

    assert (statuses, models, prices, reconciliation) == ({}, {}, {}, frozenset())
    assert schema_drift == (
        SchemaDriftState(
            domain="models",
            tokscale_ver="4.15.2",
            drift_key="unknown_field:future",
            drift_kind="unknown_field",
            path="future",
            detail="types int; tolerated",
            contract_tokscale_ver="4.15.1",
            created_at=detected_at,
            detected_run_id=detected_run_id,
            observation_count=4,
        ),
    )


def test_run_doctor_reports_active_bigquery_transactions() -> None:
    """Surface jobs that can delay a BigQuery collection transaction."""

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
    backend = DuckDBBackend(":memory:")
    try:
        try:
            ingest_issues(backend, limit=0)
        except ValueError as error:
            assert str(error) == "limit must be positive"
        else:
            raise AssertionError("expected ValueError")
    finally:
        backend.close()
