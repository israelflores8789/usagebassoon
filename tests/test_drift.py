# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""Tests for persisted drift queries and read-only health diagnostics."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pyarrow as pa

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.drift import (
    format_checks,
    ingest_issues,
    run_doctor,
    unresolved_schema_drift,
)


def test_unresolved_schema_drift_returns_newest_events_first() -> None:
    """Load only unresolved events and preserve their diagnostic fields."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        now = datetime.now(UTC)
        backend.append(
            "schema_drift",
            pa.table(
                {
                    "drift_id": ["older", "newer"],
                    "run_id": [str(uuid4()), str(uuid4())],
                    "detected_at": [now, now],
                    "payload_kind": ["models", "pricing"],
                    "drift_kind": ["unknown_field", "type_change"],
                    "path": ["future", "resolution.price"],
                    "detail": ["additive", "changed"],
                    "tokscale_ver": ["4.15.1", "4.15.2"],
                    "resolved": [True, False],
                }
            ),
        )
        records = unresolved_schema_drift(backend)
        assert len(records) == 1
        assert records[0].drift_id == "newer"
        assert records[0].drift_kind == "type_change"
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
            "schema_drift",
            pa.table(
                {
                    "drift_id": ["drift-1"],
                    "run_id": [run_id],
                    "detected_at": [now],
                    "payload_kind": ["models"],
                    "drift_kind": ["unknown_field"],
                    "path": ["futureMetric"],
                    "detail": ["types int; tolerated"],
                    "tokscale_ver": ["4.15.1"],
                    "resolved": [False],
                }
            ),
        )
        backend.append(
            "ingest_runs",
            pa.table(
                {
                    "run_id": [run_id],
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
            "SELECT count(*) AS count FROM schema_drift"
        ).to_pylist() == [{"count": 1}]
        assert format_checks(report.checks)[0].startswith("OK configuration:")
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
