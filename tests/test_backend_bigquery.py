# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_bigquery.py — Offline BigQuery batch and schema unit tests."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast
from uuid import uuid4

import pyarrow as pa
from google.cloud import bigquery

from usagebassoon.backends.base import CurrentStateWrite, PersistenceBatch
from usagebassoon.backends.bigquery import BigQueryBackend, _schema_from_arrow


class _OfflineClient:
    """Minimal client placeholder used when only SQL generation is under test."""

    def close(self) -> None:
        """Satisfy the BigQuery client close surface."""


class _Job:
    """Minimal completed BigQuery job with a deterministic row result."""

    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        """Store rows returned when the fake job is awaited."""
        self._rows = rows or []

    def result(self) -> list[dict[str, object]]:
        """Return the completed job's query result rows."""
        return self._rows


class _BatchClient:
    """Offline client recording BigQuery batch transport operations."""

    def __init__(self) -> None:
        """Initialize recorded calls."""
        self.loads: list[tuple[str, bigquery.LoadJobConfig]] = []
        self.deleted: list[str] = []
        self.queries: list[str] = []

    def load_table_from_dataframe(
        self,
        _: object,
        destination: str,
        *,
        job_config: bigquery.LoadJobConfig,
        location: str,
    ) -> _Job:
        """Record one explicit-schema staging load."""
        assert location == "US"
        self.loads.append((destination, job_config))
        return _Job()

    def query(
        self,
        sql: str,
        *,
        job_config: bigquery.QueryJobConfig,
        location: str,
    ) -> _Job:
        """Return the script summary expected by one staged current table."""
        assert job_config.query_parameters
        assert location == "US"
        self.queries.append(sql)
        return _Job(
            [
                {
                    "already_committed": False,
                    "inserted_daily_activity": 1,
                    "updated_daily_activity": 0,
                }
            ]
        )

    def delete_table(self, table: str, *, not_found_ok: bool) -> None:
        """Record best-effort staging cleanup."""
        assert not_found_ok
        self.deleted.append(table)

    def close(self) -> None:
        """Satisfy the BigQuery client close surface."""


def _backend() -> BigQueryBackend:
    """Build a BigQuery backend without credentials or network access."""
    return BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon",
        client=cast(bigquery.Client, _OfflineClient()),
    )


def test_arrow_schema_mapping_is_explicit_and_preserves_logical_types() -> None:
    """Map canonical Arrow primitives and repeated strings without inference."""
    data = pa.table(
        {
            "name": ["session"],
            "count": [1],
            "cost": [1.25],
            "enabled": [True],
            "day": [date(2026, 9, 16)],
            "captured_at": [datetime(2026, 9, 16, tzinfo=UTC)],
            "models": [["gpt-5"]],
        }
    )
    fields = _schema_from_arrow(data)
    assert [(field.name, field.field_type, field.mode) for field in fields] == [
        ("name", "STRING", "NULLABLE"),
        ("count", "INT64", "NULLABLE"),
        ("cost", "FLOAT64", "NULLABLE"),
        ("enabled", "BOOL", "NULLABLE"),
        ("day", "DATE", "NULLABLE"),
        ("captured_at", "TIMESTAMP", "NULLABLE"),
        ("models", "STRING", "REPEATED"),
    ]


def test_batch_script_uses_run_scoped_staging_and_a_single_transaction() -> None:
    """Generate only trusted identifiers and one idempotent batch script."""
    backend = _backend()
    run_id = str(uuid4())
    current = pa.table(
        {
            "source_id": ["source"],
            "day": [date(2026, 9, 16)],
            "intensity": [1],
            "active_time_ms": [100],
            "last_updated_at": [datetime(2026, 9, 16, tzinfo=UTC)],
        }
    )
    batch = PersistenceBatch(
        run_id=run_id,
        current_state=(
            CurrentStateWrite(
                "daily_activity",
                current,
                ("source_id", "day"),
                ("intensity", "active_time_ms"),
            ),
        ),
        append_only={
            "run_metrics": pa.table({"run_id": [run_id], "source_id": ["source"]})
        },
        ingest_runs=pa.table(
            {"run_id": [run_id], "rows_inserted": [0], "rows_updated": [0]}
        ),
    )
    stages = {
        "daily_activity": backend._stage_ref("daily_activity", run_id),
        "run_metrics": backend._stage_ref("run_metrics", run_id),
        "ingest_runs": backend._stage_ref("ingest_runs", run_id),
    }
    script = backend._batch_script(batch, stages)
    assert "BEGIN TRANSACTION;" in script
    assert "COMMIT TRANSACTION;" in script
    assert "IF NOT already_committed THEN" in script
    assert "WHERE `run_id` = @run_id" in script
    assert run_id.replace("-", "") in stages["daily_activity"]
    assert "MERGE `usagebassoon-test.usagebassoon.daily_activity`" in script


def test_batch_persistence_loads_explicit_schemas_and_cleans_stages() -> None:
    """Use one explicit-schema load per table before running the batch script."""
    client = _BatchClient()
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon",
        client=cast(bigquery.Client, client),
    )
    run_id = str(uuid4())
    current = pa.table(
        {
            "source_id": ["source"],
            "day": [date(2026, 9, 16)],
            "intensity": [1],
            "active_time_ms": [100],
            "last_updated_at": [datetime(2026, 9, 16, tzinfo=UTC)],
        }
    )
    batch = PersistenceBatch(
        run_id=run_id,
        current_state=(
            CurrentStateWrite(
                "daily_activity",
                current,
                ("source_id", "day"),
                ("intensity", "active_time_ms"),
            ),
        ),
        append_only={
            "run_metrics": pa.table({"run_id": [run_id], "source_id": ["source"]})
        },
        ingest_runs=pa.table(
            {"run_id": [run_id], "rows_inserted": [0], "rows_updated": [0]}
        ),
    )
    result = backend.persist_batch(batch)
    assert (result.inserted, result.updated, result.already_committed) == (1, 0, False)
    assert len(client.loads) == 3
    assert len(client.deleted) == 3
    schema = client.loads[0][1].schema
    assert schema is not None
    assert schema[1].field_type == "DATE"
    assert client.queries[0].count("BEGIN TRANSACTION;") == 1
