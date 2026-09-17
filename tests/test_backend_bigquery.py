# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_bigquery.py — Offline BigQuery batch and schema unit tests."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast
from uuid import uuid4

import pyarrow as pa
from google.api_core.exceptions import BadRequest
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
        assert "`" not in destination
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
        assert "`" not in table
        self.deleted.append(table)

    def get_table(self, _: str) -> bigquery.Table:
        """Return a table with required fields for direct-append testing."""
        return bigquery.Table(
            "usagebassoon-test.usagebassoon_emulated.run_metrics",
            schema=[
                bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("source_id", "STRING", mode="REQUIRED"),
            ],
        )

    def close(self) -> None:
        """Satisfy the BigQuery client close surface."""


class _TransactionClient:
    """Offline client recording active-transaction inspection SQL."""

    def __init__(self) -> None:
        """Initialize the recorded inspection statement."""
        self.statement = ""

    def query(
        self,
        statement: str,
        *,
        job_config: bigquery.QueryJobConfig,
        location: str,
    ) -> _Job:
        """Return one running transaction job for diagnostic assertions."""
        assert job_config.default_dataset is not None
        assert location == "US"
        self.statement = statement
        return _Job([{"job_id": "job-1", "transaction_id": "transaction-1"}])

    def close(self) -> None:
        """Satisfy the BigQuery client close surface."""


def _backend() -> BigQueryBackend:
    """Build a BigQuery backend without credentials or network access."""
    return BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
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
            "updated_at": [datetime(2026, 9, 16, tzinfo=UTC)],
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
    assert "MERGE `usagebassoon-test.usagebassoon_emulated.daily_activity`" in script


def test_view_sql_uses_fully_qualified_bigquery_relations() -> None:
    """Qualify view definitions while retaining portable shipped SQL files."""
    backend = _backend()

    qualified = backend._qualify_view_sql(
        "CREATE OR REPLACE VIEW report_summary AS "
        "SELECT * FROM sessions JOIN tags ON TRUE"
    )

    table_prefix = "usagebassoon-test.usagebassoon_emulated"
    assert f"CREATE OR REPLACE VIEW `{table_prefix}.report_summary`" in qualified
    assert f"FROM `{table_prefix}.sessions` AS sessions" in qualified
    assert f"JOIN `{table_prefix}.tags` AS tags" in qualified


def test_bigquery_classifies_only_concurrent_transaction_aborts_as_retryable() -> None:
    """Retry the documented transaction-conflict response and no other bad request."""
    backend = _backend()

    assert backend.is_retryable_error(
        BadRequest("Transaction is aborted due to concurrent update against table")
    )
    assert not backend.is_retryable_error(BadRequest("invalid query"))


def test_bigquery_inspects_running_dataset_transaction_jobs() -> None:
    """Query regional job metadata for running transactions affecting this dataset."""
    client = _TransactionClient()
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )

    transactions = backend.active_transactions(2)

    assert [(item.job_id, item.transaction_id) for item in transactions] == [
        ("job-1", "transaction-1")
    ]
    assert "`usagebassoon-test`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT" in (
        client.statement
    )
    assert (
        "query LIKE '%`usagebassoon-test.usagebassoon_emulated.%'" in client.statement
    )
    assert client.statement.endswith("LIMIT 2")


def test_batch_persistence_loads_explicit_schemas_and_cleans_stages() -> None:
    """Use one explicit-schema load per table before running the batch script."""
    client = _BatchClient()
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )
    run_id = str(uuid4())
    current = pa.table(
        {
            "source_id": ["source"],
            "day": [date(2026, 9, 16)],
            "intensity": [1],
            "active_time_ms": [100],
            "updated_at": [datetime(2026, 9, 16, tzinfo=UTC)],
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


def test_direct_append_uses_the_existing_required_schema() -> None:
    """Preserve destination field modes when restoring or importing data."""
    client = _BatchClient()
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )

    backend.append(
        "run_metrics",
        pa.table({"run_id": ["run"], "source_id": ["source"]}),
    )

    schema = client.loads[0][1].schema
    assert schema is not None
    assert [field.mode for field in schema] == ["REQUIRED", "REQUIRED"]
