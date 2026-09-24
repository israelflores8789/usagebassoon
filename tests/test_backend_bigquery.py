# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_bigquery.py — Offline BigQuery batch and schema unit tests."""

from __future__ import annotations

from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import UTC, date, datetime
from types import TracebackType
from typing import Self, cast
from uuid import uuid4

import pyarrow as pa
import pytest
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types as bigquery_storage_types

from usagebassoon.backends.base import (
    CuratedIdentity,
    CurrentStateWrite,
    PersistenceBatch,
)
from usagebassoon.backends.bigquery import BigQueryBackend, _schema_from_arrow


class _OfflineClient:
    """Minimal client placeholder used when only SQL generation is under test."""

    def close(self) -> None:
        """Satisfy the BigQuery client close surface."""


class _Job:
    """Minimal completed BigQuery job with a deterministic row result."""

    def __init__(
        self,
        rows: list[dict[str, object]] | None = None,
        *,
        affected_rows: int = 0,
    ) -> None:
        """Store rows returned when the fake job is awaited."""
        self._rows = rows or []
        self.num_dml_affected_rows = affected_rows

    def result(self, *, timeout: float | None = None) -> list[dict[str, object]]:
        """Return the completed job's query result rows."""
        assert timeout == 120.0
        return self._rows

    def cancel(self) -> None:
        """Satisfy the timeout-cancellation surface."""

    def to_arrow(self, *, create_bqstorage_client: bool) -> pa.Table:
        """Return an empty Arrow table for bound-query transport tests."""
        assert not create_bqstorage_client
        return pa.table({})


class _TimeoutJob:
    """Minimal job that times out until the backend cancels it."""

    job_id = "stuck-job"

    def __init__(self, expected_timeout: float = 120.0) -> None:
        """Track whether timeout handling canceled the remote job."""
        self.cancelled = False
        self.expected_timeout = expected_timeout

    def result(self, *, timeout: float | None = None) -> None:
        """Raise the same timeout exposed by the BigQuery client."""
        assert timeout == self.expected_timeout
        raise FutureTimeoutError

    def cancel(self) -> None:
        """Record the backend's best-effort cancellation."""
        self.cancelled = True


class _BatchClient:
    """Offline client recording BigQuery batch transport operations."""

    def __init__(self) -> None:
        """Initialize recorded calls."""
        self.loads: list[tuple[str, bigquery.LoadJobConfig]] = []
        self.deleted: list[str] = []
        self.queries: list[str] = []

    def load_table_from_file(
        self,
        payload: object,
        destination: str,
        *,
        job_config: bigquery.LoadJobConfig,
        location: str,
    ) -> _Job:
        """Record one explicit-schema Parquet staging load."""
        assert location == "US"
        assert "`" not in destination
        assert hasattr(payload, "read")
        assert job_config.source_format == bigquery.SourceFormat.PARQUET
        assert job_config.parquet_options is not None
        assert job_config.parquet_options.enable_list_inference
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
            "usagebassoon-test.usagebassoon_emulated.ingest_runs",
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


class _CurationClient:
    """Offline client recording curation queries and their named parameters."""

    def __init__(self) -> None:
        """Initialize recorded curation transport calls."""
        self.queries: list[str] = []
        self.configurations: list[bigquery.QueryJobConfig] = []

    def query(
        self,
        sql: str,
        *,
        job_config: bigquery.QueryJobConfig,
        location: str,
    ) -> _Job:
        """Record one curation query and return its intended atomic outcome."""
        assert location == "US"
        self.queries.append(sql)
        self.configurations.append(job_config)
        if sql.startswith("DELETE"):
            return _Job(affected_rows=1)
        if "BEGIN TRANSACTION" in sql:
            return _Job(
                [
                    {
                        "source_exists": True,
                        "destination_exists": False,
                        "deleted_rows": 1,
                    }
                ]
            )
        return _Job()

    def close(self) -> None:
        """Satisfy the BigQuery client close surface."""


def _backend() -> BigQueryBackend:
    """Build a BigQuery backend without credentials or network access."""
    return BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, _OfflineClient()),
    )


class _StorageReadClient:
    """Fake Storage Read API client recording stream-name arguments."""

    def __init__(self, stream_name: str, table: pa.Table) -> None:
        """Prepare one read session and one Arrow result."""
        self._stream_name = stream_name
        self._table = table
        self.stream_names: list[str] = []

    def __enter__(self) -> Self:
        """Return this fake as a context-managed client."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the context without additional work."""

    def create_read_session(
        self,
        *,
        parent: str,
        read_session: bigquery_storage_types.ReadSession,
        max_stream_count: int,
        timeout: float,
    ) -> bigquery_storage_types.ReadSession:
        """Return a session containing one stream."""
        assert parent == "projects/usagebassoon-test"
        assert read_session.data_format == bigquery_storage_types.DataFormat.ARROW
        assert max_stream_count == 1
        assert timeout == 120.0
        serialized_schema = pa.BufferOutputStream()
        with pa.ipc.new_stream(serialized_schema, self._table.schema):
            pass
        return bigquery_storage_types.ReadSession(
            arrow_schema=bigquery_storage_types.ArrowSchema(
                serialized_schema=serialized_schema.getvalue().to_pybytes()
            ),
            streams=[bigquery_storage_types.ReadStream(name=self._stream_name)],
        )

    def read_rows(
        self,
        name: str,
        *,
        timeout: float,
    ) -> list[bigquery_storage_types.ReadRowsResponse]:
        """Record a stream name and return one serialized Arrow response."""
        assert timeout == 120.0
        self.stream_names.append(name)
        return [
            bigquery_storage_types.ReadRowsResponse(
                arrow_record_batch=bigquery_storage_types.ArrowRecordBatch(
                    serialized_record_batch=self._table.to_batches()[0]
                    .serialize()
                    .to_pybytes()
                )
            )
        ]


def test_bigquery_job_timeout_cancels_and_fails_loudly() -> None:
    """Cancel a stuck remote job and expose its identity in the exception."""
    job = _TimeoutJob()

    with pytest.raises(RuntimeError, match="stuck-job exceeded 120 seconds"):
        _backend()._wait_for_job(cast(bigquery.job.QueryJob, job))

    assert job.cancelled


def test_bigquery_uses_configured_job_timeout() -> None:
    """Pass a custom job wait to BigQuery and report it on timeout."""
    job = _TimeoutJob(expected_timeout=45.0)
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        timeout_seconds=45.0,
        client=cast(bigquery.Client, _OfflineClient()),
    )

    with pytest.raises(RuntimeError, match="stuck-job exceeded 45 seconds"):
        backend._wait_for_job(cast(bigquery.job.QueryJob, job))

    assert job.cancelled


def test_bigquery_query_configuration_sets_the_billing_ceiling() -> None:
    """Apply the configured bytes-billed cap to every query-job configuration."""
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        maximum_bytes_billed=1_073_741_824,
        client=cast(bigquery.Client, _OfflineClient()),
    )

    assert backend._query_config().maximum_bytes_billed == 1_073_741_824


@pytest.mark.parametrize("location", ["US`", "US; DROP TABLE jobs", "us central1"])
def test_bigquery_rejects_locations_unsafe_for_information_schema(
    location: str,
) -> None:
    """Reject interpolated job-metadata locations before constructing SQL."""
    with pytest.raises(ValueError, match="location identifier"):
        BigQueryBackend(
            "usagebassoon-test",
            "usagebassoon_emulated",
            location=location,
            client=cast(bigquery.Client, _OfflineClient()),
        )


def test_bigquery_arrow_reader_passes_stream_name_to_storage_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass each ReadStream name to the Storage client's read_rows method."""
    stream_name = "projects/usagebassoon-test/locations/us/sessions/s/streams/x"
    expected = pa.table(
        {
            "value": [1],
            "models_used": [["gpt-5.6-luna", "gpt-5.6-terra"]],
        }
    )
    storage_reader = _StorageReadClient(stream_name, expected)

    def make_storage_reader(*, credentials: object) -> _StorageReadClient:
        """Return the recording fake in place of a network client."""
        assert credentials is None
        return storage_reader

    monkeypatch.setattr(
        bigquery_storage_v1,
        "BigQueryReadClient",
        make_storage_reader,
    )
    destination = bigquery.TableReference(
        bigquery.DatasetReference("usagebassoon-test", "usagebassoon_emulated"),
        "query_results",
    )
    job = cast(
        bigquery.job.QueryJob,
        type("QueryJobStub", (), {"destination": destination})(),
    )

    result = _backend()._read_query_arrow(job)

    assert result.equals(expected)
    assert storage_reader.stream_names == [stream_name]


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
            CurrentStateWrite(
                "reconciliation_issues",
                pa.table(
                    {
                        "run_id": [run_id],
                        "source_id": ["source"],
                        "check_name": ["models_payload_totals"],
                        "issue_key": ["total_input_mismatch"],
                        "message": ["mismatch"],
                        "created_at": [datetime(2026, 9, 16, tzinfo=UTC)],
                        "updated_at": [datetime(2026, 9, 16, tzinfo=UTC)],
                        "detected_run_id": [run_id],
                        "updated_run_id": [run_id],
                        "resolved_at": [None],
                    }
                ),
                ("source_id", "check_name", "issue_key"),
                ("message", "updated_at", "updated_run_id", "resolved_at"),
            ),
        ),
        append_only={},
        ingest_runs=pa.table(
            {"run_id": [run_id], "rows_inserted": [0], "rows_updated": [0]}
        ),
    )
    stages = {
        "daily_activity": backend._stage_ref("daily_activity", run_id),
        "reconciliation_issues": backend._stage_ref("reconciliation_issues", run_id),
        "ingest_runs": backend._stage_ref("ingest_runs", run_id),
    }
    script = backend._batch_script(batch, stages)
    assert "BEGIN TRANSACTION;" in script
    assert "COMMIT TRANSACTION;" in script
    assert "IF NOT already_committed THEN" in script
    assert "WHERE `run_id` = @run_id" in script
    assert "source.`updated_at` >= target.`updated_at`" in script
    assert run_id.replace("-", "") in stages["daily_activity"]
    assert "MERGE `usagebassoon-test.usagebassoon_emulated.daily_activity`" in script
    assert (
        "MERGE `usagebassoon-test.usagebassoon_emulated.reconciliation_issues`"
        in script
    )
    assert "COALESCE(target.`detected_run_id`, source.`detected_run_id`)" in script


def test_merge_qualifies_target_columns_that_match_the_source_alias() -> None:
    """Keep a column called source distinct from the MERGE source table alias."""
    backend = _backend()
    write = CurrentStateWrite(
        "price_versions",
        pa.table(
            {
                "source_id": ["source-id"],
                "day": [date(2026, 9, 17)],
                "model": ["model"],
                "source": ["tokscale"],
            }
        ),
        ("source_id", "day", "model"),
        ("source",),
    )

    statement = backend._merge_from_data(write, "`staged`")

    assert "target.`source` = source.`source`" in statement


def test_schema_drift_merge_adds_observation_count_once_per_run() -> None:
    """Keep BigQuery event counts cumulative and safe for run retries."""
    backend = _backend()
    drift = pa.table(
        {
            "source_id": ["source"],
            "domain": ["models"],
            "tokscale_ver": ["4.15.2"],
            "drift_key": ["unknown_field:future"],
            "drift_kind": ["unknown_field"],
            "path": ["future"],
            "detail": ["types int; tolerated"],
            "contract_tokscale_ver": ["4.15.1"],
            "created_at": [datetime(2026, 9, 17, tzinfo=UTC)],
            "updated_at": [datetime(2026, 9, 17, tzinfo=UTC)],
            "detected_run_id": ["first-run"],
            "updated_run_id": ["first-run"],
            "resolved": [False],
            "observation_count": [2],
        }
    )
    write = CurrentStateWrite(
        "schema_drift_events",
        drift,
        ("source_id", "domain", "tokscale_ver", "drift_key"),
        ("updated_at", "updated_run_id", "resolved"),
    )

    statement = backend._merge_from_data(write, "`staged`")

    assert (
        "CASE WHEN source.`updated_run_id` = target.`updated_run_id` OR "
        "source.`observation_count` = 0 THEN target.`observation_count` ELSE "
        "target.`observation_count` + source.`observation_count` END"
    ) in statement


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

    qualified_reports = backend._qualify_view_sql(
        "CREATE OR REPLACE VIEW report_summary AS "
        "SELECT * FROM report_session_models "
        "JOIN report_daily_usage ON TRUE"
    )
    assert (
        f"FROM `{table_prefix}.report_session_models` AS report_session_models"
        in qualified_reports
    )
    assert (
        f"JOIN `{table_prefix}.report_daily_usage` AS report_daily_usage"
        in qualified_reports
    )


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


def test_bigquery_curation_operations_use_complete_bound_identities() -> None:
    """Build typed delete and transactional rename SQL without interpolating values."""
    client = _CurationClient()
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )
    source = CuratedIdentity(
        "tags",
        (
            ("source_id", "source"),
            ("scope", "client"),
            ("client", "codex"),
            ("workspace", ""),
            ("session_id", ""),
            ("tag", "old"),
        ),
    )
    destination = CuratedIdentity(
        "tags",
        (
            ("source_id", "source"),
            ("scope", "client"),
            ("client", "codex"),
            ("workspace", ""),
            ("session_id", ""),
            ("tag", "new"),
        ),
    )

    assert backend.delete_curated(source) == 1
    result = backend.rename_curated(
        source,
        destination,
        updated_at=datetime(2026, 9, 17, tzinfo=UTC),
    )

    assert result.renamed
    assert "BEGIN TRANSACTION;" in client.queries[1]
    assert "ASSERT deleted_rows = 1" in client.queries[1]
    assert "= 'source'" not in client.queries[0]
    parameters = client.configurations[0].query_parameters
    assert [parameter.name for parameter in parameters] == [
        "source_id",
        "scope",
        "client",
        "workspace",
        "session_id",
        "tag",
    ]


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
            "reconciliation_issues": pa.table(
                {"run_id": [run_id], "source_id": ["source"]}
            )
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
        "ingest_runs",
        pa.table({"run_id": ["run"], "source_id": ["source"]}),
    )

    schema = client.loads[0][1].schema
    assert schema is not None
    assert [field.mode for field in schema] == ["REQUIRED", "REQUIRED"]
