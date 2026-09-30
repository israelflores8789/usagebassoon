# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_bigquery.py — Offline BigQuery batch and schema unit tests."""

from __future__ import annotations

from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import UTC, date, datetime, timedelta
from threading import Barrier, Lock
from types import TracebackType
from typing import Self, cast, override
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
import sqlglot
from google.api_core.exceptions import BadRequest, NotFound
from google.auth.crypt import Signer
from google.cloud import bigquery, bigquery_datatransfer, bigquery_storage_v1
from google.cloud.bigquery.table import TableListItem
from google.cloud.bigquery_storage_v1 import types as bigquery_storage_types
from google.oauth2.service_account import Credentials
from sqlglot import exp

from usagebassoon.backends.bigquery import BigQueryBackend, _schema_from_arrow
from usagebassoon.backends.bigquery_compaction import install_compaction
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS, normalize
from usagebassoon.persistence import persist_run
from usagebassoon.schema_assets import SCHEMA_VERSION, schema_hash


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
        """Record one explicit-schema Parquet publication or restore load."""
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
        """Record maintenance queries without imposing collection DML semantics."""
        assert job_config.maximum_bytes_billed == 1_073_741_824
        assert location == "US"
        self.queries.append(sql)
        return _Job()

    def delete_table(self, table: str, *, not_found_ok: bool) -> None:
        """Record best-effort staging cleanup."""
        assert not_found_ok
        assert "`" not in table
        self.deleted.append(table)

    def get_table(self, table_id: str) -> bigquery.Table:
        """Return a table with required fields for direct-append testing."""
        return bigquery.Table(
            table_id,
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
    backend = _backend()

    with pytest.raises(RuntimeError, match="stuck-job exceeded 120 seconds") as failure:
        backend._wait_for_job(cast(bigquery.job.QueryJob, job))

    assert job.cancelled
    assert backend.is_retryable_error(failure.value)
    assert not backend.is_retryable_error(ValueError("invalid observation"))


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


def test_view_sql_uses_fully_qualified_bigquery_relations() -> None:
    """Qualify view definitions while retaining portable shipped SQL files."""
    backend = _backend()

    qualified = backend._qualify_view_sql(
        "CREATE OR REPLACE VIEW report_summary AS "
        "SELECT * FROM sessions JOIN tags ON TRUE"
    )

    table_prefix = "usagebassoon-test.usagebassoon_emulated"
    assert f"CREATE OR REPLACE VIEW `{table_prefix}.report_summary`" in qualified
    assert f"FROM `{table_prefix}.sessions`" in qualified
    assert f"JOIN `{table_prefix}.tags`" in qualified

    qualified_reports = backend._qualify_view_sql(
        "CREATE OR REPLACE VIEW report_summary AS "
        "SELECT * FROM report_session_models "
        "JOIN report_daily_usage ON TRUE"
    )
    assert f"FROM `{table_prefix}.report_session_models`" in qualified_reports
    assert f"JOIN `{table_prefix}.report_daily_usage`" in qualified_reports


def test_bigquery_classifies_transient_publication_failures() -> None:
    from google.api_core.exceptions import ServiceUnavailable

    backend = _backend()
    assert backend.is_retryable_error(ServiceUnavailable("unavailable"))
    assert not backend.is_retryable_error(BadRequest("bad schema"))


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


def test_publication_appends_to_bronze_without_queries_or_stages(
    collection_bundle: CollectionBundle,
) -> None:
    """Use direct load jobs with explicit schemas and no synchronous DML."""
    client = _BatchClient()
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )
    bundle = normalize(collection_bundle)
    persist_run(backend, bundle)
    assert client.queries == []
    assert client.deleted == []
    assert client.loads[-1][0].endswith(".collection_ledger")
    expected = {
        "collection_ledger" if table == "collection_ledger" else "raw_" + table
        for table, data in bundle.tables.items()
        if data.num_rows
    }
    assert {
        destination.rsplit(".", 1)[-1] for destination, _ in client.loads
    } == expected
    assert all(
        config.write_disposition == "WRITE_APPEND"
        and config.create_disposition == "CREATE_NEVER"
        for _, config in client.loads
    )


def test_partial_publication_replays_event_ids_and_withholds_coverage(
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent facts finish before coverage is certified, including on retry."""
    from google.api_core.exceptions import ServiceUnavailable

    backend = _backend()
    bundle = normalize(collection_bundle)
    facts = [table for table in bundle.tables if table != "collection_ledger"]
    barrier = Barrier(len(facts))
    lock = Lock()
    calls: list[tuple[str, tuple[str, ...]]] = []
    failure = True

    def append(table: str, data: pa.Table) -> None:
        """Require overlapping loads and fail one table on the first attempt."""
        nonlocal failure
        if table != "collection_ledger":
            barrier.wait(timeout=5)
        with lock:
            calls.append((table, tuple(data.column("event_id").to_pylist())))
            if table == "daily_stats" and failure:
                failure = False
                raise ServiceUnavailable("controlled append failure")

    monkeypatch.setattr(backend, "append", append)
    with pytest.raises(ServiceUnavailable):
        persist_run(backend, bundle)
    assert "collection_ledger" not in {table for table, _ in calls}
    first = dict(calls)
    persist_run(backend, bundle)
    assert calls[-1][0] == "collection_ledger"
    assert first == dict(calls[len(facts) : -1])


def test_matching_preflight_reads_metadata_without_query_jobs() -> None:
    """Validate an initialized warehouse without schema or data queries."""

    class Client(_BatchClient):
        def get_dataset(self, _: str) -> bigquery.Dataset:
            dataset = bigquery.Dataset("usagebassoon-test.usagebassoon_emulated")
            dataset.location = "US"
            return dataset

        @override
        def get_table(self, table_id: str) -> bigquery.Table:
            table = super().get_table(table_id)
            table.labels = {
                "usagebassoon_schema_version": str(SCHEMA_VERSION),
                "usagebassoon_schema_hash": schema_hash("bigquery"),
            }
            return table

    client = Client()
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )
    backend.preflight()
    assert client.queries == []
    assert client.loads == []


def test_canonical_required_fields_survive_bigquery_schema_mapping() -> None:
    """Never relax warehouse keys or event identities during a load."""
    for schema in CANONICAL_TABLE_SCHEMAS.values():
        mapped = _schema_from_arrow(pa.Table.from_pylist([], schema=schema))
        for field, native in zip(schema, mapped, strict=True):
            if not pa.types.is_list(field.type):
                assert native.mode == ("NULLABLE" if field.nullable else "REQUIRED")


class _InitClient(_BatchClient):
    """Metadata-backed initialization fake with one interrupted view installation."""

    def __init__(self) -> None:
        super().__init__()
        self.tables: dict[str, bigquery.Table] = {}
        self.created: list[str] = []
        self.fail_views = True

    def get_dataset(self, _: str) -> bigquery.Dataset:
        dataset = bigquery.Dataset("usagebassoon-test.usagebassoon_emulated")
        dataset.location = "US"
        return dataset

    def list_tables(self, _: str) -> list[bigquery.Table]:
        return list(self.tables.values())

    @override
    def get_table(self, table_id: str) -> bigquery.Table:
        name = table_id.rsplit(".", 1)[-1]
        if name not in self.tables:
            raise NotFound("missing initialization object")
        return bigquery.Table.from_api_repr(self.tables[name].to_api_repr())

    def update_table(self, table: bigquery.Table, _: list[str]) -> bigquery.Table:
        self.tables[table.table_id] = table
        return table

    @override
    def query(
        self,
        sql: str,
        *,
        job_config: bigquery.QueryJobConfig,
        location: str,
    ) -> _Job:
        assert location == "US" and job_config.default_dataset is not None
        self.queries.append(sql)
        if "CREATE OR REPLACE VIEW" in sql:
            if self.fail_views:
                self.fail_views = False
                raise BadRequest("controlled view installation failure")
            return _Job()
        for statement in sqlglot.parse(sql, read="bigquery"):
            if isinstance(statement, exp.Create):
                name = statement.this.this.name
                assert name not in self.tables
                fields = []
                for column in statement.this.expressions:
                    kind = column.args["kind"].sql(dialect="bigquery")
                    required = any(
                        isinstance(constraint.kind, exp.NotNullColumnConstraint)
                        for constraint in column.args.get("constraints", [])
                    )
                    mode = "REQUIRED" if required else "NULLABLE"
                    if kind == "ARRAY<STRING>":
                        kind, mode = "STRING", "REPEATED"
                    fields.append(bigquery.SchemaField(column.name, kind, mode=mode))
                table = bigquery.Table(
                    "usagebassoon-test.usagebassoon_emulated." + name,
                    schema=fields,
                )
                table.expires = datetime.now(UTC) + timedelta(days=1)
                for partition in statement.find_all(exp.PartitionedByProperty):
                    field = partition.this.name
                    table.time_partitioning = bigquery.TimePartitioning(
                        field=None if field == "_PARTITIONDATE" else field,
                        expiration_ms=86400000,
                    )
                self.tables[name] = table
                self.created.append(name)
            elif isinstance(statement, exp.Insert):
                assert statement.expression.args.get("from_") is not None
        return _Job()


def test_init_replays_an_interruption_without_recreating_tables() -> None:
    """Block incomplete opens, resume initialization, and clear inherited expiry."""
    client = _InitClient()
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )
    with pytest.raises(BadRequest, match="controlled"):
        backend.apply_ddl()
    with pytest.raises(RuntimeError, match="incomplete"):
        backend.preflight()
    created = client.created.copy()
    backend.apply_ddl()
    assert client.created == created
    backend.preflight()
    jobs = len(client.queries)
    backend.apply_ddl()
    assert len(client.queries) == jobs
    for name, table in client.tables.items():
        assert table.expires is None
        if table.time_partitioning is not None:
            expected = 90 * 86400000 if name.startswith("raw_") else None
            assert table.time_partitioning.expiration_ms == expected


@pytest.mark.parametrize(
    ("version", "hash_value", "message"),
    [
        (SCHEMA_VERSION + 1, None, "newer UsageBassoon"),
        (SCHEMA_VERSION, "0" * 63, "schema hash"),
    ],
)
def test_preflight_rejects_unsupported_markers_without_schema_jobs(
    version: int,
    hash_value: str | None,
    message: str,
) -> None:
    """Refuse newer warehouses and mismatched baselines without altering data."""
    client = _InitClient()
    marker = bigquery.Table("usagebassoon-test.usagebassoon_emulated.schema_marker")
    marker.labels = {
        "usagebassoon_schema_version": str(version),
        "usagebassoon_schema_hash": hash_value or schema_hash("bigquery"),
    }
    client.tables["schema_marker"] = marker
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )
    with pytest.raises(RuntimeError, match=message):
        backend.preflight()
    assert client.queries == []


def test_failed_ledger_append_leaves_facts_visible_and_retry_safe(
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing completion repeats work without hiding successful fact appends."""
    from google.api_core.exceptions import ServiceUnavailable

    backend = _backend()
    bundle = normalize(collection_bundle)
    calls: list[tuple[str, tuple[str, ...]]] = []
    lock = Lock()
    fail_ledger = True

    def append(table: str, data: pa.Table) -> None:
        """Record stable event IDs and fail the first final ledger append."""
        nonlocal fail_ledger
        with lock:
            calls.append((table, tuple(data.column("event_id").to_pylist())))
            if table == "collection_ledger" and fail_ledger:
                fail_ledger = False
                raise ServiceUnavailable("controlled ledger failure")

    monkeypatch.setattr(backend, "append", append)
    with pytest.raises(ServiceUnavailable, match="ledger failure"):
        persist_run(backend, bundle)
    assert calls[-1][0] == "collection_ledger"
    first = dict(calls)
    calls.clear()
    persist_run(backend, bundle)
    assert calls[-1][0] == "collection_ledger"
    assert dict(calls) == first


@pytest.mark.parametrize(
    ("message", "exception_type"),
    [
        ("Query error: restore requires an empty warehouse at [4:1]", ValueError),
        ("controlled invalid restore SQL", BadRequest),
    ],
)
def test_restore_translates_emptiness_errors_and_cleans_stages(
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
    message: str,
    exception_type: type[Exception],
) -> None:
    """Surface the destination precondition while preserving unrelated API errors."""
    failure = BadRequest(message)

    class Client(_BatchClient):
        """Expose one destination table and reject the restore transaction."""

        def list_tables(self, dataset: str) -> list[TableListItem]:
            """Return the initialized destination table for emptiness validation."""
            assert dataset == "usagebassoon-test.usagebassoon_emulated"
            return [
                TableListItem(
                    {
                        "tableReference": {
                            "projectId": "usagebassoon-test",
                            "datasetId": "usagebassoon_emulated",
                            "tableId": "daily_stats",
                        },
                        "id": "usagebassoon-test:usagebassoon_emulated.daily_stats",
                        "type": "TABLE",
                    }
                )
            ]

        @override
        def query(
            self,
            sql: str,
            *,
            job_config: bigquery.QueryJobConfig,
            location: str,
        ) -> _Job:
            """Raise the backend error after all restore assertions are assembled."""
            assert "restore requires an empty warehouse" in sql
            assert job_config.default_dataset is not None and location == "US"
            raise failure

    client = Client()
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )
    staged: list[str] = []

    def load(data: pa.Table, destination: str, *, disposition: str) -> None:
        """Record the staged snapshot without making a cloud call."""
        assert data.num_rows > 0 and disposition == "WRITE_TRUNCATE"
        staged.append(destination)

    monkeypatch.setattr(backend, "_load", load)
    bundle = normalize(collection_bundle)
    with pytest.raises(exception_type) as raised:
        backend.restore_tables({"daily_stats": bundle.tables["daily_stats"]})
    assert client.deleted == staged
    assert len(staged) == 1
    if exception_type is ValueError:
        assert str(raised.value) == "restore requires an empty warehouse"
        assert raised.value.__cause__ is failure
    else:
        assert raised.value is failure


def test_nightly_schedule_create_reuse_and_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the service account, reuse matching schedules, and update changed SQL."""
    credentials = Credentials(
        signer=cast(Signer, MagicMock(spec=Signer)),
        service_account_email="collector@example.iam.gserviceaccount.com",
        token_uri="https://oauth2.googleapis.com/token",
    )
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_it",
        credentials=credentials,
        client=cast(bigquery.Client, MagicMock(spec=bigquery.Client)),
    )
    client = MagicMock(spec=bigquery_datatransfer.DataTransferServiceClient)
    client.__enter__.return_value = client
    empty_configs: list[bigquery_datatransfer.TransferConfig] = []
    client.list_transfer_configs.return_value = empty_configs
    client.create_transfer_config.return_value = bigquery_datatransfer.TransferConfig(
        name="projects/1/locations/us/transferConfigs/1"
    )
    factory = MagicMock(return_value=client)
    monkeypatch.setattr(bigquery_datatransfer, "DataTransferServiceClient", factory)
    name = install_compaction(backend)
    factory.assert_called_with(credentials=credentials)
    request = client.create_transfer_config.call_args.kwargs["request"]
    assert request.parent == "projects/usagebassoon-test/locations/us"
    assert request.service_account_name == credentials.service_account_email
    assert request.transfer_config.schedule == "every day 02:00"
    assert "BEGIN TRANSACTION" in request.transfer_config.params["query"]
    assert (
        "`usagebassoon-test.usagebassoon_it.raw_daily_stats`"
        in request.transfer_config.params["query"]
    )
    existing = bigquery_datatransfer.TransferConfig(request.transfer_config)
    existing.name = name
    client.list_transfer_configs.return_value = [existing]
    assert install_compaction(backend) == name
    client.create_transfer_config.assert_called_once()
    client.update_transfer_config.assert_not_called()
    existing = bigquery_datatransfer.TransferConfig(
        name=name,
        display_name=existing.display_name,
        data_source_id="scheduled_query",
        params={"query": "old SQL"},
    )
    client.list_transfer_configs.return_value = [existing]
    client.update_transfer_config.return_value = existing
    assert install_compaction(backend) == name
    update = client.update_transfer_config.call_args.kwargs
    assert list(update["update_mask"].paths) == ["params", "schedule"]
    assert (
        update["transfer_config"].params["query"]
        == request.transfer_config.params["query"]
    )
    client.list_transfer_configs.return_value = [existing, existing]
    with pytest.raises(RuntimeError, match="multiple UsageBassoon"):
        install_compaction(backend)
