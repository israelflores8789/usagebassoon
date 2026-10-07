# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_bigquery.py — Offline BigQuery batch and schema unit tests."""

from __future__ import annotations

import logging
import time
from collections.abc import Generator, Iterable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from threading import Barrier, Event, Lock, Thread
from types import TracebackType
from typing import Self, cast, override
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
import sqlglot
from google.api_core.exceptions import (
    BadRequest,
    Conflict,
    DeadlineExceeded,
    NotFound,
    RetryError,
    ServiceUnavailable,
    Unknown,
)
from google.auth.credentials import AnonymousCredentials
from google.auth.crypt import Signer
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, bigquery_datatransfer
from google.cloud.bigquery.table import TableListItem
from google.cloud.bigquery_storage_v1 import types as bigquery_storage_types
from google.cloud.bigquery_storage_v1.services import big_query_read
from google.oauth2.service_account import Credentials
from sqlglot import exp

from tests._bigquery_replay import BigQueryReplayBackend
from tests.test_backend_bigquery_live import (
    _drain_compaction_schedule,
    _reset_test_schema,
)
from usagebassoon.backends.bigquery import (
    BigQueryBackend,
    _pinned_query,
    _request_retry,
    _schema_from_arrow,
)
from usagebassoon.backends.bigquery_compaction import install_compaction
from usagebassoon.config import BigQueryConfig, CollectionConfig, UsageBassoonConfig
from usagebassoon.deadlines import (
    OperationTimeout,
    current_deadline,
    http_session,
    operation,
)
from usagebassoon.diagnostics import REQUIRED_RELATIONS, run_doctor
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS, NormalizedBundle, normalize
from usagebassoon.persistence import PersistSummary, persist_run, persist_with_retries
from usagebassoon.schema_assets import SCHEMA_VERSION, schema_hash
from usagebassoon.storage_model import note_id_for_session


def test_service_account_file_scopes_the_bigquery_http_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Use scoped credentials in both the SDK and its supplied HTTP session."""
    credentials = Credentials(
        signer=cast(Signer, MagicMock(spec=Signer)),
        service_account_email="collector@example.iam.gserviceaccount.com",
        token_uri="https://oauth2.googleapis.com/token",
    )

    def load(_filename: str, *, scopes: list[str] | None = None) -> Credentials:
        """Model the SDK's scope-preserving credential file loader offline."""
        return credentials.with_scopes(scopes)

    monkeypatch.setattr(Credentials, "from_service_account_file", load)
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_it",
        credentials_file=tmp_path / "credentials.json",
    )
    try:
        session = backend.client._http
        assert isinstance(session, AuthorizedSession)
        assert not session.credentials.requires_scopes
        assert session.credentials.scopes == [
            "https://www.googleapis.com/auth/cloud-platform"
        ]
        assert session.credentials is backend._credentials
        assert session.credentials is backend.client._credentials
    finally:
        backend.close()


def test_bigquery_startup_retry_exhaustion_retries_the_same_batch(
    collection_bundle: CollectionBundle,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhausted metadata RPC retries remain eligible before backend open returns."""
    config = UsageBassoonConfig(
        tmp_path / "config.toml",
        collection_bundle.source_id,
        "bigquery",
        bigquery=BigQueryConfig("usagebassoon-test", "usagebassoon_it"),
        collection=CollectionConfig(max_retries=1, retry_initial_seconds=0),
    )
    bundle = normalize(collection_bundle)
    backend = _backend()
    attempts: list[int] = []
    published: list[NormalizedBundle] = []

    def backoff(*_args: object, **_kwargs: object) -> Iterable[float]:
        """Force SDK retry exhaustion without waiting in the test."""
        return (360.0,)

    def unavailable() -> None:
        """Model a metadata request before factory construction completes."""
        raise ServiceUnavailable("temporary metadata outage")

    def open_backend(_config: UsageBassoonConfig) -> BigQueryBackend:
        """Return the backend only after the next complete attempt starts."""
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            _request_retry()(unavailable)()
        return backend

    def persist(_backend: object, current: NormalizedBundle) -> PersistSummary:
        """Observe that startup recovery preserves the original normalized run."""
        published.append(current)
        return PersistSummary(1, 0, {})

    monkeypatch.setattr(
        "google.api_core.retry.retry_unary.exponential_sleep_generator", backoff
    )
    monkeypatch.setattr("usagebassoon.persistence.open_backend", open_backend)
    monkeypatch.setattr("usagebassoon.persistence.persist_run", persist)
    result = persist_with_retries(config, bundle, logging.getLogger("test"))
    assert result.inserted == 1
    assert attempts == [1, 2]
    assert published == [bundle] and published[0] is bundle


def test_bigquery_partial_publication_never_marks_missing_facts_complete(
    collection_bundle: CollectionBundle, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fact and ledger failures leave useful data visible and safe to recollect."""
    backend = BigQueryReplayBackend()
    bundle = normalize(collection_bundle)
    original_append = backend.append
    lock = Lock()
    failed_table = "daily_stats"

    def append(table: str, data: pa.Table) -> None:
        """Serialize the local replay engine while preserving independent loads."""
        with lock:
            if table == failed_table:
                raise ServiceUnavailable("controlled publication failure")
            original_append(table, data)

    monkeypatch.setattr(backend, "append", append)
    try:
        with pytest.raises(ServiceUnavailable):
            persist_run(backend, bundle)
        assert backend.query("SELECT * FROM current_sessions").num_rows > 0
        assert backend.query("SELECT * FROM current_daily_stats").num_rows == 0
        assert backend.query("SELECT * FROM collection_status").num_rows == 0
        failed_table = "collection_ledger"
        with pytest.raises(ServiceUnavailable):
            persist_run(backend, bundle)
        assert backend.query("SELECT * FROM current_daily_stats").num_rows > 0
        assert backend.query("SELECT * FROM collection_status").num_rows == 0
        failed_table = ""
        persist_run(backend, bundle)
        assert backend.query("SELECT * FROM collection_runs").num_rows == 1
        assert backend.query("SELECT * FROM collection_status").num_rows > 0
        for table in ("sessions", "daily_stats", "price_versions"):
            assert backend.query(f"SELECT * FROM current_{table}").num_rows == (
                bundle.tables[table].num_rows
            )
    finally:
        backend.close()


@pytest.mark.parametrize("boundary", ["submission", "upload", "cancellation"])
def test_bigquery_stalled_transport_returns_and_next_operation_runs(
    boundary: str,
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise real SDK requests stalled before submission or during cancellation."""
    from requests import Session

    release = Event()

    class StalledHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            release.wait(2)
            self.send_response(200)
            self.end_headers()

        @override
        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), StalledHandler)
    server.daemon_threads = True
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    credentials = AnonymousCredentials()
    session = cast(Session, http_session(credentials))
    client = bigquery.Client(
        project="usagebassoon-test",
        credentials=credentials,
        _http=session,
        client_options={"api_endpoint": f"http://127.0.0.1:{server.server_port}"},
    )
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_it",
        client=client,
        timeout_seconds=0.15,
    )
    started = time.monotonic()
    try:
        with pytest.raises(OperationTimeout) as failure:
            if boundary == "submission":
                backend.query("SELECT 1")
            elif boundary == "upload":
                backend.append(
                    "daily_stats", normalize(collection_bundle).tables["daily_stats"]
                )
            else:
                # Bound cancellation separately after job polling fails.
                job = bigquery.QueryJob("stuck", "SELECT 1", client)
                import usagebassoon.deadlines as deadlines

                monkeypatch.setattr(deadlines, "CLEANUP_SECONDS", 0.15)
                monkeypatch.setattr(
                    job, "result", MagicMock(side_effect=FutureTimeoutError)
                )
                backend._wait_for_job(job)
        assert time.monotonic() - started < 2
        assert backend.is_retryable_error(failure.value)
        assert backend.is_retryable_error(OperationTimeout())
        backend.client = cast(bigquery.Client, _BatchClient())
        backend.append(
            "daily_stats", normalize(collection_bundle).tables["daily_stats"]
        )
    finally:
        release.set()
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("boundary", ["submission", "metadata", "cancellation"])
def test_bigquery_transient_http_requests_recover_within_the_budget(
    boundary: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Exercise actual SDK retries for a failed submission, lookup, or cancellation."""
    import json

    from requests import Session

    calls: list[str] = []
    identifiers: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def respond(self, *, cancellation: bool = False) -> None:
            """Fail one request, then return a valid minimal BigQuery resource."""
            calls.append(self.command)
            reference: dict[str, str] = {
                "projectId": "usagebassoon-test",
                "jobId": "job",
                "location": "US",
            }
            if self.command == "POST":
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                if raw:
                    body = cast(dict[str, object], json.loads(raw))
                    reference = cast(dict[str, str], body["jobReference"])
                    identifiers.append(reference["jobId"])
            if len(calls) == 1:
                self.send_response(503)
                body_bytes = json.dumps(
                    {"error": {"code": 503, "message": "temporarily unavailable"}}
                ).encode()
            else:
                self.send_response(200)
                job: dict[str, object] = {
                    "jobReference": reference,
                    "configuration": {
                        "query": {"query": "SELECT 1", "useLegacySql": False}
                    },
                    "status": {"state": "DONE"},
                }
                resource: dict[str, object]
                if self.command == "GET":
                    resource = {
                        "datasetReference": {
                            "projectId": "usagebassoon-test",
                            "datasetId": "usagebassoon_it",
                        },
                        "location": "US",
                    }
                else:
                    resource = {"job": job} if cancellation else job
                body_bytes = json.dumps(resource).encode()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body_bytes)))
            self.end_headers()
            self.wfile.write(body_bytes)

        def do_POST(self) -> None:
            """Serve job submissions and cancellation acknowledgements."""
            self.respond(cancellation="/cancel" in self.path)

        def do_GET(self) -> None:
            """Serve the dataset metadata lookup."""
            self.respond()

        @override
        def log_message(self, format: str, *args: object) -> None:
            """Avoid the HTTP server's independent diagnostic sink."""
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    credentials = AnonymousCredentials()
    client = bigquery.Client(
        project="usagebassoon-test",
        credentials=credentials,
        _http=cast(Session, http_session(credentials)),
        client_options={"api_endpoint": f"http://127.0.0.1:{server.server_port}"},
    )
    backend = BigQueryBackend(
        "usagebassoon-test", "usagebassoon_it", client=client, timeout_seconds=2
    )
    try:
        if boundary == "submission":
            monkeypatch.setattr(backend, "_wait_for_job", MagicMock(return_value=[]))
            monkeypatch.setattr(
                backend, "_read_query_arrow", MagicMock(return_value=pa.table({}))
            )
            backend.query("SELECT 1")
            assert len(identifiers) == 2 and len(set(identifiers)) == 1
        elif boundary == "metadata":
            marker = bigquery.Table("usagebassoon-test.usagebassoon_it.schema_marker")
            marker.labels = {
                "usagebassoon_schema_version": str(SCHEMA_VERSION),
                "usagebassoon_schema_hash": schema_hash("bigquery"),
            }
            monkeypatch.setattr(client, "get_table", MagicMock(return_value=marker))
            backend.preflight()
        else:
            job = bigquery.QueryJob("job", "SELECT 1", client)
            monkeypatch.setattr(
                job, "result", MagicMock(side_effect=FutureTimeoutError)
            )
            with pytest.raises(OperationTimeout):
                backend._wait_for_job(job)
        assert len(calls) == 2
        assert "retrying within operation budget" in caplog.text
        assert "could not cancel" not in caplog.text
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_bigquery_parallel_loads_and_ledger_share_one_attempt_budget(
    monkeypatch: pytest.MonkeyPatch,
    collection_bundle: CollectionBundle,
) -> None:
    """Concurrent loads cannot each reset the budget or certify an expired attempt."""
    import usagebassoon.deadlines as deadlines

    clock = [0.0]
    monkeypatch.setattr(deadlines, "monotonic", lambda: clock[0])
    backend = _backend()
    backend.timeout_seconds = 10
    bundle = normalize(collection_bundle)
    tables = [
        name
        for name, data in bundle.tables.items()
        if name != "collection_ledger" and data.num_rows
    ]
    barrier = Barrier(len(tables))
    lock = Lock()
    scopes: list[object] = []
    observed: list[str] = []
    expire = True

    def append(table: str, _data: pa.Table) -> None:
        if table != "collection_ledger":
            barrier.wait(timeout=2)
        deadline = current_deadline()
        assert deadline is not None
        with lock:
            scopes.append(deadline)
            observed.append(table)
            if expire:
                clock[0] = 11
            deadline.remaining()

    monkeypatch.setattr(backend, "append", append)
    with pytest.raises(OperationTimeout):
        persist_run(backend, bundle)
    assert "collection_ledger" not in observed
    assert len({id(scope) for scope in scopes}) == 1
    first_scope = scopes[0]
    expire = False
    scopes.clear()
    observed.clear()
    persist_run(backend, bundle)
    assert observed[-1] == "collection_ledger"
    assert len({id(scope) for scope in scopes}) == 1
    assert scopes[0] is not first_scope


class _OfflineClient:
    """Minimal client placeholder used when only SQL generation is under test."""

    def close(self) -> None:
        """Satisfy the BigQuery client close surface."""

    def list_jobs(self, **_kwargs: object) -> MagicMock:
        """Expose an empty project-wide job page without cloud access."""
        return _job_page([])


def _job_page(jobs: list[object]) -> MagicMock:
    """Provide the pagination boundary used by the official BigQuery client."""
    pager = MagicMock()
    pager.pages = [jobs]
    pager.next_page_token = None
    return pager


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

    def result(
        self,
        *,
        timeout: float | None = None,
        retry: object = None,
        job_retry: object = None,
    ) -> list[dict[str, object]]:
        """Return the completed job's query result rows."""
        del retry, job_retry
        assert timeout is not None and 0 < timeout <= 180.0
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

    def result(self, *, timeout: float | None = None, retry: object = None) -> None:
        """Raise the same timeout exposed by the BigQuery client."""
        del retry
        assert timeout is not None and 0 < timeout <= min(120.0, self.expected_timeout)
        raise FutureTimeoutError

    def cancel(self, *, timeout: float, retry: object) -> None:
        """Record the backend's best-effort cancellation."""
        assert 0 < timeout <= 15
        del retry
        self.cancelled = True


class _BatchClient:
    """Offline client recording BigQuery batch transport operations."""

    def list_jobs(self, **_kwargs: object) -> MagicMock:
        """Return no active restore jobs in completed batch scenarios."""
        return _job_page([])

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
        **_transport: object,
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
        **_transport: object,
    ) -> _Job:
        """Record maintenance queries without imposing collection DML semantics."""
        assert job_config.maximum_bytes_billed == 1_073_741_824
        assert location == "US"
        self.queries.append(sql)
        return _Job()

    def delete_table(
        self,
        table: str,
        *,
        not_found_ok: bool,
        retry: object = None,
        timeout: float | None = None,
    ) -> None:
        """Record best-effort staging cleanup."""
        del retry, timeout
        assert not_found_ok
        assert "`" not in table
        self.deleted.append(table)

    def get_table(self, table_id: str, **_transport: object) -> bigquery.Table:
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
        **_transport: object,
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


@pytest.mark.parametrize(
    ("sql", "parameters", "expected"),
    [
        (
            "SELECT * FROM daily_cost WHERE session_id = 'provider:session' LIMIT 1",
            None,
            "SELECT * FROM daily_cost WHERE session_id = 'provider:session' LIMIT 1",
        ),
        (
            "SELECT 'provider:session' AS `field:name`, :client AS bound "
            "/* :block */ -- :line\n",
            {"client": "bound:value"},
            "SELECT 'provider:session' AS `field:name`, @client AS bound "
            "/* :block */ -- :line\n",
        ),
        (
            'SELECT "provider:session" AS value, :client AS bound # :comment\n',
            {"client": "bound:value"},
            'SELECT "provider:session" AS value, @client AS bound # :comment\n',
        ),
        (
            r"SELECT 'it\'s:session' AS value, :client AS bound",
            {"client": "bound:value"},
            r"SELECT 'it\'s:session' AS value, @client AS bound",
        ),
        (
            "SELECT r'''provider:session''' AS value, :client AS bound",
            {"client": "bound:value"},
            "SELECT r'''provider:session''' AS value, @client AS bound",
        ),
        (
            "SELECT @client AS value, :other AS bound",
            {"client": "native:value", "other": "bound:value"},
            "SELECT @client AS value, @other AS bound",
        ),
        (
            "SELECT :select AS value, :select AS bound",
            {"select": "keyword:value"},
            "SELECT @select AS value, @select AS bound",
        ),
    ],
)
def test_bigquery_query_preserves_sql_outside_parameter_spans(
    monkeypatch: pytest.MonkeyPatch,
    sql: str,
    parameters: dict[str, str] | None,
    expected: str,
) -> None:
    """Send exact SQL and separate bound values to the official driver."""
    backend = _backend()
    client = MagicMock(spec=bigquery.Client)
    backend.client = client

    def arrow_result(_job: bigquery.QueryJob) -> pa.Table:
        """Avoid the Storage transport while inspecting query submission."""
        return pa.table({})

    monkeypatch.setattr(backend, "_read_query_arrow", arrow_result)
    backend.query(sql, parameters)
    assert client.query.call_args.args[0] == expected
    bindings = client.query.call_args.kwargs["job_config"].query_parameters
    assert [(item.name, item.type_, item.value) for item in bindings] == [
        (name, "STRING", value) for name, value in (parameters or {}).items()
    ]


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
        retry: object,
        timeout: float,
    ) -> bigquery_storage_types.ReadSession:
        """Return a session containing one stream."""
        assert retry is None
        assert parent == "projects/usagebassoon-test"
        assert read_session.data_format == bigquery_storage_types.DataFormat.ARROW
        assert max_stream_count == 1
        assert 0 < timeout <= 30.0
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
        *,
        read_stream: str,
        offset: int,
        retry: object,
        timeout: float,
    ) -> Iterable[bigquery_storage_types.ReadRowsResponse]:
        """Record a stream name and return one serialized Arrow response."""
        assert retry is None
        assert 0 < timeout <= 180.0
        assert offset == 0
        self.stream_names.append(read_stream)
        return [
            bigquery_storage_types.ReadRowsResponse(
                row_count=self._table.num_rows,
                arrow_record_batch=bigquery_storage_types.ArrowRecordBatch(
                    serialized_record_batch=self._table.to_batches()[0]
                    .serialize()
                    .to_pybytes()
                ),
            )
        ]


@pytest.mark.parametrize("timeout", [None, 45.0])
def test_bigquery_job_timeout_cancels_and_is_retryable(timeout: float | None) -> None:
    """Honor default/custom waits, cancel stuck jobs, and report retryable failure."""
    expected = 180.0 if timeout is None else timeout
    job = _TimeoutJob(expected_timeout=expected)
    backend = (
        _backend()
        if timeout is None
        else BigQueryBackend(
            "usagebassoon-test",
            "usagebassoon_emulated",
            timeout_seconds=timeout,
            client=cast(bigquery.Client, _OfflineClient()),
        )
    )
    with pytest.raises(RuntimeError, match="stuck-job wait timed out") as failure:
        backend._wait_for_job(cast(bigquery.job.QueryJob, job))
    assert job.cancelled
    assert backend.is_retryable_error(failure.value)
    assert not backend.is_retryable_error(ValueError("invalid observation"))


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
        big_query_read,
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


@pytest.mark.parametrize("boundary", ["initial", "midstream"])
@pytest.mark.parametrize(
    "failure_type", [DeadlineExceeded, ServiceUnavailable, Unknown]
)
def test_bigquery_read_recovery_preserves_rows_and_operation_budget(
    boundary: str,
    failure_type: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Resume initial and partial failures without replaying consumed Arrow rows."""
    import usagebassoon.deadlines as deadlines

    clock = [0.0]
    monkeypatch.setattr(deadlines, "monotonic", lambda: clock[0])

    def advance(delay: float) -> None:
        """Account for retry backoff in the operation clock."""
        clock[0] += delay

    monkeypatch.setattr(
        "usagebassoon.backends.bigquery.time.sleep",
        advance,
    )
    expected = pa.table({"value": [1, 2, 3, 4]})
    calls: list[tuple[int, float]] = []

    class Reader(_StorageReadClient):
        @override
        def read_rows(
            self, *, read_stream: str, offset: int, retry: object, timeout: float
        ) -> Generator[bigquery_storage_types.ReadRowsResponse]:
            """Fail once, then return only the rows after the saved offset."""
            assert retry is None and read_stream == "stream"
            calls.append((offset, timeout))
            clock[0] += 35
            if len(calls) == 1:
                if boundary == "midstream":
                    batch = expected.slice(0, 2).to_batches()[0]
                    yield bigquery_storage_types.ReadRowsResponse(
                        row_count=2,
                        arrow_record_batch=bigquery_storage_types.ArrowRecordBatch(
                            serialized_record_batch=batch.serialize().to_pybytes()
                        ),
                    )
                raise failure_type("transient read failure")
            batch = expected.slice(offset).to_batches()[0]
            yield bigquery_storage_types.ReadRowsResponse(
                row_count=batch.num_rows,
                arrow_record_batch=bigquery_storage_types.ArrowRecordBatch(
                    serialized_record_batch=batch.serialize().to_pybytes()
                ),
            )

    reader = Reader("stream", expected)

    def make_reader(*, credentials: object) -> Reader:
        """Use the recording reader without opening a cloud channel."""
        del credentials
        return reader

    monkeypatch.setattr(big_query_read, "BigQueryReadClient", make_reader)
    job = MagicMock(spec=bigquery.QueryJob)
    job.destination = bigquery.TableReference.from_string(
        "usagebassoon-test.usagebassoon_it.result"
    )
    actual = _backend()._read_query_arrow(job)
    assert actual.equals(expected)
    assert calls[0] == (0, 180)
    assert calls[1][0] == (2 if boundary == "midstream" else 0)
    assert 0 < calls[1][1] <= 145
    assert "retrying from row offset" in caplog.text


@pytest.mark.parametrize("overrun", [False, True])
def test_bigquery_progressing_stream_uses_remaining_budget(
    overrun: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Healthy streams may exceed 30s, but progress cannot renew the operation."""
    import usagebassoon.deadlines as deadlines

    clock = [0.0]
    monkeypatch.setattr(deadlines, "monotonic", lambda: clock[0])
    expected = pa.table({"value": [1, 2]})
    timeouts: list[float] = []

    class Reader(_StorageReadClient):
        @override
        def read_rows(
            self, *, read_stream: str, offset: int, retry: object, timeout: float
        ) -> Generator[bigquery_storage_types.ReadRowsResponse]:
            """Advance real work beyond a short request timeout."""
            assert offset == 0 and retry is None and read_stream == "stream"
            timeouts.append(timeout)
            for _ in range(4 if overrun else 2):
                clock[0] += 35
                yield bigquery_storage_types.ReadRowsResponse(
                    row_count=2,
                    arrow_record_batch=bigquery_storage_types.ArrowRecordBatch(
                        serialized_record_batch=expected.to_batches()[0]
                        .serialize()
                        .to_pybytes()
                    ),
                )

    reader = Reader("stream", expected)

    def make_reader(*, credentials: object) -> Reader:
        """Use the recording reader without opening a cloud channel."""
        del credentials
        return reader

    monkeypatch.setattr(big_query_read, "BigQueryReadClient", make_reader)
    job = MagicMock(spec=bigquery.QueryJob)
    job.destination = bigquery.TableReference.from_string(
        "usagebassoon-test.usagebassoon_it.result"
    )

    def consume() -> pa.Table:
        """Share the budget already partly consumed by submission and execution."""
        with operation(180):
            clock[0] += 70
            return _backend()._read_query_arrow(job)

    if overrun:
        with pytest.raises(OperationTimeout):
            consume()
    else:
        assert consume().to_pylist() == expected.to_pylist() * 2
    assert timeouts == [110]


def test_bigquery_repeated_read_failures_do_not_extend_the_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated reconnects expire, and a later operation receives a fresh budget."""
    import usagebassoon.deadlines as deadlines

    clock = [0.0]
    monkeypatch.setattr(deadlines, "monotonic", lambda: clock[0])

    def advance(delay: float) -> None:
        """Account for retry backoff in the operation clock."""
        clock[0] += delay

    monkeypatch.setattr(
        "usagebassoon.backends.bigquery.time.sleep",
        advance,
    )
    expected = pa.table({"value": [1]})
    calls: list[float] = []
    fail = True

    class Reader(_StorageReadClient):
        @override
        def read_rows(
            self, *, read_stream: str, offset: int, retry: object, timeout: float
        ) -> Iterable[bigquery_storage_types.ReadRowsResponse]:
            """Consume the shared allowance without producing a response."""
            assert read_stream == "stream" and offset == 0 and retry is None
            calls.append(timeout)
            if fail:
                clock[0] += min(60, timeout)
                raise DeadlineExceeded("stalled read")
            return super().read_rows(
                read_stream=read_stream, offset=offset, retry=retry, timeout=timeout
            )

    reader = Reader("stream", expected)

    def make_reader(*, credentials: object) -> Reader:
        """Use the recording reader without opening a cloud channel."""
        del credentials
        return reader

    monkeypatch.setattr(big_query_read, "BigQueryReadClient", make_reader)
    job = MagicMock(spec=bigquery.QueryJob)
    job.destination = bigquery.TableReference.from_string(
        "usagebassoon-test.usagebassoon_it.result"
    )
    backend = _backend()
    with pytest.raises(OperationTimeout):
        backend._read_query_arrow(job)
    assert len(calls) == 3 and calls[0] > calls[1] > calls[2]
    assert current_deadline() is None
    fail = False
    assert backend._read_query_arrow(job).equals(expected)
    assert calls[-1] == 180


def test_bigquery_upload_retry_rewinds_and_resolves_the_same_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lost acknowledgement cannot submit a second load under a new ID."""
    from io import BytesIO
    from typing import BinaryIO

    import usagebassoon.deadlines as deadlines

    clock = [0.0]
    monkeypatch.setattr(deadlines, "monotonic", lambda: clock[0])

    def advance(delay: float) -> None:
        """Account for retry backoff in the operation clock."""
        clock[0] += delay

    monkeypatch.setattr(
        "google.api_core.retry.retry_unary.time.sleep",
        advance,
    )
    client = MagicMock(spec=bigquery.Client)
    accepted = MagicMock(spec=bigquery.LoadJob)
    client.get_job.return_value = accepted
    identifiers: list[str] = []
    payloads: list[bytes] = []

    def upload(
        payload: BinaryIO, _destination: str, *, job_id: str, **_kwargs: object
    ) -> bigquery.LoadJob:
        """Model an accepted upload whose acknowledgement was lost."""
        identifiers.append(job_id)
        payloads.append(payload.read())
        if len(identifiers) == 1:
            clock[0] += 2
            raise ServiceUnavailable("acknowledgement lost")
        raise Conflict("job already exists")

    client.load_table_from_file.side_effect = upload
    backend = BigQueryBackend("usagebassoon-test", "usagebassoon_it", client=client)
    payload = BytesIO(b"same parquet payload")
    result = backend._submit_load(
        payload,
        "usagebassoon-test.usagebassoon_it.raw_daily_stats",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    assert result is accepted
    assert len(identifiers) == 2 and len(set(identifiers)) == 1
    assert payloads == [b"same parquet payload"] * 2
    assert client.get_job.call_args.args == (identifiers[0],)
    assert backend.is_retryable_error(DeadlineExceeded("deadline"))
    assert backend.is_retryable_error(
        RetryError("exhausted", ServiceUnavailable("busy"))
    )
    assert not backend.is_retryable_error(
        RetryError("invalid", BadRequest("bad query"))
    )


@pytest.mark.parametrize("boundary", ["initial", "midstream"])
def test_bigquery_native_read_deadline_stops_stalls_and_allows_next_operation(
    boundary: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise native gRPC cancellation before and after the first Arrow batch."""
    import grpc
    from google.cloud.bigquery_storage_v1.services.big_query_read.transports import (
        grpc as storage_transport,
    )

    expected = pa.table({"value": [1]})
    serialized_schema = pa.BufferOutputStream()
    with pa.ipc.new_stream(serialized_schema, expected.schema):
        pass
    session = bigquery_storage_types.ReadSession(
        arrow_schema=bigquery_storage_types.ArrowSchema(
            serialized_schema=serialized_schema.getvalue().to_pybytes()
        ),
        streams=[bigquery_storage_types.ReadStream(name="stream")],
    )
    response = bigquery_storage_types.ReadRowsResponse(
        row_count=1,
        arrow_record_batch=bigquery_storage_types.ArrowRecordBatch(
            serialized_record_batch=expected.to_batches()[0].serialize().to_pybytes()
        ),
    )
    release = Event()
    observed: list[int] = []

    def create_session(
        *_args: object, **_kwargs: object
    ) -> bigquery_storage_types.ReadSession:
        """Serve a real Storage Read session over the local gRPC transport."""
        return session

    def read(
        *args: object, **_kwargs: object
    ) -> Generator[bigquery_storage_types.ReadRowsResponse]:
        """Hold the RPC open until the client's native deadline cancels it."""
        request = cast(bigquery_storage_types.ReadRowsRequest, args[0])
        observed.append(request.offset)
        if release.is_set():
            yield response
            return
        if boundary == "midstream":
            yield response
        release.wait(2)

    with ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor)
        server.add_generic_rpc_handlers(
            [
                grpc.method_handlers_generic_handler(
                    "google.cloud.bigquery.storage.v1.BigQueryRead",
                    {
                        "CreateReadSession": grpc.unary_unary_rpc_method_handler(
                            create_session,
                            request_deserializer=bigquery_storage_types.CreateReadSessionRequest.deserialize,
                            response_serializer=bigquery_storage_types.ReadSession.serialize,
                        ),
                        "ReadRows": grpc.unary_stream_rpc_method_handler(
                            read,
                            request_deserializer=bigquery_storage_types.ReadRowsRequest.deserialize,
                            response_serializer=bigquery_storage_types.ReadRowsResponse.serialize,
                        ),
                    },
                )
            ]
        )
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        constructor = big_query_read.BigQueryReadClient

        def reader(*, credentials: object) -> big_query_read.BigQueryReadClient:
            """Connect the real generated client to the isolated local server."""
            del credentials
            return constructor(
                transport=storage_transport.BigQueryReadGrpcTransport(
                    channel=grpc.insecure_channel(f"127.0.0.1:{port}")
                )
            )

        monkeypatch.setattr(big_query_read, "BigQueryReadClient", reader)
        backend = _backend()
        backend.timeout_seconds = 0.2
        job = MagicMock(spec=bigquery.QueryJob)
        job.destination = bigquery.TableReference.from_string(
            "usagebassoon-test.usagebassoon_it.result"
        )
        started = time.monotonic()
        try:
            with pytest.raises(OperationTimeout):
                backend._read_query_arrow(job)
            assert time.monotonic() - started < 2
            assert current_deadline() is None
            release.set()
            assert backend._read_query_arrow(job).equals(expected)
            assert observed == [0, 0]
        finally:
            release.set()
            server.stop(grace=None).wait(timeout=2)


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
        def get_dataset(self, _: str, **_transport: object) -> bigquery.Dataset:
            dataset = bigquery.Dataset("usagebassoon-test.usagebassoon_emulated")
            dataset.location = "US"
            return dataset

        @override
        def get_table(self, table_id: str, **_transport: object) -> bigquery.Table:
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

    def get_dataset(self, _: str, **_transport: object) -> bigquery.Dataset:
        dataset = bigquery.Dataset("usagebassoon-test.usagebassoon_emulated")
        dataset.location = "US"
        return dataset

    def list_tables(self, _: str, **_transport: object) -> list[bigquery.Table]:
        return list(self.tables.values())

    @override
    def get_table(self, table_id: str, **_transport: object) -> bigquery.Table:
        name = table_id.rsplit(".", 1)[-1]
        if name not in self.tables:
            raise NotFound("missing initialization object")
        return bigquery.Table.from_api_repr(self.tables[name].to_api_repr())

    def update_table(
        self, table: bigquery.Table, _: list[str], **_transport: object
    ) -> bigquery.Table:
        self.tables[table.table_id] = table
        return table

    @override
    def query(
        self,
        sql: str,
        *,
        job_config: bigquery.QueryJobConfig,
        location: str,
        **_transport: object,
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

        def list_tables(
            self, dataset: str, **_transport: object
        ) -> list[TableListItem]:
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
            retry: object = None,
            timeout: float | None = None,
            **_transport: object,
        ) -> _Job:
            """Raise the backend error after all restore assertions are assembled."""
            if sql.startswith("SELECT 1 FROM") and "restore_receipts" in sql:
                return _Job()
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

    def create(table: bigquery.Table, **_transport: object) -> bigquery.Table:
        """Record ownership and expiry before a verified Parquet load."""
        assert table.labels["usagebassoon_kind"] == "restore_stage"
        assert table.expires is not None
        staged.append(str(table.reference))
        return table

    monkeypatch.setattr(client, "create_table", create, raising=False)

    def no_stages(_remaining: object) -> list[dict[str, object]]:
        """Start with no abandoned stages in the initialized destination."""
        return []

    monkeypatch.setattr(backend, "_restore_stage_records", no_stages)
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
    assert list(update["update_mask"].paths) == ["params", "schedule", "disabled"]
    assert (
        update["transfer_config"].params["query"]
        == request.transfer_config.params["query"]
    )
    client.list_transfer_configs.return_value = [existing, existing]
    with pytest.raises(RuntimeError, match="multiple UsageBassoon"):
        install_compaction(backend)


def test_restore_wait_observes_terminal_job_before_allowing_stage_cleanup() -> None:
    """Cancellation acknowledgement alone does not establish job termination."""
    backend = _backend()
    job = MagicMock(spec=bigquery.QueryJob)
    job.job_id = "restore-commit"
    job.state = "RUNNING"
    job.result.side_effect = FutureTimeoutError("commit wait expired")
    states: list[str] = []

    def terminal(**_kwargs: object) -> None:
        states.append("terminal")
        job.state = "DONE"

    job.reload.side_effect = terminal
    with pytest.raises(RuntimeError, match="exceeded"):
        backend._wait_restore_job(job)
    assert states == ["terminal"]
    assert job.cancel.called
    assert job.state == "DONE"
    assert job.reload.call_args.kwargs["timeout"] <= 60


def test_unsettled_restore_commit_preserves_owned_stage(
    tmp_path: object,
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timed-out transaction cannot dispose of input while completion is unknown."""
    from pathlib import Path

    import pyarrow.parquet as pq

    assert isinstance(tmp_path, Path)
    backend = _backend()
    client = MagicMock(spec=bigquery.Client)
    backend.client = cast(bigquery.Client, client)
    job = MagicMock(spec=bigquery.QueryJob)
    job.job_id = "uncertain-commit"
    job.state = "RUNNING"
    job.result.side_effect = FutureTimeoutError("lost acknowledgement")
    job.reload.side_effect = OSError("job inspection unavailable")
    client.query.return_value = job
    client.load_table_from_file.return_value = _Job()
    client.list_tables.return_value = list[TableListItem]()

    def no_cleanup() -> None:
        """No previous attempt exists before this transaction is submitted."""

    def no_receipt(_operation: str) -> bool:
        """The transaction has not yet produced an observable receipt."""
        return False

    monkeypatch.setattr(backend, "cleanup_restore_stages", no_cleanup)
    monkeypatch.setattr(backend, "restore_committed", no_receipt)
    path = tmp_path / "daily_stats.parquet"
    pq.write_table(normalize(collection_bundle).tables["daily_stats"], path)
    with pytest.raises(RuntimeError, match="completion could not be determined"):
        backend.restore_snapshot(
            {"daily_stats": path}, operation_id="owned", snapshot_id="snapshot"
        )
    client.create_table.assert_called_once()
    job.reload.assert_called_once()
    client.delete_table.assert_not_called()


def test_absent_receipt_with_active_job_is_unknown_without_surviving_stages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Job discovery does not depend on a surviving stage table."""
    from hashlib import sha256

    backend = _backend()
    client = MagicMock(spec=bigquery.Client)
    backend.client = cast(bigquery.Client, client)
    job = MagicMock(spec=bigquery.QueryJob)
    job.labels = {
        "usagebassoon_kind": "restore_job",
        "usagebassoon_restore": "owned",
        "usagebassoon_destination": sha256(backend.dataset_ref.encode()).hexdigest()[
            :63
        ],
    }
    client.list_jobs.side_effect = [_job_page([]), _job_page([job])]

    def absent(_operation: str, _remaining: object) -> bool:
        return False

    monkeypatch.setattr(backend, "_restore_receipt", absent)
    with pytest.raises(RuntimeError, match="still active"):
        backend.restore_committed("owned")
    client.list_tables.assert_not_called()


def test_cleanup_drains_active_restore_even_when_stages_are_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An interrupted empty restore is still a real transaction to settle."""
    from hashlib import sha256

    backend = _backend()
    client = MagicMock(spec=bigquery.Client)
    backend.client = cast(bigquery.Client, client)
    job = MagicMock(spec=bigquery.QueryJob)
    job.state = "RUNNING"
    job.labels = {
        "usagebassoon_kind": "restore_job",
        "usagebassoon_restore": "owned",
        "usagebassoon_destination": sha256(backend.dataset_ref.encode()).hexdigest()[
            :63
        ],
    }
    client.list_jobs.side_effect = [_job_page([]), _job_page([job])]
    order: list[str] = []

    def terminal(**_kwargs: object) -> None:
        order.append("terminal")
        job.state = "DONE"

    def stages(_remaining: object) -> list[dict[str, object]]:
        assert order == ["terminal"]
        return []

    job.reload.side_effect = terminal
    monkeypatch.setattr(backend, "_restore_stage_records", stages)
    backend.cleanup_restore_stages()
    job.cancel.assert_called_once()
    client.delete_table.assert_not_called()


@pytest.mark.parametrize("qualified", [False, True])
def test_manual_compaction_is_visible_without_a_transfer_schedule(
    monkeypatch: pytest.MonkeyPatch,
    qualified: bool,
) -> None:
    """Restore rejects manual maintenance as well as Scheduled Query transfers."""
    import usagebassoon.backends.bigquery_compaction as module

    backend = _backend()
    client = MagicMock(spec=bigquery.Client)
    backend.client = cast(bigquery.Client, client)
    job = MagicMock(spec=bigquery.QueryJob)
    job.job_id = "manual-compaction"
    job.query = (
        "BEGIN TRANSACTION; UPDATE "
        + (
            f"`{backend.dataset_ref}.compaction_ledger` "
            if qualified
            else "compaction_ledger "
        )
        + "SET compacted_at = CURRENT_TIMESTAMP();"
    )
    job.default_dataset = (
        None
        if qualified
        else bigquery.DatasetReference(backend.project, backend.dataset)
    )
    job.labels = dict[str, str]()
    client.list_jobs.side_effect = [
        _job_page([]),
        _job_page([job]),
        _job_page([]),
        _job_page([job]),
    ]
    transfer = MagicMock(spec=bigquery_datatransfer.DataTransferServiceClient)
    transfer.__enter__.return_value = transfer
    pager = MagicMock()
    pager.pages = [bigquery_datatransfer.ListTransferConfigsResponse()]
    transfer.list_transfer_configs.return_value = pager
    monkeypatch.setattr(
        bigquery_datatransfer,
        "DataTransferServiceClient",
        MagicMock(return_value=transfer),
    )
    with pytest.raises(RuntimeError, match="manual-compaction"):
        module.pause_compaction(backend)
    enabled, detail = module.compaction_status(backend)
    assert not enabled and "manual-compaction" in detail


def test_restore_requires_project_wide_job_visibility_before_deleting_stages() -> None:
    """Permission failure cannot be interpreted as an idle project."""
    from google.api_core.exceptions import Forbidden

    backend = _backend()
    client = MagicMock(spec=bigquery.Client)
    backend.client = cast(bigquery.Client, client)
    client.list_jobs.side_effect = Forbidden("project jobs are not visible")
    with pytest.raises(RuntimeError, match=r"bigquery.jobs.listAll"):
        backend.cleanup_restore_stages()
    client.delete_table.assert_not_called()


def test_recovery_schedule_pause_waits_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disable future runs and wait for active work without changing query contents."""
    import usagebassoon.backends.bigquery_compaction as module

    backend = _backend()
    config = bigquery_datatransfer.TransferConfig(
        name="projects/1/locations/us/transferConfigs/1",
        display_name=f"UsageBassoon nightly compaction: {backend.dataset}",
        data_source_id="scheduled_query",
        disabled=False,
    )
    client = MagicMock(spec=bigquery_datatransfer.DataTransferServiceClient)
    client.__enter__.return_value = client
    config_page = MagicMock()
    config_page.pages = [
        bigquery_datatransfer.ListTransferConfigsResponse(transfer_configs=[config])
    ]
    client.list_transfer_configs.return_value = config_page
    run = bigquery_datatransfer.TransferRun(
        state=bigquery_datatransfer.TransferState.RUNNING
    )
    running_page = MagicMock()
    running_page.pages = [
        bigquery_datatransfer.ListTransferRunsResponse(transfer_runs=[run])
    ]
    finished_page = MagicMock()
    finished_page.pages = [bigquery_datatransfer.ListTransferRunsResponse()]
    client.list_transfer_runs.side_effect = [running_page, finished_page]
    monkeypatch.setattr(
        bigquery_datatransfer,
        "DataTransferServiceClient",
        MagicMock(return_value=client),
    )

    def no_sleep(_seconds: float) -> None:
        """Keep bounded-wait behavior deterministic without wall-clock pauses."""

    monkeypatch.setattr(module.time, "sleep", no_sleep)
    with pytest.raises(RuntimeError, match="run remains active"):
        module.pause_compaction(backend)
    module.pause_compaction(backend)
    update = client.update_transfer_config.call_args.kwargs
    assert update["transfer_config"].disabled is True
    assert list(update["update_mask"].paths) == ["disabled"]
    assert client.list_transfer_runs.call_count == 2
    client.list_transfer_runs.side_effect = None
    client.list_transfer_runs.return_value = running_page
    clock = [0.0]

    def tick() -> float:
        clock[0] += 0.3
        return clock[0]

    monkeypatch.setattr(module.time, "monotonic", tick)
    with pytest.raises(RuntimeError, match=r"deadline exceeded|run remains active"):
        module.pause_compaction(backend, timeout=1.0)


def test_bigquery_snapshot_stream_handles_empty_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bounded Arrow streams ignore zero-row pages while retaining real observations."""
    client = MagicMock(spec=bigquery.Client)
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_emulated",
        client=cast(bigquery.Client, client),
    )
    stamp = datetime.now(UTC)
    timestamp_job = MagicMock(spec=bigquery.QueryJob)
    timestamp_job.result.return_value = [{"captured_at": stamp}]
    data_job = MagicMock(spec=bigquery.QueryJob)
    schema = CANONICAL_TABLE_SCHEMAS["notes"]
    empty = pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in schema], schema=schema
    )
    data = pa.Table.from_pylist(
        [
            {
                "event_id": "event",
                "note_id": note_id_for_session("source", "codex", "session"),
                "source_id": "source",
                "client": "codex",
                "session_id": "session",
                "note": "note",
                "created_at": stamp,
                "updated_at": stamp,
                "collected_at": stamp,
                "op": "upsert",
            }
        ],
        schema=schema,
    ).to_batches()[0]

    def result_batches(_job: bigquery.QueryJob) -> Generator[pa.RecordBatch]:
        """Provide bounded Arrow batches without opening a cloud transport."""
        yield empty
        yield data

    monkeypatch.setattr(backend, "_read_query_batches", result_batches)
    client.query.side_effect = (timestamp_job, data_job)
    with backend.stream_snapshot(("notes",)) as captured:
        assert captured.captured_at == stamp
        batches = list(captured.tables["notes"])
        assert len(batches) == 1 and batches[0].num_rows == 1
        assert batches[0].schema.equals(schema)


def _views() -> dict[str, str]:
    """Read canonical view definitions for SQL-generation regression coverage."""
    statements = sqlglot.parse(
        resources.files("usagebassoon.sql.bigquery").joinpath("views.sql").read_text(),
        read="bigquery",
    )
    return {
        statement.this.name: statement.expression.sql(dialect="bigquery")
        for statement in statements
        if isinstance(statement, exp.Create) and statement.expression is not None
    }


@pytest.mark.parametrize(
    "relation",
    (
        *REQUIRED_RELATIONS,
        "open_schema_drift_events",
        "open_reconciliation_issues",
        "compaction_backlog",
    ),
)
def test_pinned_diagnostics_expand_every_installed_view_input(relation: str) -> None:
    """Cover nested gold/raw joins, ledger, debug, and backlog dependencies."""
    sql = _pinned_query(
        f"SELECT * FROM {relation}", _views(), "usagebassoon-test.usagebassoon_it"
    )
    tree = sqlglot.parse_one(sql, read="bigquery")
    tables = list(tree.find_all(exp.Table))
    ctes = {cte.alias for cte in tree.find_all(exp.CTE)}
    assert tables
    for table in tables:
        if table.catalog:
            assert table.catalog == "usagebassoon-test"
            assert table.db == "usagebassoon_it"
            assert table.args.get("version") is not None
        else:
            assert table.name in ctes
    assert "CURRENT_TIMESTAMP" not in sql
    assert "CURRENT_DATE" not in sql
    assert "@doctor_read_at" in sql


def test_pinned_query_distinguishes_cte_from_same_named_physical_table() -> None:
    """Only the CTE's actual raw-table input receives time travel."""
    sql = _pinned_query(
        "WITH daily_stats AS (SELECT * FROM raw_daily_stats) SELECT * FROM daily_stats",
        {},
        "usagebassoon-test.usagebassoon_it",
    )
    tree = sqlglot.parse_one(sql, read="bigquery")
    inputs = {table.name: table for table in tree.find_all(exp.Table)}
    assert inputs["daily_stats"].args.get("version") is None
    assert inputs["raw_daily_stats"].args.get("version") is not None


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM daily_stats",
        "SELECT * FROM missing_view",
        "SELECT * FROM other.dataset.daily_stats",
    ],
)
def test_pinned_query_fails_closed(sql: str) -> None:
    """Never silently replace an unavailable or foreign relation with live reads."""
    with pytest.raises(ValueError):
        _pinned_query(sql, {}, "usagebassoon-test.usagebassoon_it")


def test_bigquery_read_session_uses_first_server_timestamp_without_leaking_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Later queries share one timestamp and the original backend stays unpinned."""
    stamp = datetime(2026, 10, 1, 22, 0, tzinfo=UTC)
    client = MagicMock(spec=bigquery.Client)
    job = client.query.return_value
    job.result.return_value = [
        {
            "captured_at": stamp,
            "views": [
                {
                    "table_name": "installed",
                    "view_definition": "SELECT * FROM daily_stats",
                }
            ],
        }
    ]
    backend = BigQueryBackend("usagebassoon-test", "usagebassoon_it", client=client)

    def arrow_result(_job: bigquery.QueryJob) -> pa.Table:
        """Avoid the transport while recording bound query jobs."""
        return pa.table({})

    monkeypatch.setattr(backend, "_read_query_arrow", arrow_result)
    with backend.consistent_read() as read:
        read.query("SELECT * FROM installed")
        read.query("SELECT * FROM raw_daily_stats")
    calls = client.query.call_args_list
    assert "CURRENT_TIMESTAMP() AS captured_at" in calls[0].args[0]
    for call in calls[1:]:
        assert "FOR SYSTEM_TIME AS OF @doctor_read_at" in call.args[0]
        parameters = call.kwargs["job_config"].query_parameters
        assert len(parameters) == 1 and parameters[0].value == stamp
    backend.query("SELECT * FROM daily_stats")
    assert "FOR SYSTEM_TIME" not in client.query.call_args.args[0]


def test_cleanup_waits_for_background_job_before_deleting_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A running compactor delays schema removal until its job is terminal."""
    client = MagicMock(spec=bigquery.Client)
    client.location = "US"
    job_results: list[list[dict[str, str]]] = [[{"job_id": "compactor"}], []]
    client.query.return_value.result.side_effect = job_results
    table = MagicMock()
    table.table_type = "TABLE"
    client.list_tables.return_value = [table]
    sleeps: list[float] = []
    monkeypatch.setattr("tests.test_backend_bigquery_live.time.sleep", sleeps.append)
    _reset_test_schema(client, "usagebassoon-test.usagebassoon_it")
    assert sleeps and client.query.call_count == 2
    names = [call[0] for call in client.mock_calls]
    assert names.index("delete_table") > max(
        i for i, name in enumerate(names) if name == "query"
    )


def test_cleanup_timeout_leaves_schema_intact(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failure to establish quiescence must never drop or truncate tables."""
    client = MagicMock(spec=bigquery.Client)
    client.location = "US"
    client.query.return_value.result.return_value = [{"job_id": "compactor"}]
    clock = iter([0.0, 0.0, 601.0, 601.0])
    monkeypatch.setattr(
        "tests.test_backend_bigquery_live.time.monotonic", lambda: next(clock)
    )
    sleeps: list[float] = []
    monkeypatch.setattr("tests.test_backend_bigquery_live.time.sleep", sleeps.append)
    with pytest.raises(TimeoutError):
        _reset_test_schema(client, "usagebassoon-test.usagebassoon_it")
    client.list_tables.assert_not_called()
    client.delete_table.assert_not_called()


def test_schedule_drain_disables_launches_and_waits_for_pending_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cloud transfers can remain queued after the test body has finished."""
    client = MagicMock(spec=bigquery_datatransfer.DataTransferServiceClient)
    runs: list[list[bigquery_datatransfer.TransferRun]] = [
        [
            bigquery_datatransfer.TransferRun(
                state=bigquery_datatransfer.TransferState.PENDING
            )
        ],
        [],
    ]
    client.list_transfer_runs.side_effect = runs
    sleeps: list[float] = []
    monkeypatch.setattr("tests.test_backend_bigquery_live.time.sleep", sleeps.append)
    _drain_compaction_schedule(client, "projects/1/locations/us/transferConfigs/1")
    assert sleeps and client.list_transfer_runs.call_count == 2
    request = client.list_transfer_runs.call_args.kwargs["request"]
    assert set(request.states) == {
        bigquery_datatransfer.TransferState.PENDING,
        bigquery_datatransfer.TransferState.RUNNING,
    }
    config = client.update_transfer_config.call_args.kwargs["transfer_config"]
    assert config.disabled


def test_doctor_snapshot_failure_never_falls_back_to_live_reads() -> None:
    """Report initialization failure instead of silently weakening consistency."""
    backend = MagicMock(spec=BigQueryBackend)
    backend.consistent_read.return_value.__enter__.side_effect = RuntimeError(
        "snapshot unavailable"
    )
    report = run_doctor(backend, backend_name="bigquery", database="usagebassoon_it")
    assert report.status == "error"
    assert "snapshot unavailable" in report.errors[0].message
    backend.query.assert_not_called()


def test_restore_cleanup_discards_nonexpired_owned_stages_after_job_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry preparation waits for terminal jobs instead of waiting for stage expiry."""
    backend = _backend()
    table = bigquery.Table(f"{backend.dataset_ref}._stage_notes_attempt")
    operation = "11111111-1111-4111-8111-111111111111"
    table.labels = {
        "usagebassoon_kind": "restore_stage",
        "usagebassoon_restore": operation,
    }
    table.expires = datetime.now(UTC) + timedelta(days=1)
    stage: dict[str, object] = {
        "table": str(table.reference),
        "operation_id": operation,
        "expires_at": table.expires,
    }

    def owned_stages(_remaining: object) -> list[dict[str, object]]:
        """Expose the crashed attempt's nonexpired stage."""
        return [stage]

    monkeypatch.setattr(backend, "_restore_stage_records", owned_stages)

    def uncommitted(_operation: str, _remaining: object) -> bool:
        """No application transaction has committed in this interrupted attempt."""
        return False

    monkeypatch.setattr(backend, "_restore_receipt", uncommitted)
    client = MagicMock(spec=bigquery.Client)
    backend.client = cast(bigquery.Client, client)
    job = MagicMock(spec=bigquery.QueryJob)
    from hashlib import sha256

    job.labels = {
        "usagebassoon_kind": "restore_job",
        "usagebassoon_restore": operation,
        "usagebassoon_destination": sha256(backend.dataset_ref.encode()).hexdigest()[
            :63
        ],
    }
    job.state = "RUNNING"

    def finish(**_kwargs: object) -> None:
        job.state = "DONE"

    job.reload.side_effect = finish
    client.list_jobs.side_effect = [_job_page([job]), _job_page([])]
    client.get_table.return_value = table
    backend.cleanup_restore_stages()
    job.cancel.assert_called_once()
    job.reload.assert_called_once()
    client.delete_table.assert_called_once()
    assert client.delete_table.call_args.args[0] == table.reference


def test_recovery_schedule_pagination_reapplies_remaining_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pagination cannot reset discovery's operation deadline on each request."""
    import usagebassoon.backends.bigquery_compaction as module

    backend = _backend()
    client = MagicMock(spec=bigquery_datatransfer.DataTransferServiceClient)
    client.__enter__.return_value = client
    clock = [0.0]
    calls: list[tuple[str, float]] = []

    def list_configs(
        *, request: dict[str, str], retry: object, timeout: float
    ) -> MagicMock:
        assert retry is None
        calls.append((request["page_token"], timeout))
        clock[0] += 20.0
        response = bigquery_datatransfer.ListTransferConfigsResponse(
            next_page_token="next" if len(calls) == 1 else ""
        )
        pager = MagicMock()
        pager.pages = [response]
        return pager

    client.list_transfer_configs.side_effect = list_configs
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        bigquery_datatransfer,
        "DataTransferServiceClient",
        MagicMock(return_value=client),
    )
    module.pause_compaction(backend, timeout=60)
    assert calls == [("", 60.0), ("next", 40.0)]
    client.update_transfer_config.assert_not_called()
