# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""bigquery.py — Append publication and transactional BigQuery maintenance."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from contextvars import copy_context
from copy import copy
from datetime import UTC, datetime, timedelta
from functools import wraps
from hashlib import sha256
from importlib import resources
from itertools import pairwise
from math import isfinite
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile
from typing import BinaryIO, cast, override
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import sqlglot
from google.api_core.exceptions import (
    BadRequest,
    Conflict,
    DeadlineExceeded,
    GoogleAPICallError,
    NotFound,
    RetryError,
    Unknown,
)
from google.api_core.retry import Retry, exponential_sleep_generator, if_transient_error
from google.auth import default as default_credentials
from google.auth.credentials import Credentials
from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery
from google.cloud.bigquery_storage_v1 import types as bigquery_storage_types
from google.cloud.bigquery_storage_v1.services import big_query_read
from google.oauth2 import service_account
from pandas_gbq.arrow import from_read_rows_response
from requests import Session
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.tokens import TokenType

from usagebassoon.backends.base import (
    AbstractStorageBackend,
    ActiveTransaction,
    BatchPersistResult,
    CuratedIdentity,
    CuratedRenameResult,
    PersistenceBatch,
    SnapshotRead,
    SnapshotStream,
    StorageBackend,
    UpsertResult,
    is_simple_identifier,
)
from usagebassoon.deadlines import (
    RESTORE_SECONDS,
    SNAPSHOT_SECONDS,
    OperationTimeout,
    bounded,
    cleanup_budget,
    http_session,
    remaining_seconds,
)
from usagebassoon.deadlines import (
    operation as operation_budget,
)
from usagebassoon.schema_assets import (
    SCHEMA_VERSION,
    pending_migrations,
    schema_hash,
)
from usagebassoon.storage_model import (
    CANONICAL_TABLE_SCHEMAS,
    DEBUG_TABLES,
    EVENT_KEYS,
    SNAPSHOT_TABLES,
    STATE_KEYS,
)

_PROJECT_ID = re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]\Z")
_DATASET_ID = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,1023}\Z")
_LOCATION_ID = re.compile(r"[A-Za-z][A-Za-z0-9-]{0,62}\Z")
_LOG = logging.getLogger("usagebassoon")
_CONCURRENT_TRANSACTION_MESSAGE = "transaction is aborted due to concurrent update"
_VIEW_RELATIONS = tuple(STATE_KEYS) + tuple(EVENT_KEYS)
_READ_TIMESTAMP_PARAMETER = "doctor_read_at"


def _pinned_query(sql: str, views: Mapping[str, str], dataset_ref: str) -> str:
    """Expand installed views and pin their physical inputs and freshness clock.

    Args:
        sql: Read-only BigQuery query.
        views: Installed view definitions captured with the read timestamp.
        dataset_ref: Fully qualified backend dataset.

    Returns:
        SQL using one timestamp parameter for all physical reads.

    Raises:
        ValueError: If a query writes, uses foreign tables, or has cyclic views.
    """
    project, dataset = dataset_ref.split(".")
    physical = (
        set(STATE_KEYS)
        | {"raw_" + name for name in set(STATE_KEYS) | DEBUG_TABLES}
        | {
            "collection_ledger",
            "compaction_ledger",
            "schema_marker",
            "schema_migrations",
        }
    )

    def expand(statement: str, ancestors: tuple[str, ...]) -> exp.Query:
        """Resolve view dependencies without confusing CTEs with base tables."""
        tree = sqlglot.parse_one(statement, read="bigquery")
        if not isinstance(tree, exp.Query):
            raise ValueError("consistent reads require a SELECT query")
        for scope in traverse_scope(tree):
            for source in scope.sources.values():
                if not isinstance(source, exp.Table):
                    continue
                if source.catalog not in {"", project} or source.db not in {
                    "",
                    dataset,
                }:
                    raise ValueError("consistent reads require backend dataset tables")
                name = source.name
                if name in views:
                    if name in ancestors:
                        raise ValueError(f"cyclic installed view {name!r}")
                    definition = expand(views[name], (*ancestors, name))
                    source.replace(definition.subquery(alias=source.alias_or_name))
                elif name in physical:
                    source.set("catalog", exp.to_identifier(project, quoted=True))
                    source.set("db", exp.to_identifier(dataset, quoted=True))
                    source.set(
                        "version",
                        exp.Version(
                            this="TIMESTAMP",
                            kind="AS OF",
                            expression=exp.Parameter(
                                this=exp.Var(this=_READ_TIMESTAMP_PARAMETER)
                            ),
                        ),
                    )
                else:
                    raise ValueError(f"unavailable diagnostic relation {name!r}")
        return tree

    tree = expand(sql, ())

    def pin_clock(node: exp.Expression) -> exp.Expression:
        """Keep retention and arrival-age calculations at the same read instant."""
        stamp = exp.Parameter(this=exp.Var(this=_READ_TIMESTAMP_PARAMETER))
        if isinstance(node, exp.CurrentTimestamp):
            return stamp
        if isinstance(node, exp.CurrentDate):
            return exp.Date(this=stamp, zone=node.this)
        return node

    return tree.transform(pin_clock).sql(dialect="bigquery")


class _JobTimeout(OperationTimeout):
    """A job wait expired; replaying a publication keeps its event identifiers."""


class _RestoreUncertain(RuntimeError):
    """A submitted restore job has not been observed in a terminal state."""


def _transient_request_error(error: Exception) -> bool:
    """Recognize transport failures that permit replay with stable identities."""
    return if_transient_error(error) or isinstance(
        error, (OperationTimeout, DeadlineExceeded)
    )


class _RequestRetry(Retry):
    """Report retry exhaustion as an operation timeout, including backend startup."""

    @override
    def __call__[**P, T](
        self,
        func: Callable[P, T],
        on_error: Callable[[Exception], object] | None = None,
    ) -> Callable[P, T]:
        """Preserve the SDK retry interface and the shared timeout exception."""
        retried = super().__call__(func, on_error)

        @wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            """Keep exhausted startup requests eligible for a new attempt."""
            try:
                return retried(*args, **kwargs)
            except RetryError as error:
                if not _transient_request_error(error.cause):
                    raise
                raise OperationTimeout(
                    "BigQuery request could not recover within the operation budget"
                ) from error

        return wrapped


def _request_retry(remaining: Callable[[], float] | None = None) -> Retry:
    """Retry transient RPC failures without extending the enclosing deadline."""
    allowance = remaining or (lambda: remaining_seconds(None))

    def retryable(error: Exception) -> bool:
        """Stop recovery before an expired operation issues another request."""
        if not _transient_request_error(error):
            return False
        allowance()
        return True

    def report(error: Exception) -> None:
        """Record handled transport failures without logging request payloads."""
        _LOG.warning(
            "transient BigQuery request failed; retrying within operation budget",
            exc_info=(type(error), error, error.__traceback__),
        )

    return _RequestRetry(
        predicate=retryable,
        initial=0.5,
        maximum=5.0,
        timeout=allowance(),
        on_error=report,
    )


def _validate_project(project: str) -> None:
    """Require a canonical GCP project identifier."""
    if not _PROJECT_ID.fullmatch(project):
        raise ValueError(f"invalid BigQuery project identifier: {project!r}")


def _validate_dataset(dataset: str) -> None:
    """Require a canonical BigQuery dataset identifier."""
    if not _DATASET_ID.fullmatch(dataset):
        raise ValueError(f"invalid BigQuery dataset identifier: {dataset!r}")


def _validate_location(location: str) -> None:
    """Require a safe canonical BigQuery location identifier."""
    if _LOCATION_ID.fullmatch(location) is None:
        raise ValueError(f"invalid BigQuery location identifier: {location!r}")


def _schema_from_arrow(data: pa.Table) -> list[bigquery.SchemaField]:
    """Map the canonical Arrow table schema to an explicit BigQuery schema.

    Args:
        data: Normalized Arrow table to load.

    Returns:
        BigQuery schema fields preserving Arrow's supported logical types.

    Raises:
        ValueError: If a normalized table has an unsupported Arrow type.
    """
    fields: list[bigquery.SchemaField] = []
    for field in data.schema:
        if not is_simple_identifier(field.name):
            raise ValueError(f"invalid Arrow field name {field.name!r}")
        arrow_type = field.type
        mode = "NULLABLE" if field.nullable else "REQUIRED"
        if pa.types.is_list(arrow_type):
            if not pa.types.is_string(arrow_type.value_type):
                raise ValueError(
                    f"unsupported repeated Arrow type for {field.name!r}: {arrow_type}"
                )
            kind = "STRING"
            mode = "REPEATED"
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            kind = "STRING"
        elif pa.types.is_boolean(arrow_type):
            kind = "BOOL"
        elif pa.types.is_integer(arrow_type) or pa.types.is_unsigned_integer(
            arrow_type
        ):
            kind = "INT64"
        elif pa.types.is_floating(arrow_type):
            kind = "FLOAT64"
        elif pa.types.is_date(arrow_type):
            kind = "DATE"
        elif pa.types.is_timestamp(arrow_type):
            kind = "TIMESTAMP"
        else:
            raise ValueError(f"unsupported Arrow type for {field.name!r}: {arrow_type}")
        fields.append(bigquery.SchemaField(field.name, kind, mode=mode))
    return fields


class BigQueryBackend(AbstractStorageBackend):
    """Append-only publication with asynchronous transactional gold compaction.

    Implements the StorageBackend protocol.
    """

    dialect = "bigquery"

    @property
    @override
    def max_concurrent_queries(self) -> int:
        """Return the bounded read-query concurrency supported by this client."""
        return 8

    def __init__(
        self,
        project: str,
        dataset: str,
        *,
        location: str = "US",
        credentials: Credentials | None = None,
        credentials_file: Path | None = None,
        maximum_bytes_billed: int = 1_073_741_824,
        timeout_seconds: float = 180.0,
        client: bigquery.Client | None = None,
    ) -> None:
        """Create a backend bound to a validated BigQuery dataset.

        Args:
            project: GCP project identifier.
            dataset: BigQuery dataset name.
            location: Required dataset and job location.
            credentials: Explicit Google credentials, normally for tests.
            credentials_file: Service-account JSON file, preferred over ADC.
            maximum_bytes_billed: Billing cap applied to query jobs.
            timeout_seconds: Complete operation budget; cleanup adds at most 15s.
            client: Injected client for offline unit tests.

        Raises:
            ValueError: If identifiers or credential sources conflict.
        """
        _validate_project(project)
        _validate_dataset(dataset)
        _validate_location(location)
        if maximum_bytes_billed < 1:
            raise ValueError("maximum_bytes_billed must be positive")
        if not isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if credentials is not None and credentials_file is not None:
            raise ValueError("credentials and credentials_file cannot be combined")
        with operation_budget(timeout_seconds):
            resolved_credentials = credentials
            if credentials_file is not None:
                try:
                    resolved_credentials = (
                        service_account.Credentials.from_service_account_file(
                            str(credentials_file),
                            scopes=["https://www.googleapis.com/auth/cloud-platform"],
                        )
                    )
                except (GoogleAuthError, OSError, ValueError) as error:
                    raise RuntimeError(
                        "BigQuery credentials file could not be loaded"
                    ) from error
            self._read_at: datetime | None = None
            self._read_views: dict[str, str] = {}
            self.project = project
            self.dataset = dataset
            self.location = location
            self.maximum_bytes_billed = maximum_bytes_billed
            self.timeout_seconds = timeout_seconds
            self.dataset_ref = f"{project}.{dataset}"
            try:
                if resolved_credentials is None and client is None:
                    from google.auth.transport.requests import Request

                    resolved_credentials, _ = default_credentials(
                        request=Request(session=cast(Session, http_session())),
                        scopes=["https://www.googleapis.com/auth/cloud-platform"],
                    )
                self._credentials = resolved_credentials
                self.client = client or bigquery.Client(
                    project=project,
                    credentials=resolved_credentials,
                    location=location,
                    _http=cast("Session", http_session(resolved_credentials)),
                )
            except GoogleAuthError as error:
                raise RuntimeError(
                    "BigQuery authentication failed; configure ADC or credentials_file"
                ) from error

    @contextmanager
    @override
    def consistent_read(self) -> Generator[StorageBackend]:
        """Capture server time and installed views in the first diagnostic query.

        Concurrent workers share a scoped copy, leaving ordinary backend reads
        unaffected. Transaction-job metadata remains a live operational check.
        """
        with operation_budget(self.timeout_seconds):
            rows = self._wait_for_job(
                self.client.query(
                    "SELECT CURRENT_TIMESTAMP() AS captured_at, ARRAY("
                    "SELECT AS STRUCT table_name, view_definition FROM "
                    f"`{self.dataset_ref}.INFORMATION_SCHEMA.VIEWS`) AS views",
                    job_config=self._query_config(),
                    location=self.location,
                    timeout=remaining_seconds(),
                    retry=_request_retry(),
                    job_retry=None,
                )
            )
            row = next(iter(rows))
            timestamp = row["captured_at"]
            definitions = row["views"]
            if not isinstance(timestamp, datetime) or not isinstance(definitions, list):
                raise RuntimeError(
                    "BigQuery did not return diagnostic snapshot metadata"
                )
            views: dict[str, str] = {}
            for definition in definitions:
                if not isinstance(definition, Mapping):
                    raise RuntimeError(
                        "BigQuery returned invalid installed view metadata"
                    )
                name, sql = (
                    definition.get("table_name"),
                    definition.get("view_definition"),
                )
                if not isinstance(name, str) or not isinstance(sql, str):
                    raise RuntimeError(
                        "BigQuery returned invalid installed view metadata"
                    )
                views[name] = sql
            scoped = copy(self)
            scoped._read_at = timestamp
            scoped._read_views = views
        yield scoped

    @bounded
    def _wait_for_job(
        self, job: bigquery.job.QueryJob | bigquery.job.LoadJob
    ) -> Iterable[Mapping[str, object]]:
        """Await one remote job for a bounded period and cancel a timeout.

        Args:
            job: Submitted BigQuery query or load job.

        Returns:
            The completed BigQuery result iterator.

        Raises:
            RuntimeError: If the job does not finish within the configured bound.
        """
        try:
            return cast(
                Iterable[Mapping[str, object]],
                job.result(
                    timeout=remaining_seconds(120.0),
                    retry=_request_retry(),
                ),
            )
        except FutureTimeoutError as error:
            job_id = job.job_id or "unknown"
            try:
                with cleanup_budget():
                    job.cancel(
                        timeout=remaining_seconds(),
                        retry=_request_retry(),
                    )
            except Exception:
                _LOG.exception("could not cancel timed-out BigQuery job %s", job_id)
            raise _JobTimeout(
                f"BigQuery job {job_id} wait timed out; "
                "completion may be uncertain after attempted cancellation"
            ) from error

    def _table_id(self, table: str) -> str:
        """Return an unquoted fully qualified table ID for BigQuery APIs."""
        if not is_simple_identifier(table):
            raise ValueError(f"invalid BigQuery table identifier: {table!r}")
        return f"{self.dataset_ref}.{table}"

    def _table_ref(self, table: str) -> str:
        """Return a safely quoted fully qualified table reference for SQL."""
        return f"`{self._table_id(table)}`"

    def _query_config(
        self,
        *,
        parameters: list[bigquery.ScalarQueryParameter] | None = None,
    ) -> bigquery.QueryJobConfig:
        """Build the fixed Standard SQL configuration for backend jobs."""
        return bigquery.QueryJobConfig(
            default_dataset=self.dataset_ref,
            maximum_bytes_billed=self.maximum_bytes_billed,
            query_parameters=parameters or [],
        )

    @override
    def is_retryable_error(self, error: Exception) -> bool:
        """Retry transient publication failures without changing observation IDs."""
        if isinstance(error, RetryError):
            return self.is_retryable_error(error.cause)
        return _transient_request_error(error)

    @override
    @bounded
    def compaction_backlog(self) -> pa.Table | None:
        """Read every overdue arrival bucket through the installed health view."""
        return self.query(
            "SELECT domain, arrival_day, pending_rows, age_days "
            "FROM compaction_backlog WHERE age_days >= 2 "
            "ORDER BY age_days DESC"
        )

    @override
    @bounded
    def active_transactions(self, limit: int) -> tuple[ActiveTransaction, ...]:
        """Return running transaction jobs that mutate this configured dataset.

        Args:
            limit: Maximum jobs to return.

        Returns:
            Active transaction identifiers ordered by start time.

        Raises:
            RuntimeError: If BigQuery cannot read project job metadata.
            ValueError: If ``limit`` is not positive.
        """
        if limit < 1:
            raise ValueError("limit must be positive")
        jobs_view = (
            f"`{self.project}`.`region-{self.location.casefold()}`."
            "INFORMATION_SCHEMA.JOBS_BY_PROJECT"
        )
        statement = (
            "SELECT job_id, transaction_id "
            f"FROM {jobs_view} "
            "WHERE state = 'RUNNING' "
            "AND transaction_id IS NOT NULL "
            f"AND query LIKE '%`{self.dataset_ref}.%' "
            "ORDER BY creation_time DESC "
            f"LIMIT {limit}"
        )
        try:
            rows = self._wait_for_job(
                self.client.query(
                    statement,
                    job_config=self._query_config(),
                    location=self.location,
                    timeout=remaining_seconds(),
                    retry=_request_retry(),
                    job_retry=None,
                )
            )
        except GoogleAPICallError as error:
            raise RuntimeError(
                "BigQuery active-transaction inspection failed"
            ) from error
        return tuple(
            ActiveTransaction(str(row["job_id"]), str(row["transaction_id"]))
            for row in rows
            if row["job_id"] is not None and row["transaction_id"] is not None
        )

    def _qualify_view_sql(self, sql: str) -> str:
        """Qualify known packaged relations without changing explicit aliases."""
        relations = (
            set(STATE_KEYS)
            | set(EVENT_KEYS)
            | {"compaction_ledger", "schema_migrations", "schema_marker"}
        )
        relations |= {"raw_" + table for table in set(STATE_KEYS) | DEBUG_TABLES}
        package = resources.files("usagebassoon.sql.bigquery")
        relations |= set(
            re.findall(
                r"CREATE OR REPLACE VIEW ([A-Za-z_][A-Za-z0-9_]*)",
                package.joinpath("views.sql").read_text(),
            )
        )
        pattern = re.compile(
            r"\b(CREATE\s+OR\s+REPLACE\s+VIEW|FROM|JOIN|INTO|UPDATE)\s+"
            r"([A-Za-z_][A-Za-z0-9_]*)\b",
            flags=re.IGNORECASE,
        )

        def qualify(match: re.Match[str]) -> str:
            """Qualify one recognized warehouse relation."""
            if match[2] in relations:
                return f"{match[1]} {self._table_ref(match[2])}"
            return match[0]

        return pattern.sub(qualify, sql)

    @override
    @bounded
    def apply_ddl(self) -> None:
        """Initialize explicitly, including safe replay of an interrupted init."""
        try:
            actual = self.client.get_dataset(
                self.dataset_ref,
                timeout=remaining_seconds(),
                retry=_request_retry(),
            )
        except NotFound:
            actual = bigquery.Dataset(self.dataset_ref)
            actual.location = self.location
            actual = self.client.create_dataset(
                actual,
                timeout=remaining_seconds(),
                retry=_request_retry(),
            )
        if str(actual.location).casefold() != self.location.casefold():
            raise ValueError("configured BigQuery location does not match the dataset")
        try:
            marker = self.client.get_table(
                self._table_id("schema_marker"),
                timeout=remaining_seconds(),
                retry=_request_retry(),
            )
        except NotFound:
            marker = None
        labels = marker.labels or {} if marker is not None else {}
        initializing = labels.get("usagebassoon_initializing") == "true"
        if labels.get("usagebassoon_schema_version") and not initializing:
            self.preflight()
            return
        package = resources.files("usagebassoon.sql.bigquery")
        native = package.joinpath("ddl.sql").read_text()
        definitions = [
            statement
            for statement in sqlglot.parse(native, read="bigquery")
            if statement is not None
        ]
        existing: set[str] = {
            str(table.table_id)
            for table in self.client.list_tables(
                self.dataset_ref,
                timeout=remaining_seconds(),
                retry=_request_retry(),
            )
        }
        if initializing:
            if labels.get("usagebassoon_schema_hash") != schema_hash("bigquery"):
                raise RuntimeError("interrupted init belongs to a different schema")
        elif existing - {"schema_marker"}:
            raise RuntimeError(
                "uninitialized dataset contains tables; initialize an empty dataset"
            )
        if marker is None:
            marker_sql = next(
                statement.sql(dialect="bigquery")
                for statement in definitions
                if isinstance(statement, exp.Create)
                and statement.this.this.name == "schema_marker"
            )
            self._wait_for_job(
                self.client.query(
                    marker_sql,
                    job_config=self._query_config(),
                    location=self.location,
                    timeout=remaining_seconds(),
                    retry=_request_retry(),
                    job_retry=None,
                )
            )
            marker = self.client.get_table(
                self._table_id("schema_marker"),
                timeout=remaining_seconds(),
                retry=_request_retry(),
            )
            existing.add("schema_marker")
        marker.labels = {
            "usagebassoon_schema_version": str(SCHEMA_VERSION),
            "usagebassoon_schema_hash": schema_hash("bigquery"),
            "usagebassoon_initializing": "true",
        }
        marker = self.client.update_table(
            marker,
            ["labels"],
            timeout=remaining_seconds(),
            retry=_request_retry(),
        )
        pending: list[str] = []
        for statement in definitions:
            if isinstance(statement, exp.Create):
                name = statement.this.this.name
                if name in existing:
                    self._check_existing_definition(statement)
                else:
                    pending.append(statement.sql(dialect="bigquery"))
            elif isinstance(statement, exp.Insert):
                # Only the compaction singleton is seeded by the baseline.
                row = statement.expression.expressions[0]
                values = ", ".join(
                    value.sql(dialect="bigquery") for value in row.expressions
                )
                pending.append(
                    "INSERT INTO compaction_ledger SELECT "
                    + values
                    + " FROM UNNEST([1]) WHERE NOT EXISTS("
                    "SELECT 1 FROM compaction_ledger "
                    "WHERE domain = '__lock__')"
                )
            else:
                raise RuntimeError("unsupported statement in initialization baseline")
        if pending:
            self._wait_for_job(
                self.client.query(
                    ";\n".join(pending),
                    job_config=self._query_config(),
                    location=self.location,
                    timeout=remaining_seconds(),
                    retry=_request_retry(),
                    job_retry=None,
                )
            )
        for statement in definitions:
            if isinstance(statement, exp.Create):
                self._apply_retention(statement.this.this.name)
        self._wait_for_job(
            self.client.query(
                self._qualify_view_sql(package.joinpath("views.sql").read_text()),
                job_config=self._query_config(),
                location=self.location,
                timeout=remaining_seconds(),
                retry=_request_retry(),
                job_retry=None,
            )
        )
        marker = self.client.get_table(
            self._table_id("schema_marker"),
            timeout=remaining_seconds(),
            retry=_request_retry(),
        )
        marker.labels = {
            "usagebassoon_schema_version": str(SCHEMA_VERSION),
            "usagebassoon_schema_hash": schema_hash("bigquery"),
            "usagebassoon_initializing": "false",
        }
        self.client.update_table(
            marker,
            ["labels"],
            timeout=remaining_seconds(),
            retry=_request_retry(),
        )

    def _check_existing_definition(self, statement: exp.Create) -> None:
        """Reject partial-init objects whose columns differ from the native baseline."""
        name = statement.this.this.name
        table = self.client.get_table(
            self._table_id(name),
            timeout=remaining_seconds(),
            retry=_request_retry(),
        )
        aliases = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL"}
        actual = [
            (
                field.name,
                aliases.get(field.field_type, field.field_type),
                field.mode,
            )
            for field in table.schema
        ]
        expected: list[tuple[str, str, str]] = []
        for column in statement.this.expressions:
            if not isinstance(column, exp.ColumnDef):
                continue
            kind = column.args["kind"].sql(dialect="bigquery")
            required = any(
                isinstance(constraint.kind, exp.NotNullColumnConstraint)
                for constraint in column.args.get("constraints", [])
            )
            mode = "REQUIRED" if required else "NULLABLE"
            if kind == "ARRAY<STRING>":
                kind, mode = "STRING", "REPEATED"
            expected.append((column.name, kind, mode))
        if actual != expected:
            raise RuntimeError(f"interrupted init found an incompatible table: {name}")

    def _apply_retention(self, name: str) -> None:
        """Override dataset expiration defaults so durable history never expires."""
        table = self.client.get_table(
            self._table_id(name),
            timeout=remaining_seconds(),
            retry=_request_retry(),
        )
        fields: list[str] = []
        if table.expires is not None:
            table.expires = None
            fields.append("expires")
        partition = table.time_partitioning
        if partition is not None:
            expiration = 90 * 24 * 60 * 60 * 1000 if name.startswith("raw_") else None
            if partition.expiration_ms != expiration:
                partition.expiration_ms = expiration
                table.time_partitioning = partition
                fields.append("time_partitioning")
        if fields:
            self.client.update_table(
                table,
                fields,
                timeout=remaining_seconds(),
                retry=_request_retry(),
            )

    @override
    @bounded
    def preflight(self) -> None:
        """Validate schema metadata with no query jobs on the common path."""
        try:
            actual = self.client.get_dataset(
                self.dataset_ref,
                timeout=remaining_seconds(),
                retry=_request_retry(),
            )
        except NotFound as error:
            raise RuntimeError(
                "warehouse is not initialized; run bassoon init"
            ) from error
        if str(actual.location).casefold() != self.location.casefold():
            raise ValueError("configured BigQuery location does not match the dataset")
        try:
            marker = self.client.get_table(
                self._table_id("schema_marker"),
                timeout=remaining_seconds(),
                retry=_request_retry(),
            )
        except NotFound as error:
            raise RuntimeError(
                "warehouse is not initialized; run bassoon init"
            ) from error
        labels = marker.labels or {}
        if labels.get("usagebassoon_initializing") == "true":
            raise RuntimeError(
                "warehouse initialization is incomplete; run bassoon init"
            )
        version = labels.get("usagebassoon_schema_version")
        if version is None:
            raise RuntimeError("warehouse is not initialized; run bassoon init")
        steps = pending_migrations(
            int(version), labels.get("usagebassoon_schema_hash", ""), "bigquery"
        )
        for step in steps:
            parameters = [
                bigquery.ScalarQueryParameter("version", "INT64", step.version),
                bigquery.ScalarQueryParameter(
                    "hash", "STRING", step.target_hashes["bigquery"]
                ),
            ]
            ledger = self._wait_for_job(
                self.client.query(
                    f"SELECT schema_hash FROM {self._table_ref('schema_migrations')} "
                    "WHERE version = @version",
                    job_config=self._query_config(parameters=parameters[:1]),
                    location=self.location,
                    timeout=remaining_seconds(),
                    retry=_request_retry(),
                    job_retry=None,
                )
            )
            recorded = list(ledger)
            if any(
                row["schema_hash"] != step.target_hashes["bigquery"] for row in recorded
            ):
                raise RuntimeError("migration ledger hash does not match this package")
            if not recorded:
                self._wait_for_job(
                    self.client.query(
                        self._qualify_view_sql(step.sql("bigquery")),
                        job_config=self._query_config(),
                        location=self.location,
                        timeout=remaining_seconds(),
                        retry=_request_retry(),
                        job_retry=None,
                    )
                )
                self._wait_for_job(
                    self.client.query(
                        f"INSERT INTO {self._table_ref('schema_migrations')} "
                        "(source_id, version, schema_hash, applied_at) "
                        "VALUES ('00000000-0000-0000-0000-000000000000', "
                        "@version, @hash, CURRENT_TIMESTAMP())",
                        job_config=self._query_config(parameters=parameters),
                        location=self.location,
                        timeout=remaining_seconds(),
                        retry=_request_retry(),
                        job_retry=None,
                    )
                )
            if step.version == SCHEMA_VERSION:
                from usagebassoon.backends.bigquery_compaction import (
                    install_compaction,
                )

                install_compaction(self, enabled=None)
            marker.labels = {
                **labels,
                "usagebassoon_schema_version": str(step.version),
                "usagebassoon_schema_hash": step.target_hashes["bigquery"],
            }
            self.client.update_table(
                marker,
                ["labels"],
                timeout=remaining_seconds(),
                retry=_request_retry(),
            )

    def _snapshot_stream(self, tables: Sequence[str]) -> SnapshotStream:
        """Pin canonical gold and raw query streams to one warehouse instant."""
        timestamp = next(
            iter(
                self._wait_for_job(
                    self.client.query(
                        "SELECT CURRENT_TIMESTAMP() AS captured_at",
                        job_config=self._query_config(),
                        location=self.location,
                        timeout=remaining_seconds(),
                        retry=_request_retry(),
                        job_retry=None,
                    )
                )
            )
        )["captured_at"]
        if not isinstance(timestamp, datetime):
            raise RuntimeError("BigQuery did not return a snapshot timestamp")
        view_sql = (
            resources.files("usagebassoon.sql.bigquery")
            .joinpath("views.sql")
            .read_text()
        )
        result: dict[str, Iterable[pa.RecordBatch]] = {}
        for table in tables:
            if table not in SNAPSHOT_TABLES:
                raise ValueError(f"unsupported snapshot table {table!r}")
            relation = "current_" + table
            # Debug snapshots preserve the event stream, including resolution events.
            if table in DEBUG_TABLES:
                relation = "replay_" + table
            statement = re.search(
                rf"CREATE OR REPLACE VIEW {relation} AS\n(.*?);", view_sql, re.DOTALL
            )
            if statement is None:
                raise RuntimeError(f"missing canonical snapshot relation {relation}")
            sql = statement[1]
            physical = (
                set(STATE_KEYS)
                | {"raw_" + name for name in set(STATE_KEYS) | DEBUG_TABLES}
                | {"collection_ledger"}
            )

            def at_capture(
                match: re.Match[str], physical_tables: set[str] = physical
            ) -> str:
                """Bind a physical input to the shared capture instant."""
                if match[1] in physical_tables:
                    return (
                        f"FROM {self._table_ref(match[1])}"
                        + (f" AS {match[2]} " if match[2] else " ")
                        + "FOR SYSTEM_TIME AS OF @captured_at"
                    )
                return match[0]

            sql = re.sub(
                r"\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)\b"
                r"(?:\s+AS\s+([A-Za-z_][A-Za-z0-9_]*))?",
                at_capture,
                sql,
            )
            # Debug replay relations reference a base table directly; state and ledger
            # definitions above also contain only physical tables.
            job = self.client.query(
                sql,
                job_config=self._query_config(
                    parameters=[
                        bigquery.ScalarQueryParameter(
                            "captured_at", "TIMESTAMP", timestamp
                        )
                    ]
                ),
                location=self.location,
                timeout=remaining_seconds(),
                retry=_request_retry(),
                job_retry=None,
            )
            self._wait_for_job(job)
            schema = CANONICAL_TABLE_SCHEMAS[table]

            def batches(
                query_job: bigquery.QueryJob = job, canonical: pa.Schema = schema
            ) -> Iterable[pa.RecordBatch]:
                """Read bounded result pages as canonical Arrow batches."""
                for batch in self._read_query_batches(query_job):
                    yield from (
                        pa.Table.from_batches([batch])
                        .select(canonical.names)
                        .cast(canonical)
                        .to_batches(max_chunksize=65536)
                    )

            result[table] = batches()
        return SnapshotStream(captured_at=timestamp, tables=result)

    @contextmanager
    @override
    def stream_snapshot(self, tables: Sequence[str]) -> Generator[SnapshotStream]:
        """Yield streams pinned to the same BigQuery time-travel timestamp.

        Yields:
            Canonical Arrow batch streams.
        """
        with operation_budget(SNAPSHOT_SECONDS):
            yield self._snapshot_stream(tables)

    @override
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Materialize a consistent read for callers explicitly requesting tables."""
        with operation_budget(SNAPSHOT_SECONDS), self.stream_snapshot(tables) as stream:
            result = {
                name: pa.Table.from_batches(
                    list(batches), schema=CANONICAL_TABLE_SCHEMAS[name]
                )
                for name, batches in stream.tables.items()
            }
            return SnapshotRead(stream.captured_at, result)

    @override
    @bounded
    def configure_maintenance(self, *, enabled: bool) -> str | None:
        """Provision the native maintenance schedule in the requested state."""
        from usagebassoon.backends.bigquery_compaction import install_compaction

        return install_compaction(self, enabled=enabled)

    @override
    def snapshot_provenance(self) -> dict[str, object]:
        """Identify BigQuery and its installed scheduled-SQL contract."""
        return {
            "source_backend": "bigquery",
            "backend_schema_version": SCHEMA_VERSION,
            "backend_schema_hash": schema_hash("bigquery"),
        }

    @override
    @bounded
    def prepare_recovery(self, *, notice: Callable[[str], None] | None = None) -> None:
        """Disable scheduled compaction and wait briefly for existing runs."""
        from usagebassoon.backends.bigquery_compaction import pause_compaction

        if notice:
            notice("Disabling scheduled compaction...")
        pause_compaction(self, timeout=min(60.0, self.timeout_seconds))

    @override
    @bounded
    def maintenance_status(self) -> tuple[bool, str] | None:
        """Inspect native schedule state through the authorized lazy SDK path."""
        from usagebassoon.backends.bigquery_compaction import compaction_status

        return compaction_status(self, timeout=min(30.0, self.timeout_seconds))

    @override
    @bounded
    def check_restore_empty(self) -> None:
        """Reject populated gold, raw, progress, and unexpected base tables."""
        for item in self.client.list_tables(
            self.dataset_ref,
            timeout=remaining_seconds(),
            retry=_request_retry(),
        ):
            if item.table_type == "VIEW" or item.table_id in {
                "schema_marker",
                "schema_migrations",
                "restore_receipts",
            }:
                continue
            if item.table_id.startswith("_stage_"):
                table = self.client.get_table(
                    item.reference,
                    timeout=remaining_seconds(),
                    retry=_request_retry(),
                )
                labels = table.labels or {}
                if labels.get("usagebassoon_kind") == "restore_stage" and labels.get(
                    "usagebassoon_restore"
                ):
                    continue
            predicate = (
                " WHERE domain <> '__lock__'"
                if item.table_id == "compaction_ledger"
                else ""
            )
            rows = self._wait_for_job(
                self.client.query(
                    f"SELECT 1 FROM {self._table_ref(item.table_id)}{predicate} "
                    "LIMIT 1",
                    job_config=self._query_config(),
                    location=self.location,
                    timeout=remaining_seconds(),
                    retry=_request_retry(),
                    job_retry=None,
                )
            )
            if next(iter(rows), None) is not None:
                raise ValueError("restore requires an empty warehouse")

    @override
    @bounded
    def restore_committed(self, operation_id: str) -> bool:
        """Resolve restore completion from the receipt committed with its rows."""
        deadline = time.monotonic() + remaining_seconds(None)
        remaining = self._recovery_budget(deadline)
        committed = self._restore_receipt(operation_id, remaining)
        if not committed and any(
            (job.labels or {}).get("usagebassoon_restore") == operation_id
            for job in self._restore_jobs(remaining)
        ):
            raise RuntimeError(
                "Restore completion could not be determined: "
                "an owned job is still active. "
                "Keep writers stopped and retry with --cleanup-stages."
            )
        return committed

    def _restore_receipt(
        self, operation_id: str, remaining: Callable[[], float]
    ) -> bool:
        """Inspect a receipt within the caller's recovery deadline."""
        job = self.client.query(
            f"SELECT 1 FROM {self._table_ref('restore_receipts')} "
            "WHERE operation_id = @operation_id LIMIT 1",
            job_config=self._query_config(
                parameters=[
                    bigquery.ScalarQueryParameter(
                        "operation_id", "STRING", operation_id
                    )
                ]
            ),
            location=self.location,
            retry=_request_retry(remaining),
            timeout=remaining(),
            job_retry=None,
        )
        rows = job.result(
            timeout=min(120.0, remaining()),
            retry=_request_retry(remaining),
            job_retry=None,
        )
        return next(iter(rows), None) is not None

    @staticmethod
    def _recovery_budget(deadline: float) -> Callable[[], float]:
        """Share one deadline across all recovery RPCs and waits."""

        def remaining() -> float:
            """Fail closed when recovery completion cannot be established in time."""
            budget = deadline - time.monotonic()
            if budget <= 0:
                raise RuntimeError(
                    "Restore completion could not be determined before the "
                    "recovery deadline; staging was preserved"
                )
            return budget

        return remaining

    def _restore_jobs(
        self, remaining: Callable[[], float]
    ) -> list[bigquery.QueryJob | bigquery.LoadJob]:
        """Find every active owned job, even when no staging table survives."""
        from google.api_core.exceptions import Forbidden

        result: list[bigquery.QueryJob | bigquery.LoadJob] = []
        destination = sha256(self.dataset_ref.encode()).hexdigest()[:63]
        for state in ("pending", "running"):
            token: str | None = None
            while True:
                try:
                    pager = self.client.list_jobs(
                        project=self.project,
                        all_users=True,
                        state_filter=state,
                        page_token=token,
                        retry=_request_retry(remaining),
                        timeout=remaining(),
                    )
                    page = next(iter(pager.pages))
                except Forbidden as error:
                    raise RuntimeError(
                        "Restore job visibility requires bigquery.jobs.listAll "
                        "on the project; completion cannot be determined"
                    ) from error
                for job in page:
                    if isinstance(job, (bigquery.QueryJob, bigquery.LoadJob)):
                        labels: dict[str, str] = job.labels or {}
                        if (
                            labels.get("usagebassoon_kind") == "restore_job"
                            and labels.get("usagebassoon_destination") == destination
                        ):
                            result.append(job)
                remaining()
                token = pager.next_page_token
                if not token:
                    break
        return result

    @override
    @bounded
    def restore_stages(self) -> list[dict[str, object]]:
        """Inspect only label-owned restore stages, including their expiry."""
        return self._restore_stage_records(
            self._recovery_budget(time.monotonic() + remaining_seconds(None))
        )

    def _restore_stage_records(
        self, remaining: Callable[[], float]
    ) -> list[dict[str, object]]:
        """Discover disposable stages with bounded metadata reads."""
        result: list[dict[str, object]] = []
        token: str | None = None
        while True:
            pager = self.client.list_tables(
                self.dataset_ref,
                page_token=token,
                retry=_request_retry(remaining),
                timeout=remaining(),
            )
            page = next(iter(pager.pages))
            for item in page:
                labels: dict[str, str] = item.labels
                if labels.get("usagebassoon_kind") != "restore_stage" or not labels.get(
                    "usagebassoon_restore"
                ):
                    continue
                table = self.client.get_table(
                    item.reference,
                    retry=_request_retry(remaining),
                    timeout=remaining(),
                )
                labels = table.labels or {}
                if labels.get("usagebassoon_kind") == "restore_stage" and labels.get(
                    "usagebassoon_restore"
                ):
                    result.append(
                        {
                            "table": str(table.reference),
                            "operation_id": labels["usagebassoon_restore"],
                            "expires_at": table.expires,
                        }
                    )
            remaining()
            token = pager.next_page_token
            if not token:
                return result

    @override
    @bounded
    def cleanup_restore_stages(self) -> None:
        """Drain owned restore jobs and immediately discard disposable stages."""
        deadline = time.monotonic() + remaining_seconds(None)
        remaining = self._recovery_budget(deadline)
        self._drain_restore_jobs(remaining)
        stages = self._restore_stage_records(remaining)
        for stage in stages:
            operation = str(stage["operation_id"])
            committed = self._restore_receipt(operation, remaining)
            _LOG.info(
                "Discarding owned restore stage %s; committed=%s",
                stage["table"],
                committed,
            )
            table = self.client.get_table(
                str(stage["table"]),
                retry=Retry(predicate=lambda _error: False),
                timeout=remaining(),
            )
            labels = table.labels or {}
            if (
                labels.get("usagebassoon_kind") != "restore_stage"
                or labels.get("usagebassoon_restore") != operation
            ):
                raise RuntimeError("restore stage ownership changed; refusing cleanup")
            self.client.delete_table(
                table.reference,
                not_found_ok=True,
                retry=Retry(predicate=lambda _error: False),
                timeout=remaining(),
            )

    def _drain_restore_jobs(self, remaining: Callable[[], float]) -> None:
        """Cancel owned jobs and establish terminal status before deleting stages."""
        for job in self._restore_jobs(remaining):
            self._drain_restore_job(job, remaining)

    @staticmethod
    def _drain_restore_job(
        job: bigquery.QueryJob | bigquery.LoadJob, remaining: Callable[[], float]
    ) -> None:
        """Observe the exact submitted job until cancellation or completion settles."""
        if job.state != "DONE":
            job.cancel(retry=_request_retry(remaining), timeout=remaining())
        while job.state != "DONE":
            job.reload(retry=_request_retry(remaining), timeout=remaining())
            if job.state != "DONE":
                time.sleep(min(0.5, remaining()))

    def _wait_restore_job(
        self, job: bigquery.QueryJob | bigquery.LoadJob
    ) -> Iterable[Mapping[str, object]]:
        """Resolve a failed wait using the exact submitted job reference."""
        try:
            options = _request_retry()
            rows = (
                job.result(
                    timeout=remaining_seconds(120.0), retry=options, job_retry=None
                )
                if isinstance(job, bigquery.QueryJob)
                else job.result(timeout=remaining_seconds(120.0), retry=options)
            )
            return cast(Iterable[Mapping[str, object]], rows)
        except Exception as wait_error:
            try:
                with cleanup_budget():
                    self._drain_restore_job(
                        job,
                        self._recovery_budget(
                            time.monotonic() + remaining_seconds(15.0)
                        ),
                    )
            except Exception as error:
                _LOG.warning(
                    "Restore job completion remains unknown; preserving staging",
                    exc_info=True,
                )
                raise _RestoreUncertain(
                    "Restore completion could not be determined; keep writers "
                    "stopped and retry receipt inspection or --cleanup-stages"
                ) from error
            if isinstance(wait_error, FutureTimeoutError):
                raise _JobTimeout(
                    f"BigQuery restore job exceeded {self.timeout_seconds:.0f} "
                    "seconds; its terminal state was subsequently observed"
                ) from wait_error
            raise

    @override
    def restore_tables(self, tables: Mapping[str, pa.Table]) -> None:
        """Restore library-owned Arrow data through the verified file path."""
        with (
            operation_budget(RESTORE_SECONDS),
            TemporaryDirectory(prefix="usagebassoon-restore-") as temporary,
        ):
            files: dict[str, Path] = {}
            for table, data in tables.items():
                if table not in SNAPSHOT_TABLES:
                    raise ValueError(f"unsupported restore table: {table}")
                if data.num_rows:
                    files[table] = Path(temporary) / f"{table}.parquet"
                    pq.write_table(data, files[table])
            self.restore_snapshot(
                files, operation_id=str(uuid4()), snapshot_id="library"
            )

    @override
    def restore_snapshot(
        self, files: Mapping[str, Path], *, operation_id: str, snapshot_id: str
    ) -> None:
        """Stage verified Parquet then publish gold and the receipt atomically."""
        with operation_budget(RESTORE_SECONDS):
            if set(files) - set(SNAPSHOT_TABLES):
                raise ValueError("unsupported restore tables")
            if self.restore_committed(operation_id):
                return
            self.cleanup_restore_stages()
            attempt_id = uuid4().hex
            stages: dict[str, str] = {}
            cleanup_allowed = True
            try:
                for table, path in files.items():
                    stage = self._stage_id(table, attempt_id)
                    schema = CANONICAL_TABLE_SCHEMAS[table]
                    owned = bigquery.Table(
                        stage,
                        schema=_schema_from_arrow(
                            pa.Table.from_batches([], schema=schema)
                        ),
                    )
                    owned.labels = {
                        "usagebassoon_kind": "restore_stage",
                        "usagebassoon_restore": operation_id,
                        "usagebassoon_attempt": attempt_id,
                    }
                    owned.expires = datetime.now(UTC) + timedelta(days=1)
                    self.client.create_table(
                        owned,
                        timeout=remaining_seconds(),
                        retry=_request_retry(),
                    )
                    stages[table] = stage
                    options = bigquery.ParquetOptions()
                    options.enable_list_inference = True
                    with path.open("rb") as payload:
                        self._wait_restore_job(
                            self._submit_load(
                                payload,
                                stage,
                                job_config=bigquery.LoadJobConfig(
                                    labels={
                                        "usagebassoon_kind": "restore_job",
                                        "usagebassoon_restore": operation_id,
                                        "usagebassoon_destination": sha256(
                                            self.dataset_ref.encode()
                                        ).hexdigest()[:63],
                                    },
                                    source_format=bigquery.SourceFormat.PARQUET,
                                    parquet_options=options,
                                    schema=owned.schema,
                                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                    create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
                                ),
                            )
                        )
                self._commit_restore(stages, operation_id, snapshot_id)
            except _RestoreUncertain:
                cleanup_allowed = False
                raise
            except BadRequest as error:
                if "restore requires an empty warehouse" in str(error):
                    raise ValueError("restore requires an empty warehouse") from error
                raise
            except BaseException:
                cleanup_allowed = False
                raise
            finally:
                if cleanup_allowed:
                    try:
                        remaining = self._recovery_budget(
                            time.monotonic() + remaining_seconds(None)
                        )
                        self._drain_restore_jobs(remaining)
                        self._delete_stages(tuple(stages.values()), remaining=remaining)
                    except Exception:
                        _LOG.warning(
                            "Restore jobs could not be drained; "
                            "owned staging remains for retry",
                            exc_info=True,
                        )

    def _commit_restore(
        self, stages: Mapping[str, str], operation_id: str, snapshot_id: str
    ) -> None:
        """Recheck emptiness and commit state with its unambiguous receipt."""
        statements = ["BEGIN TRANSACTION;"]
        statements.append(
            "UPDATE compaction_ledger SET compacted_at = CURRENT_TIMESTAMP() "
            "WHERE domain = '__lock__';"
        )
        statements.append(
            "ASSERT (SELECT COUNT(*) FROM compaction_ledger "
            "WHERE domain = '__lock__') = 1 "
            "AS 'restore control row is missing';"
        )
        # Include both gold and bronze, diagnostics, progress, and foreign tables.
        # The schema/control bootstrap is metadata and deliberately exempt.
        for item in self.client.list_tables(
            self.dataset_ref,
            timeout=remaining_seconds(),
            retry=_request_retry(),
        ):
            if (
                item.table_type == "VIEW"
                or item.full_table_id.replace(":", ".") in stages.values()
            ):
                continue
            name = item.table_id
            if name in {"schema_migrations", "schema_marker", "restore_receipts"}:
                continue
            predicate = (
                " WHERE domain <> '__lock__'" if name == "compaction_ledger" else ""
            )
            statements.append(
                "ASSERT NOT EXISTS(SELECT 1 FROM "
                f"{self._table_ref(name)}{predicate}) "
                "AS 'restore requires an empty warehouse';"
            )
        for table, stage in stages.items():
            destination = "raw_" + table if table in DEBUG_TABLES else table
            columns = ", ".join(
                self._column(name) for name in CANONICAL_TABLE_SCHEMAS[table].names
            )
            statements.append(
                f"INSERT INTO {self._table_ref(destination)} ({columns}) "
                f"SELECT {columns} FROM {f'`{stage}`'};"
            )
        statements.append(
            f"INSERT INTO {self._table_ref('restore_receipts')} "
            "(source_id, operation_id, snapshot_id, committed_at) VALUES "
            "('00000000-0000-0000-0000-000000000000', "
            "@operation_id, @snapshot_id, CURRENT_TIMESTAMP());"
        )
        statements.append("COMMIT TRANSACTION;")
        options = self._query_config(
            parameters=[
                bigquery.ScalarQueryParameter("operation_id", "STRING", operation_id),
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
            ]
        )
        options.labels = {
            "usagebassoon_kind": "restore_job",
            "usagebassoon_restore": operation_id,
            "usagebassoon_destination": sha256(self.dataset_ref.encode()).hexdigest()[
                :63
            ],
        }
        self._wait_restore_job(
            self.client.query(
                "\n".join(statements),
                job_config=options,
                location=self.location,
                timeout=remaining_seconds(),
                retry=_request_retry(),
                job_retry=None,
            )
        )

    @bounded
    def _submit_load(
        self,
        payload: BinaryIO,
        destination: str,
        *,
        job_config: bigquery.LoadJobConfig,
    ) -> bigquery.LoadJob:
        """Retry a rewound upload using one job ID, resolving accepted duplicates."""
        job_id = "usagebassoon_" + uuid4().hex

        def submit() -> bigquery.LoadJob:
            """Recalculate the allowance before each upload or job inspection."""
            payload.seek(0)
            try:
                return self.client.load_table_from_file(
                    payload,
                    destination,
                    job_config=job_config,
                    job_id=job_id,
                    location=self.location,
                    timeout=remaining_seconds(),
                    # Retry ownership stays here so each attempt gets remaining time.
                    num_retries=0,
                )
            except Conflict as error:
                job = self.client.get_job(
                    job_id,
                    location=self.location,
                    timeout=remaining_seconds(),
                    retry=_request_retry(),
                )
                if not isinstance(job, bigquery.LoadJob):
                    raise RuntimeError(
                        "BigQuery upload ID resolved to a non-load job"
                    ) from error
                return job

        return _request_retry()(submit)()

    @bounded
    def _load(
        self,
        data: pa.Table,
        destination: str,
        *,
        disposition: str,
        schema: Sequence[bigquery.SchemaField] | None = None,
    ) -> None:
        """Load canonical Arrow data through an explicit Parquet payload."""
        parquet_options = bigquery.ParquetOptions()
        parquet_options.enable_list_inference = True
        with TemporaryFile(mode="w+b") as payload:
            pq.write_table(data, payload)
            payload.seek(0)
            self._wait_for_job(
                self._submit_load(
                    payload,
                    destination,
                    job_config=bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.PARQUET,
                        parquet_options=parquet_options,
                        schema=(
                            list(schema)
                            if schema is not None
                            else _schema_from_arrow(data)
                        ),
                        write_disposition=disposition,
                        create_disposition=(
                            bigquery.CreateDisposition.CREATE_NEVER
                            if disposition == bigquery.WriteDisposition.WRITE_APPEND
                            else bigquery.CreateDisposition.CREATE_IF_NEEDED
                        ),
                    ),
                )
            )

    def _stage_id(self, table: str, run_id: str) -> str:
        """Return one collision-resistant staging table ID for BigQuery APIs."""
        compact_run_id = run_id.replace("-", "")
        return self._table_id(f"_stage_{table}_{compact_run_id}")

    def _stage_ref(self, table: str, run_id: str) -> str:
        """Return one collision-resistant staging table reference for SQL."""
        return f"`{self._stage_id(table, run_id)}`"

    def _delete_stages(
        self, stages: Sequence[str], *, remaining: Callable[[], float] | None = None
    ) -> None:
        """Best-effort remove staging tables after a batch reaches a terminal state."""
        for stage in stages:
            try:
                self.client.delete_table(
                    stage,
                    not_found_ok=True,
                    retry=_request_retry(remaining),
                    timeout=remaining() if remaining else self.timeout_seconds,
                )
                _LOG.info("removed BigQuery staging table %s", stage)
            except Exception:
                _LOG.exception("could not remove BigQuery staging table %s", stage)

    @override
    @bounded
    def has_committed_run(self, run_id: str) -> bool:
        """Read a deduplicated collection summary when explicitly requested."""
        return bool(
            self.query(
                "SELECT run_id FROM collection_runs WHERE run_id = :run_id LIMIT 1",
                {"run_id": run_id},
            ).num_rows
        )

    @override
    @bounded
    def upsert(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> UpsertResult:
        """Publish state observations directly to bronze without a mutation job."""
        if table not in STATE_KEYS:
            raise ValueError(f"unsupported state table {table!r}")
        self._validate_upsert(table, data, natural_keys, change_fields)
        self.append(table, data)
        return UpsertResult(inserted=data.num_rows)

    @staticmethod
    def _column(column: str) -> str:
        """Return a validated, quoted internal column reference fragment."""
        if not is_simple_identifier(column):
            raise ValueError(f"invalid BigQuery column identifier: {column!r}")
        return f"`{column}`"

    def _change_sql(
        self,
        change_fields: Sequence[str],
        repeated_fields: frozenset[str] = frozenset(),
    ) -> str:
        """Build a null-safe material-change predicate."""
        return (
            " OR ".join(
                (
                    f"TO_JSON_STRING(target.{self._column(field)}) IS DISTINCT FROM "
                    f"TO_JSON_STRING(source.{self._column(field)})"
                    if field in repeated_fields
                    else f"target.{self._column(field)} IS DISTINCT FROM "
                    f"source.{self._column(field)}"
                )
                for field in change_fields
            )
            or "FALSE"
        )

    @override
    @bounded
    def append(self, table: str, data: pa.Table) -> None:
        """Append observations to their declared physical publication table."""
        if table not in CANONICAL_TABLE_SCHEMAS:
            raise ValueError(f"unsupported publication table {table!r}")
        if not data.num_rows:
            return
        schema = CANONICAL_TABLE_SCHEMAS[table]
        data = data.select(schema.names).cast(schema)
        if any(
            data.column(name).null_count
            for name in ("source_id", "event_id", "collected_at")
        ):
            raise ValueError(
                "publication requires non-null source_id, event_id, collected_at"
            )
        destination = table if table == "collection_ledger" else "raw_" + table
        self._load(
            data,
            self._table_id(destination),
            disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

    @override
    @bounded
    def persist_batch(self, batch: PersistenceBatch) -> BatchPersistResult:
        """Publish independent Arrow tables concurrently, then append the run ledger.

        A failed table never certifies collection coverage. Retries replay the same
        event IDs; canonical views absorb duplicate physical observations.
        """
        self._validate_batch(batch)
        writes = {write.table: write.data for write in batch.current_state}
        writes.update(batch.append_only)
        errors: list[Exception] = []
        with ThreadPoolExecutor(max_workers=max(1, len(writes))) as executor:
            futures = [
                executor.submit(copy_context().run, self.append, table, data)
                for table, data in writes.items()
                if data.num_rows
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as error:
                    errors.append(error)
        if errors:
            raise errors[0]
        self.append("collection_ledger", batch.collection_ledger)
        return BatchPersistResult(
            {
                table: UpsertResult(inserted=data.num_rows)
                for table, data in writes.items()
            }
        )

    @override
    @bounded
    def query(self, sql: str, parameters: Mapping[str, str] | None = None) -> pa.Table:
        """Run BigQuery Standard SQL with named string parameters as Arrow."""
        bindings = parameters or {}
        invalid = [name for name in bindings if not is_simple_identifier(name)]
        if invalid:
            raise ValueError(f"invalid BigQuery parameter names: {invalid!r}")
        tokens = sqlglot.tokenize(sql, read="bigquery")
        parts: list[str] = []
        position = 0
        for token, following in pairwise(tokens):
            if (
                token.token_type == TokenType.COLON
                and following.start == token.end + 1
                and is_simple_identifier(sql[following.start : following.end + 1])
            ):
                parts.extend((sql[position : token.start], "@"))
                position = token.end + 1
        parts.append(sql[position:])
        statement = "".join(parts)
        query_parameters = [
            bigquery.ScalarQueryParameter(name, "STRING", value)
            for name, value in bindings.items()
        ]
        if self._read_at is not None:
            if _READ_TIMESTAMP_PARAMETER in bindings:
                raise ValueError("diagnostic timestamp parameter is reserved")
            statement = _pinned_query(statement, self._read_views, self.dataset_ref)
            query_parameters.append(
                bigquery.ScalarQueryParameter(
                    _READ_TIMESTAMP_PARAMETER, "TIMESTAMP", self._read_at
                )
            )
        job = self.client.query(
            statement,
            job_config=self._query_config(parameters=query_parameters),
            location=self.location,
            timeout=remaining_seconds(),
            retry=_request_retry(),
            job_retry=None,
        )
        self._wait_for_job(job)
        return self._read_query_arrow(job)

    @bounded
    def _read_query_arrow(self, job: bigquery.job.QueryJob) -> pa.Table:
        """Read a completed query as Arrow within the enclosing operation budget."""
        return pa.Table.from_batches(list(self._read_query_batches(job)))

    def _read_query_batches(
        self, job: bigquery.job.QueryJob
    ) -> Generator[pa.RecordBatch]:
        """Yield result batches using explicit, deadline-aware stream recovery."""
        with operation_budget(self.timeout_seconds):
            destination = job.destination
            if destination is None:
                raise RuntimeError(
                    "BigQuery query completed without a result destination"
                )
            table = (
                f"projects/{destination.project}/datasets/{destination.dataset_id}/"
                f"tables/{destination.table_id}"
            )
            # The generated client avoids hidden reconnects with a stale timeout.
            with big_query_read.BigQueryReadClient(
                credentials=self._credentials
            ) as reader:

                def create_session() -> bigquery_storage_types.ReadSession:
                    """Recalculate the RPC deadline on every session retry."""
                    return reader.create_read_session(
                        parent=f"projects/{self.project}",
                        read_session=bigquery_storage_types.ReadSession(
                            table=table,
                            data_format=bigquery_storage_types.DataFormat.ARROW,
                        ),
                        max_stream_count=1,
                        retry=None,
                        timeout=remaining_seconds(),
                    )

                session = _request_retry()(create_session)()
                remaining_seconds(None)
                arrow_schema = pa.ipc.read_schema(
                    pa.BufferReader(session.arrow_schema.serialized_schema)
                )
                # Preserve the result schema even when the session has no rows.
                yield pa.RecordBatch.from_pylist([], schema=arrow_schema)
                for stream in session.streams:
                    yield from self._read_stream_batches(
                        reader, stream.name, arrow_schema
                    )

    def _read_stream_batches(
        self,
        reader: big_query_read.BigQueryReadClient,
        stream_name: str,
        schema: pa.Schema,
    ) -> Generator[pa.RecordBatch]:
        """Resume transient read failures at the last consumed row offset."""
        offset = 0
        delays = exponential_sleep_generator(initial=0.5, maximum=5.0)
        while True:
            try:
                responses = reader.read_rows(
                    read_stream=stream_name,
                    offset=offset,
                    retry=None,
                    timeout=remaining_seconds(None),
                )
                try:
                    for response in responses:
                        remaining_seconds(None)
                        if not response.arrow_record_batch.serialized_record_batch:
                            continue
                        batch = cast(
                            pa.RecordBatch, from_read_rows_response(response, schema)
                        )
                        offset += batch.num_rows
                        if batch.num_rows:
                            delays = exponential_sleep_generator(
                                initial=0.5, maximum=5.0
                            )
                        yield batch
                        remaining_seconds(None)
                finally:
                    cancel = getattr(responses, "cancel", None)
                    if callable(cancel):
                        try:
                            cast(Callable[[], bool], cancel)()
                        except Exception:
                            _LOG.exception("could not close BigQuery result stream")
                return
            except Exception as error:
                if not _transient_request_error(error) and not isinstance(
                    error, Unknown
                ):
                    raise
                allowance = remaining_seconds(None)
                delay = next(delays)
                if delay >= allowance:
                    raise OperationTimeout(
                        "BigQuery read recovery exceeds the operation budget"
                    ) from error
                _LOG.warning(
                    "transient BigQuery read failed; retrying from row offset %s",
                    offset,
                    exc_info=True,
                )
                time.sleep(delay)

    @override
    @bounded
    def delete_curated(self, identity: CuratedIdentity) -> int:
        """Append a deletion observation without mutating the existing assignment."""
        data = self._curated_rows(identity)
        if not data.num_rows:
            return 0
        schema = CANONICAL_TABLE_SCHEMAS[identity.table]
        rows = data.to_pylist()
        timestamp = datetime.now(UTC)
        op_id = str(uuid4())
        for row in rows:
            row.update(
                event_id=str(uuid4()),
                collected_at=timestamp,
                updated_at=timestamp,
                op="delete",
                op_id=op_id,
                source_id=identity.source_id,
            )
        self.append(identity.table, pa.Table.from_pylist(rows, schema=schema))
        return len(rows)

    def _curated_rows(self, identity: CuratedIdentity) -> pa.Table:
        """Read one current assignment using its bound natural key."""
        predicate = " AND ".join(
            f"{self._column(name)} = :{name}" for name, _ in identity.target_values
        )
        return self.query(
            f"SELECT * FROM current_{identity.table} WHERE {predicate}",
            identity.parameters(),
        )

    @override
    @bounded
    def rename_curated(
        self,
        source: CuratedIdentity,
        destination: CuratedIdentity,
        *,
        updated_at: datetime,
    ) -> CuratedRenameResult:
        """Atomically append the new tag and old tag tombstone in one load job."""
        if source.table != "tags" or destination.table != "tags":
            raise ValueError("only tag assignments can be renamed")
        rows = self._curated_rows(source).to_pylist()
        if not rows:
            return CuratedRenameResult(renamed=False)
        if self._curated_rows(destination).num_rows:
            return CuratedRenameResult(renamed=False, destination_exists=True)
        original = rows[0]
        op_id = str(uuid4())
        deleted = {
            **original,
            "source_id": source.source_id,
            "event_id": str(uuid4()),
            "collected_at": updated_at,
            "updated_at": updated_at,
            "op": "delete",
            "op_id": op_id,
        }
        added = {
            **original,
            **dict(destination.values),
            "event_id": str(uuid4()),
            "collected_at": updated_at,
            "updated_at": updated_at,
            "op": "upsert",
            "op_id": op_id,
        }
        self.append(
            "tags",
            pa.Table.from_pylist(
                [deleted, added], schema=CANONICAL_TABLE_SCHEMAS["tags"]
            ),
        )
        return CuratedRenameResult(renamed=True)

    @contextmanager
    @override
    def transaction(self) -> Generator[None]:
        """Reject generic transaction use that cannot span BigQuery jobs."""
        raise RuntimeError("BigQuery requires persist_batch for atomic persistence")
        yield

    @override
    def close(self) -> None:
        """Close the owned BigQuery client."""
        self.client.close()
