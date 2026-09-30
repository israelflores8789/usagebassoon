# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""bigquery.py — Append publication and transactional BigQuery maintenance."""

from __future__ import annotations

import logging
import re
from collections.abc import Generator, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from tempfile import TemporaryFile
from typing import Protocol, cast, override
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import sqlglot
from google.api_core.exceptions import (
    BadRequest,
    GoogleAPICallError,
    InternalServerError,
    NotFound,
    ServiceUnavailable,
    TooManyRequests,
)
from google.auth import default as default_credentials
from google.auth.credentials import Credentials
from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types as bigquery_storage_types
from google.oauth2 import service_account
from pandas_gbq.arrow import from_read_rows_response
from sqlglot import exp

from usagebassoon.backends.base import (
    AbstractStorageBackend,
    ActiveTransaction,
    BatchPersistResult,
    CuratedIdentity,
    CuratedRenameResult,
    PersistenceBatch,
    SnapshotRead,
    UpsertResult,
    is_simple_identifier,
)
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS
from usagebassoon.schema_assets import (
    SCHEMA_VERSION,
    pending_migrations,
    schema_hash,
)
from usagebassoon.storage_model import (
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


class _JobTimeout(RuntimeError):
    """A job wait expired; replaying a publication keeps its event identifiers."""


class _StorageReadClient(Protocol):
    """Expose the high-level Storage client's stream-name read method."""

    def read_rows(
        self,
        name: str,
        *,
        timeout: float,
    ) -> Iterable[bigquery_storage_types.ReadRowsResponse]:
        """Return response messages for the named Storage read stream."""


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
        timeout_seconds: float = 120.0,
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
            timeout_seconds: Maximum wait for one job or Storage Read request.
            client: Injected client for offline unit tests.

        Raises:
            ValueError: If identifiers or credential sources conflict.
        """
        _validate_project(project)
        _validate_dataset(dataset)
        _validate_location(location)
        if maximum_bytes_billed < 1:
            raise ValueError("maximum_bytes_billed must be positive")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if credentials is not None and credentials_file is not None:
            raise ValueError("credentials and credentials_file cannot be combined")
        resolved_credentials = credentials
        if credentials_file is not None:
            try:
                resolved_credentials = (
                    service_account.Credentials.from_service_account_file(
                        str(credentials_file)
                    )
                )
            except (GoogleAuthError, OSError, ValueError) as error:
                raise RuntimeError(
                    "BigQuery credentials file could not be loaded"
                ) from error
        self.project = project
        self.dataset = dataset
        self.location = location
        self.maximum_bytes_billed = maximum_bytes_billed
        self.timeout_seconds = timeout_seconds
        self.dataset_ref = f"{project}.{dataset}"
        try:
            if resolved_credentials is None and client is None:
                resolved_credentials, _ = default_credentials(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
            self._credentials = resolved_credentials
            self.client = client or bigquery.Client(
                project=project,
                credentials=resolved_credentials,
                location=location,
            )
        except GoogleAuthError as error:
            raise RuntimeError(
                "BigQuery authentication failed; configure ADC or credentials_file"
            ) from error

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
                job.result(timeout=self.timeout_seconds),
            )
        except FutureTimeoutError as error:
            job_id = job.job_id or "unknown"
            try:
                job.cancel()
            except GoogleAPICallError:
                _LOG.exception("could not cancel timed-out BigQuery job %s", job_id)
            raise _JobTimeout(
                f"BigQuery job {job_id} exceeded {self.timeout_seconds:.0f} seconds "
                "and cancellation was requested"
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
        return isinstance(
            error,
            (_JobTimeout, ServiceUnavailable, TooManyRequests, InternalServerError),
        )

    @override
    def compaction_backlog(self) -> pa.Table | None:
        """Read every overdue arrival bucket through the installed health view."""
        return self.query(
            "SELECT domain, arrival_day, pending_rows, age_days "
            "FROM compaction_backlog WHERE age_days >= 2 "
            "ORDER BY age_days DESC"
        )

    @override
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
    def apply_ddl(self) -> None:
        """Initialize explicitly, including safe replay of an interrupted init."""
        try:
            actual = self.client.get_dataset(self.dataset_ref)
        except NotFound:
            actual = bigquery.Dataset(self.dataset_ref)
            actual.location = self.location
            actual = self.client.create_dataset(actual)
        if str(actual.location).casefold() != self.location.casefold():
            raise ValueError("configured BigQuery location does not match the dataset")
        try:
            marker = self.client.get_table(self._table_id("schema_marker"))
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
            str(table.table_id) for table in self.client.list_tables(self.dataset_ref)
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
                )
            )
            marker = self.client.get_table(self._table_id("schema_marker"))
            existing.add("schema_marker")
        marker.labels = {
            "usagebassoon_schema_version": str(SCHEMA_VERSION),
            "usagebassoon_schema_hash": schema_hash("bigquery"),
            "usagebassoon_initializing": "true",
        }
        marker = self.client.update_table(marker, ["labels"])
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
            )
        )
        marker = self.client.get_table(self._table_id("schema_marker"))
        marker.labels = {
            "usagebassoon_schema_version": str(SCHEMA_VERSION),
            "usagebassoon_schema_hash": schema_hash("bigquery"),
            "usagebassoon_initializing": "false",
        }
        self.client.update_table(marker, ["labels"])

    def _check_existing_definition(self, statement: exp.Create) -> None:
        """Reject partial-init objects whose columns differ from the native baseline."""
        name = statement.this.this.name
        table = self.client.get_table(self._table_id(name))
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
        table = self.client.get_table(self._table_id(name))
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
            self.client.update_table(table, fields)

    @override
    def preflight(self) -> None:
        """Validate schema metadata with no query jobs on the common path."""
        try:
            actual = self.client.get_dataset(self.dataset_ref)
        except NotFound as error:
            raise RuntimeError(
                "warehouse is not initialized; run bassoon init"
            ) from error
        if str(actual.location).casefold() != self.location.casefold():
            raise ValueError("configured BigQuery location does not match the dataset")
        try:
            marker = self.client.get_table(self._table_id("schema_marker"))
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
                    )
                )
            if step.version == SCHEMA_VERSION:
                from usagebassoon.backends.bigquery_compaction import install_compaction

                install_compaction(self)
            marker.labels = {
                **labels,
                "usagebassoon_schema_version": str(step.version),
                "usagebassoon_schema_hash": step.target_hashes["bigquery"],
            }
            self.client.update_table(marker, ["labels"])

    @override
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Materialize gold and the deduplicated raw tail at one warehouse instant."""
        timestamp = next(
            iter(
                self._wait_for_job(
                    self.client.query(
                        "SELECT CURRENT_TIMESTAMP() AS captured_at",
                        job_config=self._query_config(),
                        location=self.location,
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
        result: dict[str, pa.Table] = {}
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
            )
            self._wait_for_job(job)
            schema = CANONICAL_TABLE_SCHEMAS[table]
            result[table] = (
                self._read_query_arrow(job).select(schema.names).cast(schema)
            )
        return SnapshotRead(captured_at=timestamp, tables=result)

    @override
    def restore_tables(self, tables: Mapping[str, pa.Table]) -> None:
        """Assert destination emptiness and restore staged gold in one transaction."""
        unknown = set(tables) - set(SNAPSHOT_TABLES)
        if unknown:
            raise ValueError(f"unsupported restore tables: {sorted(unknown)}")
        restore_id = str(uuid4())
        stages: dict[str, str] = {}
        try:
            for table, data in tables.items():
                if not data.num_rows:
                    continue
                stage = self._stage_id(table, restore_id)
                stages[table] = stage
                schema = CANONICAL_TABLE_SCHEMAS[table]
                self._load(
                    data.select(schema.names).cast(schema),
                    stage,
                    disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
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
            for item in self.client.list_tables(self.dataset_ref):
                if (
                    item.table_type == "VIEW"
                    or item.full_table_id.replace(":", ".") in stages.values()
                ):
                    continue
                name = item.table_id
                if name in {"schema_migrations", "schema_marker"}:
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
            statements.append("COMMIT TRANSACTION;")
            self._wait_for_job(
                self.client.query(
                    "\n".join(statements),
                    job_config=self._query_config(),
                    location=self.location,
                )
            )
        except BadRequest as error:
            if "restore requires an empty warehouse" in str(error):
                raise ValueError("restore requires an empty warehouse") from error
            raise
        finally:
            self._delete_stages(tuple(stages.values()))

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
                self.client.load_table_from_file(
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
                    location=self.location,
                )
            )

    def _stage_id(self, table: str, run_id: str) -> str:
        """Return one collision-resistant staging table ID for BigQuery APIs."""
        compact_run_id = run_id.replace("-", "")
        return self._table_id(f"_stage_{table}_{compact_run_id}")

    def _stage_ref(self, table: str, run_id: str) -> str:
        """Return one collision-resistant staging table reference for SQL."""
        return f"`{self._stage_id(table, run_id)}`"

    def _delete_stages(self, stages: Sequence[str]) -> None:
        """Best-effort remove staging tables after a batch reaches a terminal state."""
        for stage in stages:
            try:
                self.client.delete_table(stage, not_found_ok=True)
                _LOG.info("removed BigQuery staging table %s", stage)
            except Exception:
                _LOG.exception("could not remove BigQuery staging table %s", stage)

    @override
    def has_committed_run(self, run_id: str) -> bool:
        """Read a deduplicated collection summary when explicitly requested."""
        return bool(
            self.query(
                "SELECT run_id FROM collection_runs WHERE run_id = :run_id LIMIT 1",
                {"run_id": run_id},
            ).num_rows
        )

    @override
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
                executor.submit(self.append, table, data)
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
    def query(self, sql: str, parameters: Mapping[str, str] | None = None) -> pa.Table:
        """Run BigQuery Standard SQL with named string parameters as Arrow."""
        bindings = parameters or {}
        invalid = [name for name in bindings if not is_simple_identifier(name)]
        if invalid:
            raise ValueError(f"invalid BigQuery parameter names: {invalid!r}")
        statement = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", r"@\1", sql)
        job = self.client.query(
            statement,
            job_config=self._query_config(
                parameters=[
                    bigquery.ScalarQueryParameter(name, "STRING", value)
                    for name, value in bindings.items()
                ]
            ),
            location=self.location,
        )
        self._wait_for_job(job)
        return self._read_query_arrow(job)

    def _read_query_arrow(self, job: bigquery.job.QueryJob) -> pa.Table:
        """Read one completed query destination through the Storage Read API.

        Args:
            job: Completed query job with a result destination table.

        Returns:
            Query rows as one canonical Arrow table.

        Raises:
            RuntimeError: If the completed job has no readable destination.
        """
        destination = job.destination
        if destination is None:
            raise RuntimeError("BigQuery query completed without a result destination")
        table = (
            f"projects/{destination.project}/datasets/{destination.dataset_id}/"
            f"tables/{destination.table_id}"
        )
        with bigquery_storage_v1.BigQueryReadClient(
            credentials=self._credentials
        ) as reader:
            session = reader.create_read_session(
                parent=f"projects/{self.project}",
                read_session=bigquery_storage_types.ReadSession(
                    table=table,
                    data_format=bigquery_storage_types.DataFormat.ARROW,
                ),
                max_stream_count=1,
                timeout=self.timeout_seconds,
            )
            arrow_schema = pa.ipc.read_schema(
                pa.BufferReader(session.arrow_schema.serialized_schema)
            )
            tables: list[pa.Table] = []
            for stream in session.streams:
                responses = cast(_StorageReadClient, reader).read_rows(
                    stream.name,
                    timeout=self.timeout_seconds,
                )
                batches = [
                    cast(
                        pa.RecordBatch,
                        from_read_rows_response(response, arrow_schema),
                    )
                    for response in responses
                    if response.arrow_record_batch.serialized_record_batch
                ]
                tables.append(pa.Table.from_batches(batches, schema=arrow_schema))
        if not tables:
            return pa.Table.from_batches([], schema=arrow_schema)
        return pa.concat_tables(tables)

    @override
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
