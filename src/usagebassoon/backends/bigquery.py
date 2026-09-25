# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""bigquery.py — Atomic BigQuery storage over canonical Arrow tables."""

from __future__ import annotations

import logging
import re
from collections.abc import Generator, Iterable, Mapping, Sequence
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from datetime import datetime
from importlib import resources
from pathlib import Path
from tempfile import TemporaryFile
from typing import Protocol, cast, override
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.auth.credentials import Credentials
from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types as bigquery_storage_types
from google.oauth2 import service_account
from pandas_gbq.arrow import from_read_rows_response

from usagebassoon.backends.base import (
    SOURCE_LEASE_SECONDS,
    AbstractStorageBackend,
    ActiveTransaction,
    BatchPersistResult,
    CuratedIdentity,
    CuratedRenameError,
    CuratedRenameResult,
    CurrentStateWrite,
    PersistenceBatch,
    SnapshotRead,
    SourceLeaseToken,
    UpsertResult,
    is_simple_identifier,
)
from usagebassoon.schema_assets import RUNTIME_SCHEMA_ASSETS

_PROJECT_ID = re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]\Z")
_DATASET_ID = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,1023}\Z")
_LOCATION_ID = re.compile(r"[A-Za-z][A-Za-z0-9-]{0,62}\Z")
_LOG = logging.getLogger("usagebassoon")
_CONCURRENT_TRANSACTION_MESSAGE = "transaction is aborted due to concurrent update"
_VIEW_RELATIONS = (
    "report_summary",
    "report_models",
    "report_daily_usage",
    "report_session_models",
    "session_model_stats_current",
    "daily_cost",
    "tagged_sessions",
    "noted_sessions",
    "session_tags",
    "session_notes",
    "session_model_stats",
    "daily_activity",
    "daily_stats",
    "daily_processed_state",
    "price_versions",
    "sessions",
    "notes",
    "tags",
)


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
        mode = "NULLABLE"
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
    """StorageBackend implementation using run-scoped remote staging tables."""

    dialect = "bigquery"

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
        self._credentials = resolved_credentials
        try:
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
            raise RuntimeError(
                f"BigQuery job {job_id} exceeded {self.timeout_seconds:.0f} seconds "
                "and was cancelled"
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
        """Recognize BigQuery's transaction-concurrency abort response.

        Args:
            error: Exception raised while executing a persistence batch.

        Returns:
            True only for the documented concurrent-update transaction abort.
        """
        return isinstance(error, GoogleAPICallError) and (
            _CONCURRENT_TRANSACTION_MESSAGE in str(error).casefold()
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
        """Fully qualify view relations required by BigQuery view definitions."""
        relations = "|".join(re.escape(relation) for relation in _VIEW_RELATIONS)
        pattern = re.compile(
            rf"\b(CREATE\s+OR\s+REPLACE\s+VIEW|FROM|JOIN)\s+({relations})\b",
            flags=re.IGNORECASE,
        )

        def qualify(match: re.Match[str]) -> str:
            """Return one view SQL clause with a fully qualified relation."""
            clause = match.group(1)
            relation = match.group(2)
            qualified = f"{clause} {self._table_ref(relation)}"
            if clause.casefold() == "create or replace view":
                return qualified
            return f"{qualified} AS {relation}"

        return pattern.sub(qualify, sql)

    @override
    def apply_ddl(self) -> None:
        """Create or validate the configured dataset, then apply packaged SQL.

        Raises:
            ValueError: If an existing dataset has another location.
        """
        try:
            actual = self.client.get_dataset(self.dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = self.location
            actual = self.client.create_dataset(dataset)
        actual_location = actual.location
        if not isinstance(actual_location, str):
            raise ValueError("BigQuery dataset did not report a string location")
        if actual_location.casefold() != self.location.casefold():
            raise ValueError(
                "configured BigQuery location "
                f"{self.location!r} does not match existing dataset location "
                f"{actual_location!r}"
            )
        package = resources.files("usagebassoon.sql.bigquery")
        try:
            for filename in RUNTIME_SCHEMA_ASSETS:
                sql = package.joinpath(filename).read_text()
                if filename == "views.sql":
                    sql = self._qualify_view_sql(sql)
                self._wait_for_job(
                    self.client.query(
                        sql,
                        job_config=self._query_config(),
                        location=self.location,
                    )
                )
        except GoogleAPICallError as error:
            raise RuntimeError("BigQuery schema initialization failed") from error

    def _lease_rows(
        self, sql: str, parameters: list[bigquery.ScalarQueryParameter]
    ) -> tuple[Mapping[str, object], ...]:
        """Run one parameterized source lease script and return its final rows."""
        return tuple(
            self._wait_for_job(
                self.client.query(
                    sql,
                    job_config=self._query_config(parameters=parameters),
                    location=self.location,
                )
            )
        )

    @override
    def ensure_source_lease(self, source_id: str) -> None:
        """Provision a source row under the seeded singleton mutation guard."""
        if not source_id or source_id == "__bootstrap__":
            raise ValueError("invalid source lease identity")
        target = self._table_ref("source_leases")
        parameters = [bigquery.ScalarQueryParameter("source_id", "STRING", source_id)]
        count_sql = (
            f"SELECT COUNT(*) AS row_count FROM {target} WHERE source_id = @source_id"
        )
        initial = self._lease_rows(count_sql, parameters)
        if len(initial) != 1:
            raise RuntimeError("source lease count query returned no row")
        count = cast(int, initial[0]["row_count"])
        if count == 1:
            return
        if count != 0:
            raise RuntimeError("source lease identity is not unique")
        script = "\n".join(
            [
                "DECLARE guarded INT64 DEFAULT 0;",
                "BEGIN TRANSACTION;",
                f"UPDATE {target} SET fence = fence + 1 "
                "WHERE source_id = '__bootstrap__';",
                "SET guarded = @@row_count;",
                "ASSERT guarded = 1 AS 'source lease bootstrap is invalid';",
                f"INSERT INTO {target} "
                "(source_id, owner_id, run_id, fence, "
                "lease_expires_at, last_renewed_at) "
                "SELECT @source_id, NULL, NULL, 0, NULL, NULL "
                "FROM (SELECT 1) AS candidate "
                "WHERE NOT EXISTS (SELECT 1 FROM "
                f"{target} WHERE source_id = @source_id);",
                "COMMIT TRANSACTION;",
                count_sql + ";",
            ]
        )
        created = self._lease_rows(script, parameters)
        if len(created) != 1 or created[0]["row_count"] != 1:
            raise RuntimeError("source lease provisioning did not create one row")

    @override
    def claim_source_lease(
        self, source_id: str, run_id: str, owner_id: str
    ) -> SourceLeaseToken | None:
        """Claim an existing row with one conditional mutating statement."""
        target = self._table_ref("source_leases")
        parameters = [
            bigquery.ScalarQueryParameter("source_id", "STRING", source_id),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("owner_id", "STRING", owner_id),
        ]
        self._lease_rows(
            f"UPDATE {target} SET owner_id = @owner_id, run_id = @run_id, "
            "fence = fence + 1, "
            "lease_expires_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), "
            f"INTERVAL {SOURCE_LEASE_SECONDS} SECOND), "
            "last_renewed_at = CURRENT_TIMESTAMP() "
            "WHERE source_id = @source_id AND "
            "(owner_id IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP())",
            parameters,
        )
        rows = self._lease_rows(
            f"SELECT fence FROM {target} WHERE source_id = @source_id "
            "AND owner_id = @owner_id AND run_id = @run_id",
            parameters,
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("source lease claim returned duplicate rows")
        return SourceLeaseToken(
            source_id, run_id, owner_id, cast(int, rows[0]["fence"])
        )

    def _lease_update_count(self, sql: str, lease: SourceLeaseToken) -> int:
        """Execute one fenced lease update and return its affected row count."""
        rows = self._lease_rows(
            sql + "\nSELECT @@row_count AS changed;",
            [
                bigquery.ScalarQueryParameter("source_id", "STRING", lease.source_id),
                bigquery.ScalarQueryParameter("run_id", "STRING", lease.run_id),
                bigquery.ScalarQueryParameter("owner_id", "STRING", lease.owner_id),
                bigquery.ScalarQueryParameter("fence", "INT64", lease.fence),
            ],
        )
        if len(rows) != 1:
            raise RuntimeError("source lease update returned no row count")
        changed = cast(int, rows[0]["changed"])
        if changed > 1:
            raise RuntimeError("source lease identity is not unique")
        return changed

    @override
    def renew_source_lease(self, lease: SourceLeaseToken) -> bool:
        """Extend only the live matching lease using BigQuery time."""
        target = self._table_ref("source_leases")
        return (
            self._lease_update_count(
                f"UPDATE {target} SET lease_expires_at = "
                "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), "
                f"INTERVAL {SOURCE_LEASE_SECONDS} SECOND), "
                "last_renewed_at = CURRENT_TIMESTAMP() "
                "WHERE source_id = @source_id AND owner_id = @owner_id "
                "AND run_id = @run_id AND fence = @fence "
                "AND lease_expires_at > CURRENT_TIMESTAMP();",
                lease,
            )
            == 1
        )

    @override
    def release_source_lease(self, lease: SourceLeaseToken) -> None:
        """Clear an owned lease without changing its fence."""
        target = self._table_ref("source_leases")
        self._lease_update_count(
            f"UPDATE {target} SET owner_id = NULL, run_id = NULL, "
            "lease_expires_at = NULL, last_renewed_at = CURRENT_TIMESTAMP() "
            "WHERE source_id = @source_id AND owner_id = @owner_id "
            "AND run_id = @run_id AND fence = @fence;",
            lease,
        )

    @override
    def guard_source_lease(self, lease: SourceLeaseToken) -> bool:
        """Reject generic guards that cannot span separate BigQuery jobs."""
        raise RuntimeError("BigQuery fences leases inside persist_batch")

    @override
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Read every table at one BigQuery warehouse timestamp."""
        timestamp_rows = self._lease_rows(
            "SELECT CURRENT_TIMESTAMP() AS captured_at",
            [],
        )
        if len(timestamp_rows) != 1 or not isinstance(
            timestamp_rows[0]["captured_at"], datetime
        ):
            raise RuntimeError("BigQuery did not return a snapshot timestamp")
        captured_at = timestamp_rows[0]["captured_at"]
        captured: dict[str, pa.Table] = {}
        for table in tables:
            statement = (
                f"SELECT * FROM {self._table_ref(table)} "
                "FOR SYSTEM_TIME AS OF @captured_at"
            )
            job = self.client.query(
                statement,
                job_config=self._query_config(
                    parameters=[
                        bigquery.ScalarQueryParameter(
                            "captured_at", "TIMESTAMP", captured_at
                        )
                    ]
                ),
                location=self.location,
            )
            self._wait_for_job(job)
            captured[table] = self._read_query_arrow(job)
        return SnapshotRead(captured_at, captured)

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
        """Return whether the ingest ledger contains a completed cycle."""
        result = self._wait_for_job(
            self.client.query(
                f"SELECT 1 FROM {self._table_ref('ingest_runs')} "
                "WHERE `run_id` = @run_id LIMIT 1",
                job_config=self._query_config(
                    parameters=[
                        bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
                    ]
                ),
                location=self.location,
            )
        )
        return next(iter(result), None) is not None

    @override
    def upsert(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> UpsertResult:
        """Apply one standalone mutation for curation or empty-store restore.

        Collection ingestion uses :meth:`persist_batch` so every fact and
        audit write belongs to one BigQuery multi-statement transaction.
        """
        if data.num_rows == 0:
            return UpsertResult()
        self._validate_upsert(table, data, natural_keys, change_fields)
        stage_id = self._stage_id(table, uuid4().hex)
        stage = f"`{stage_id}`"
        try:
            self._load(
                data, stage_id, disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            inserted, updated = self._count_changes(
                table,
                stage,
                natural_keys,
                change_fields,
            )
            self._wait_for_job(
                self.client.query(
                    self._merge_sql(table, stage, natural_keys, change_fields),
                    job_config=self._query_config(),
                    location=self.location,
                )
            )
            return UpsertResult(inserted=inserted, updated=updated)
        finally:
            self._delete_stages((stage_id,))

    def _count_changes(
        self,
        table: str,
        stage: str,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> tuple[int, int]:
        """Count inserts and material changes against a staged source."""
        join = self._join_sql(natural_keys)
        table_schema = self.client.get_table(self._table_id(table)).schema
        repeated_fields = frozenset(
            field.name for field in table_schema if field.mode == "REPEATED"
        )
        changes = self._update_condition_sql(
            change_fields,
            repeated_fields,
            frozenset(field.name for field in table_schema),
        )
        key = self._column(natural_keys[0])
        row = next(
            iter(
                self._wait_for_job(
                    self.client.query(
                        "SELECT "
                        f"COUNTIF(target.{key} IS NULL) AS inserted, "
                        f"COUNTIF(target.{key} IS NOT NULL AND ({changes})) AS updated "
                        f"FROM {stage} AS source "
                        f"LEFT JOIN {self._table_ref(table)} AS target ON {join}",
                        job_config=self._query_config(),
                        location=self.location,
                    )
                )
            ),
            None,
        )
        if row is None:
            return (0, 0)
        return (cast(int, row["inserted"]), cast(int, row["updated"]))

    @staticmethod
    def _column(column: str) -> str:
        """Return a validated, quoted internal column reference fragment."""
        if not is_simple_identifier(column):
            raise ValueError(f"invalid BigQuery column identifier: {column!r}")
        return f"`{column}`"

    def _join_sql(self, natural_keys: Sequence[str]) -> str:
        """Build a null-safe-false natural-key equality predicate."""
        return " AND ".join(
            f"target.{self._column(key)} = source.{self._column(key)}"
            for key in natural_keys
        )

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

    def _update_condition_sql(
        self,
        change_fields: Sequence[str],
        repeated_fields: frozenset[str],
        columns: frozenset[str],
    ) -> str:
        """Require a newer observation before applying a material update."""
        changes = self._change_sql(change_fields, repeated_fields)
        if "updated_at" not in columns:
            return changes
        stamp = self._column("updated_at")
        return f"source.{stamp} >= target.{stamp} AND ({changes})"

    def _merge_sql(
        self,
        table: str,
        stage: str,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> str:
        """Build a MERGE statement from trusted canonical table metadata."""
        table_schema = self.client.get_table(self._table_id(table)).schema
        columns = tuple(field.name for field in table_schema)
        repeated_fields = frozenset(
            field.name for field in table_schema if field.mode == "REPEATED"
        )
        assignments: list[str] = []
        for column in columns:
            quoted = self._column(column)
            value = f"source.{quoted}"
            if table == "reconciliation_issues" and column == "message":
                value = (
                    "CASE WHEN source.`resolved` = TRUE "
                    f"THEN target.{quoted} ELSE {value} END"
                )
            elif (
                table in {"reconciliation_issues", "schema_drift_events"}
                and column == "observation_count"
            ):
                value = (
                    "CASE WHEN source.`updated_run_id` = "
                    "target.`updated_run_id` OR source.`observation_count` = 0 "
                    f"THEN target.{quoted} ELSE target.{quoted} + source.{quoted} END"
                )
            elif column in {"created_at", "first_seen_at", "detected_run_id", "run_id"}:
                value = f"COALESCE(target.{quoted}, {value})"
            assignments.append(f"target.{quoted} = {value}")
        quoted_columns = ", ".join(self._column(column) for column in columns)
        source_values = ", ".join(
            f"source.{self._column(column)}" for column in columns
        )
        changes = self._update_condition_sql(
            change_fields, repeated_fields, frozenset(columns)
        )
        return (
            f"MERGE {self._table_ref(table)} AS target USING {stage} AS source "
            f"ON {self._join_sql(natural_keys)} "
            f"WHEN MATCHED AND ({changes}) "
            "THEN UPDATE SET "
            f"{', '.join(assignments)} "
            f"WHEN NOT MATCHED THEN INSERT ({quoted_columns}) VALUES ({source_values})"
        )

    @override
    def append(self, table: str, data: pa.Table) -> None:
        """Append one Arrow table outside collection-cycle batch persistence."""
        if data.num_rows == 0:
            return
        if not is_simple_identifier(table):
            raise ValueError(f"invalid BigQuery table identifier: {table!r}")
        table_id = self._table_id(table)
        self._load(
            data,
            table_id,
            disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=self.client.get_table(table_id).schema,
        )

    @override
    def persist_batch(self, batch: PersistenceBatch) -> BatchPersistResult:
        """Stage Arrow tables then atomically apply one BigQuery cycle script."""
        self._validate_batch(batch)
        tables: dict[str, pa.Table] = {
            write.table: write.data for write in batch.current_state
        }
        tables.update(batch.append_only)
        tables["ingest_runs"] = batch.ingest_runs
        stage_ids = {table: self._stage_id(table, batch.run_id) for table in tables}
        stages = {table: f"`{stage_id}`" for table, stage_id in stage_ids.items()}
        with self._batch_lease(batch) as lease:
            try:
                for table, data in tables.items():
                    self._load(
                        data,
                        stage_ids[table],
                        disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    )
                result = self._wait_for_job(
                    self.client.query(
                        self._batch_script(batch, stages, lease),
                        job_config=self._query_config(
                            parameters=[
                                bigquery.ScalarQueryParameter(
                                    "run_id", "STRING", batch.run_id
                                ),
                                bigquery.ScalarQueryParameter(
                                    "source_id", "STRING", lease.source_id
                                ),
                                bigquery.ScalarQueryParameter(
                                    "owner_id", "STRING", lease.owner_id
                                ),
                                bigquery.ScalarQueryParameter(
                                    "fence", "INT64", lease.fence
                                ),
                            ]
                        ),
                        location=self.location,
                    )
                )
                row = next(iter(result), None)
                if row is None:
                    raise RuntimeError(
                        "BigQuery persistence batch returned no summary row"
                    )
                already_committed = bool(row["already_committed"])
                per_table = {
                    write.table: UpsertResult(
                        inserted=cast(int, row[f"inserted_{write.table}"]),
                        updated=cast(int, row[f"updated_{write.table}"]),
                    )
                    for write in batch.current_state
                }
                return BatchPersistResult(
                    per_table, already_committed=already_committed
                )
            finally:
                self._delete_stages(tuple(stage_ids.values()))

    def _batch_script(
        self,
        batch: PersistenceBatch,
        stages: Mapping[str, str],
        lease: SourceLeaseToken,
    ) -> str:
        """Build the remote staging and atomic persistence script.

        The temporary stage tables are loaded before the script. All user
        tables are then mutated only inside the transaction. ``ingest_runs``
        is inserted last, providing the idempotency ledger for retrying the
        same UUID after an ambiguous client-side outcome.
        """
        if lease.source_id != batch.source_id or lease.run_id != batch.run_id:
            raise ValueError("source lease does not match persistence batch")
        declarations = [
            "DECLARE already_committed BOOL DEFAULT FALSE;",
            "DECLARE lease_guard INT64 DEFAULT 0;",
        ]
        for write in batch.current_state:
            declarations.extend(
                [
                    f"DECLARE inserted_{write.table} INT64 DEFAULT 0;",
                    f"DECLARE updated_{write.table} INT64 DEFAULT 0;",
                ]
            )
        statements = [
            *declarations,
            "BEGIN TRANSACTION;",
            "SET already_committed = EXISTS("
            f"SELECT 1 FROM {self._table_ref('ingest_runs')} "
            "WHERE `run_id` = @run_id AND `source_id` = @source_id);",
            "IF NOT already_committed THEN",
            f"UPDATE {self._table_ref('source_leases')} SET "
            "lease_expires_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), "
            f"INTERVAL {SOURCE_LEASE_SECONDS} SECOND), "
            "last_renewed_at = CURRENT_TIMESTAMP() "
            "WHERE source_id = @source_id AND owner_id = @owner_id "
            "AND run_id = @run_id AND fence = @fence "
            "AND lease_expires_at > CURRENT_TIMESTAMP();",
            "SET lease_guard = @@row_count;",
            "ASSERT lease_guard = 1 AS 'collection source lease was lost';",
        ]
        for write in batch.current_state:
            join = self._join_sql(write.natural_keys)
            repeated_fields = frozenset(
                field.name
                for field in write.data.schema
                if pa.types.is_list(field.type)
            )
            changes = self._update_condition_sql(
                write.change_fields,
                repeated_fields,
                frozenset(write.data.column_names),
            )
            key = self._column(write.natural_keys[0])
            target = self._table_ref(write.table)
            stage = stages[write.table]
            statements.extend(
                [
                    f"SET inserted_{write.table} = ("
                    f"SELECT COUNTIF(target.{key} IS NULL) FROM {stage} AS source "
                    f"LEFT JOIN {target} AS target ON {join});",
                    f"SET updated_{write.table} = ("
                    f"SELECT COUNTIF(target.{key} IS NOT NULL AND ({changes})) "
                    f"FROM {stage} AS source LEFT JOIN {target} AS target ON {join});",
                    self._merge_from_data(write, stage),
                ]
            )
        for table, data in batch.append_only.items():
            columns = ", ".join(self._column(name) for name in data.column_names)
            statements.append(
                f"INSERT INTO {self._table_ref(table)} ({columns}) "
                f"SELECT {columns} FROM {stages[table]};"
            )
        ingest_columns = tuple(batch.ingest_runs.column_names)
        target_columns = ", ".join(self._column(name) for name in ingest_columns)
        inserted_total = (
            " + ".join(f"inserted_{write.table}" for write in batch.current_state)
            or "0"
        )
        updated_total = (
            " + ".join(f"updated_{write.table}" for write in batch.current_state) or "0"
        )

        def source_column(name: str) -> str:
            """Return the staged expression for one ingest-run field."""
            if name == "rows_inserted":
                return f"({inserted_total})"
            if name == "rows_updated":
                return f"({updated_total})"
            return f"source.{self._column(name)}"

        source_columns = ", ".join(source_column(name) for name in ingest_columns)
        statements.extend(
            [
                f"INSERT INTO {self._table_ref('ingest_runs')} ({target_columns}) "
                f"SELECT {source_columns} FROM {stages['ingest_runs']} AS source;",
                "END IF;",
                "COMMIT TRANSACTION;",
            ]
        )
        select_columns = ["already_committed"]
        for write in batch.current_state:
            select_columns.extend(
                [f"inserted_{write.table}", f"updated_{write.table}"],
            )
        statements.append(f"SELECT {', '.join(select_columns)};")
        return "\n".join(statements)

    def _merge_from_data(self, write: CurrentStateWrite, stage: str) -> str:
        """Build a batch MERGE using Arrow data columns instead of API lookup."""
        columns = tuple(write.data.column_names)
        repeated_fields = frozenset(
            field.name for field in write.data.schema if pa.types.is_list(field.type)
        )
        assignments: list[str] = []
        for column in columns:
            quoted = self._column(column)
            value = f"source.{quoted}"
            if write.table == "reconciliation_issues" and column == "message":
                value = (
                    "CASE WHEN source.`resolved` = TRUE "
                    f"THEN target.{quoted} ELSE {value} END"
                )
            elif (
                write.table in {"reconciliation_issues", "schema_drift_events"}
                and column == "observation_count"
            ):
                value = (
                    "CASE WHEN source.`updated_run_id` = "
                    "target.`updated_run_id` OR source.`observation_count` = 0 "
                    f"THEN target.{quoted} ELSE target.{quoted} + source.{quoted} END"
                )
            elif column in {"created_at", "first_seen_at", "detected_run_id", "run_id"}:
                value = f"COALESCE(target.{quoted}, {value})"
            assignments.append(f"target.{quoted} = {value}")
        quoted_columns = ", ".join(self._column(column) for column in columns)
        source_values = ", ".join(
            f"source.{self._column(column)}" for column in columns
        )
        changes = self._update_condition_sql(
            write.change_fields, repeated_fields, frozenset(columns)
        )
        return (
            f"MERGE {self._table_ref(write.table)} AS target USING {stage} AS source "
            f"ON {self._join_sql(write.natural_keys)} "
            f"WHEN MATCHED AND ({changes}) "
            f"THEN UPDATE SET {', '.join(assignments)} "
            f"WHEN NOT MATCHED THEN INSERT ({quoted_columns}) VALUES ({source_values});"
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
        """Delete one fully identified user-curated row."""
        predicates = " AND ".join(
            f"{self._column(name)} = @{name}" for name, _ in identity.values
        )
        job = self.client.query(
            f"DELETE FROM {self._table_ref(identity.table)} WHERE {predicates}",
            job_config=self._query_config(
                parameters=[
                    bigquery.ScalarQueryParameter(name, "STRING", value)
                    for name, value in identity.values
                ]
            ),
            location=self.location,
        )
        self._wait_for_job(job)
        return job.num_dml_affected_rows or 0

    @override
    def rename_curated(
        self,
        source: CuratedIdentity,
        destination: CuratedIdentity,
        *,
        updated_at: datetime,
    ) -> CuratedRenameResult:
        """Rename one tag assignment in a single BigQuery transaction script."""
        if source.table != "tags" or destination.table != "tags":
            raise ValueError("only tag assignments can be renamed")
        source_predicate = " AND ".join(
            f"{self._column(name)} = @source_{name}" for name, _ in source.values
        )
        destination_predicate = " AND ".join(
            f"{self._column(name)} = @destination_{name}"
            for name, _ in destination.values
        )
        parameters = [
            *[
                bigquery.ScalarQueryParameter(f"source_{name}", "STRING", value)
                for name, value in source.values
            ],
            *[
                bigquery.ScalarQueryParameter(f"destination_{name}", "STRING", value)
                for name, value in destination.values
            ],
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", updated_at),
        ]
        target = self._table_ref("tags")
        insert_columns = (
            "source_id, scope, client, workspace, session_id, "
            "tag, created_at, updated_at"
        )
        insert_values = (
            "source_id, scope, client, workspace, session_id, @destination_tag, "
            "created_at, @updated_at"
        )
        script = "\n".join(
            (
                "DECLARE source_exists BOOL DEFAULT EXISTS("
                f"SELECT 1 FROM {target} WHERE {source_predicate});",
                "DECLARE destination_exists BOOL DEFAULT EXISTS("
                f"SELECT 1 FROM {target} WHERE {destination_predicate});",
                "DECLARE deleted_rows INT64 DEFAULT 0;",
                "BEGIN TRANSACTION;",
                "IF source_exists AND NOT destination_exists THEN",
                f"INSERT INTO {target} ({insert_columns}) SELECT {insert_values} "
                f"FROM {target} WHERE {source_predicate};",
                f"DELETE FROM {target} WHERE {source_predicate};",
                "SET deleted_rows = @@row_count;",
                "ASSERT deleted_rows = 1 AS "
                "'tag rename deletion did not remove exactly one row';",
                "END IF;",
                "COMMIT TRANSACTION;",
                "SELECT source_exists, destination_exists, deleted_rows;",
            )
        )
        try:
            row = next(
                iter(
                    self._wait_for_job(
                        self.client.query(
                            script,
                            job_config=self._query_config(parameters=parameters),
                            location=self.location,
                        )
                    )
                ),
                None,
            )
        except GoogleAPICallError as error:
            _LOG.warning("atomic tag rename did not complete: %s", error)
            raise CuratedRenameError("atomic tag rename did not complete") from error
        if row is None:
            raise CuratedRenameError("atomic tag rename returned no result")
        return CuratedRenameResult(
            renamed=bool(row["source_exists"]) and not bool(row["destination_exists"]),
            destination_exists=bool(row["destination_exists"]),
        )

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
