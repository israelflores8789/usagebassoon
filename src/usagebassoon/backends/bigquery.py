# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""bigquery.py — Atomic BigQuery storage over canonical Arrow tables."""

from __future__ import annotations

import logging
import re
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from typing import cast, override
from uuid import uuid4

import pyarrow as pa
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.auth.credentials import Credentials
from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery
from google.oauth2 import service_account

from usagebassoon.backends.base import (
    AbstractStorageBackend,
    ActiveTransaction,
    BatchPersistResult,
    CurrentStateWrite,
    PersistenceBatch,
    UpsertResult,
    is_simple_identifier,
)

_PROJECT_ID = re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]\Z")
_DATASET_ID = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,1023}\Z")
_LOG = logging.getLogger("usagebassoon")
_CONCURRENT_TRANSACTION_MESSAGE = "transaction is aborted due to concurrent update"
_VIEW_RELATIONS = (
    "report_summary",
    "report_models",
    "session_model_stats_current",
    "daily_cost",
    "tagged_sessions",
    "noted_sessions",
    "session_tags",
    "session_model_stats",
    "daily_activity",
    "daily_stats",
    "daily_processed_state",
    "price_versions",
    "sessions",
    "notes",
    "tags",
)


def _validate_project(project: str) -> None:
    """Require a canonical GCP project identifier."""
    if not _PROJECT_ID.fullmatch(project):
        raise ValueError(f"invalid BigQuery project identifier: {project!r}")


def _validate_dataset(dataset: str) -> None:
    """Require a canonical BigQuery dataset identifier."""
    if not _DATASET_ID.fullmatch(dataset):
        raise ValueError(f"invalid BigQuery dataset identifier: {dataset!r}")


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
        client: bigquery.Client | None = None,
    ) -> None:
        """Create a backend bound to a validated BigQuery dataset.

        Args:
            project: GCP project identifier.
            dataset: BigQuery dataset name.
            location: Required dataset and job location.
            credentials: Explicit Google credentials, normally for tests.
            credentials_file: Service-account JSON file, preferred over ADC.
            client: Injected client for offline unit tests.

        Raises:
            ValueError: If identifiers or credential sources conflict.
        """
        _validate_project(project)
        _validate_dataset(dataset)
        if not location.strip():
            raise ValueError("BigQuery location must not be empty")
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
        self.dataset_ref = f"{project}.{dataset}"
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
            rows = self.client.query(
                statement,
                job_config=self._query_config(),
                location=self.location,
            ).result()
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
            for filename in ("ddl.sql", "views.sql"):
                sql = package.joinpath(filename).read_text()
                if filename == "views.sql":
                    sql = self._qualify_view_sql(sql)
                self.client.query(
                    sql,
                    job_config=self._query_config(),
                    location=self.location,
                ).result()
        except GoogleAPICallError as error:
            raise RuntimeError("BigQuery schema initialization failed") from error

    def _load(
        self,
        data: pa.Table,
        destination: str,
        *,
        disposition: str,
        schema: Sequence[bigquery.SchemaField] | None = None,
    ) -> None:
        """Load canonical Arrow data through pandas with an explicit schema."""
        self.client.load_table_from_dataframe(
            data.to_pandas(),
            destination,
            job_config=bigquery.LoadJobConfig(
                schema=list(schema) if schema is not None else _schema_from_arrow(data),
                write_disposition=disposition,
            ),
            location=self.location,
        ).result()

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
        result = self.client.query(
            f"SELECT 1 FROM {self._table_ref('ingest_runs')} "
            "WHERE `run_id` = @run_id LIMIT 1",
            job_config=self._query_config(
                parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
            ),
            location=self.location,
        ).result()
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
            self.client.query(
                self._merge_sql(table, stage, natural_keys, change_fields),
                job_config=self._query_config(),
                location=self.location,
            ).result()
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
        changes = self._change_sql(change_fields, repeated_fields)
        key = self._column(natural_keys[0])
        row = next(
            iter(
                self.client.query(
                    "SELECT "
                    f"COUNTIF(target.{key} IS NULL) AS inserted, "
                    f"COUNTIF(target.{key} IS NOT NULL AND ({changes})) AS updated "
                    f"FROM {stage} AS source "
                    f"LEFT JOIN {self._table_ref(table)} AS target ON {join}",
                    job_config=self._query_config(),
                    location=self.location,
                ).result()
            ),
            None,
        )
        if row is None:
            return (0, 0)
        return (int(row["inserted"]), int(row["updated"]))

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
            if column in {"created_at", "first_seen_at"}:
                value = f"COALESCE(target.{quoted}, {value})"
            assignments.append(f"{quoted} = {value}")
        quoted_columns = ", ".join(self._column(column) for column in columns)
        source_values = ", ".join(
            f"source.{self._column(column)}" for column in columns
        )
        return (
            f"MERGE {self._table_ref(table)} AS target USING {stage} AS source "
            f"ON {self._join_sql(natural_keys)} "
            f"WHEN MATCHED AND ({self._change_sql(change_fields, repeated_fields)}) "
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
        try:
            for table, data in tables.items():
                self._load(
                    data,
                    stage_ids[table],
                    disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
            result = self.client.query(
                self._batch_script(batch, stages),
                job_config=self._query_config(
                    parameters=[
                        bigquery.ScalarQueryParameter("run_id", "STRING", batch.run_id)
                    ]
                ),
                location=self.location,
            ).result()
            row = next(iter(result), None)
            if row is None:
                raise RuntimeError("BigQuery persistence batch returned no summary row")
            already_committed = bool(row["already_committed"])
            per_table = {
                write.table: UpsertResult(
                    inserted=int(row[f"inserted_{write.table}"]),
                    updated=int(row[f"updated_{write.table}"]),
                )
                for write in batch.current_state
            }
            return BatchPersistResult(per_table, already_committed=already_committed)
        finally:
            self._delete_stages(tuple(stage_ids.values()))

    def _batch_script(
        self,
        batch: PersistenceBatch,
        stages: Mapping[str, str],
    ) -> str:
        """Build the remote staging and atomic persistence script.

        The temporary stage tables are loaded before the script. All user
        tables are then mutated only inside the transaction. ``ingest_runs``
        is inserted last, providing the idempotency ledger for retrying the
        same UUID after an ambiguous client-side outcome.
        """
        declarations = [
            "DECLARE already_committed BOOL DEFAULT EXISTS("
            f"SELECT 1 FROM {self._table_ref('ingest_runs')} "
            "WHERE `run_id` = @run_id);"
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
            "IF NOT already_committed THEN",
            "BEGIN TRANSACTION;",
        ]
        for write in batch.current_state:
            join = self._join_sql(write.natural_keys)
            repeated_fields = frozenset(
                field.name
                for field in write.data.schema
                if pa.types.is_list(field.type)
            )
            changes = self._change_sql(write.change_fields, repeated_fields)
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
                "COMMIT TRANSACTION;",
                "END IF;",
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
            if column in {"created_at", "first_seen_at"}:
                value = f"COALESCE(target.{quoted}, {value})"
            assignments.append(f"{quoted} = {value}")
        quoted_columns = ", ".join(self._column(column) for column in columns)
        source_values = ", ".join(
            f"source.{self._column(column)}" for column in columns
        )
        return (
            f"MERGE {self._table_ref(write.table)} AS target USING {stage} AS source "
            f"ON {self._join_sql(write.natural_keys)} "
            f"WHEN MATCHED AND ("
            f"{self._change_sql(write.change_fields, repeated_fields)}) "
            f"THEN UPDATE SET {', '.join(assignments)} "
            f"WHEN NOT MATCHED THEN INSERT ({quoted_columns}) VALUES ({source_values});"
        )

    @override
    def query(self, sql: str) -> pa.Table:
        """Run BigQuery Standard SQL and return Arrow without Storage API use."""
        return cast(
            pa.Table,
            self.client.query(
                sql,
                job_config=self._query_config(),
                location=self.location,
            )
            .result()
            .to_arrow(create_bqstorage_client=False),
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
