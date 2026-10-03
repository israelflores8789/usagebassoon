# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""duckdb_local.py — Local-file DuckDB StorageBackend implementation."""

from __future__ import annotations

import re
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from importlib import resources
from pathlib import Path
from typing import cast, override
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from usagebassoon.backends.base import (
    AbstractStorageBackend,
    CuratedIdentity,
    CuratedRenameError,
    CuratedRenameResult,
    SnapshotRead,
    SnapshotStream,
    StorageBackend,
    UpsertResult,
    is_simple_identifier,
)
from usagebassoon.schema_assets import (
    RUNTIME_SCHEMA_ASSETS,
    SCHEMA_VERSION,
    schema_hash,
)
from usagebassoon.storage_model import (
    CANONICAL_TABLE_SCHEMAS,
    DEBUG_TABLES,
    SNAPSHOT_TABLES,
    newer_observation,
)


def _identifier(value: str) -> str:
    """Return a safely quoted simple DuckDB identifier.

    Args:
        value: An internal table or column name.

    Returns:
        The quoted identifier.

    Raises:
        ValueError: If the value is not a simple identifier.
    """
    if not is_simple_identifier(value):
        raise ValueError(f"expected a simple SQL identifier, got {value!r}")
    return f'"{value}"'


class _DuckDBStorage(AbstractStorageBackend):
    """DuckDB SQL operations shared with the separate MotherDuck backend."""

    dialect = "duckdb"

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Take ownership of a DuckDB-compatible connection.

        Args:
            connection: Open connection used for backend operations.
        """
        self.connection = connection

    @override
    def apply_ddl(self) -> None:
        """Provision a fresh baseline or validate an existing initialized schema."""
        exists = self.connection.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'schema_marker' "
            "AND table_schema = current_schema() AND table_catalog = current_database()"
        ).fetchone()
        if exists:
            self.preflight()
            return
        package = resources.files("usagebassoon.sql.duckdb")
        with self.transaction():
            for filename in RUNTIME_SCHEMA_ASSETS:
                self.connection.execute(package.joinpath(filename).read_text())
            self.connection.execute(
                "INSERT INTO schema_marker VALUES (?, ?, ?)",
                [
                    "00000000-0000-0000-0000-000000000000",
                    SCHEMA_VERSION,
                    schema_hash("duckdb"),
                ],
            )

    @override
    def preflight(self) -> None:
        """Read the schema marker; never implicitly provision a fresh database."""
        try:
            row = self.connection.execute(
                "SELECT version, schema_hash FROM schema_marker"
            ).fetchone()
        except duckdb.CatalogException as error:
            raise RuntimeError(
                "warehouse is not initialized; run bassoon init"
            ) from error
        if row is None:
            raise RuntimeError("warehouse has no schema marker; run bassoon init")
        from usagebassoon.schema_assets import pending_migrations

        steps = pending_migrations(int(row[0]), str(row[1]), "duckdb")
        if steps:
            with self.transaction():
                for step in steps:
                    self.connection.execute(step.sql("duckdb"))
                    self.connection.execute(
                        "INSERT INTO schema_migrations "
                        "VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                        [
                            "00000000-0000-0000-0000-000000000000",
                            step.version,
                            step.target_hashes["duckdb"],
                        ],
                    )
                self.connection.execute(
                    "UPDATE schema_marker SET version = ?, schema_hash = ?",
                    [SCHEMA_VERSION, schema_hash("duckdb")],
                )

    @override
    def is_retryable_error(self, error: Exception) -> bool:
        """Retry concurrent row and catalog transaction conflicts."""
        return isinstance(error, duckdb.TransactionException) and (
            "conflict" in str(error).casefold()
        )

    @override
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Capture canonical state and deduplicated ledger rows in one transaction."""
        result: dict[str, pa.Table] = {}
        with self.transaction():
            captured = self.query("SELECT CURRENT_TIMESTAMP AS stamp").to_pylist()[0]
            assert captured is not None
            for table in tables:
                relation = (
                    f"current_{table}"
                    if table not in DEBUG_TABLES
                    else "replay_" + table
                )
                result[table] = (
                    self.query(f"SELECT * FROM {_identifier(relation)}")
                    .select(CANONICAL_TABLE_SCHEMAS[table].names)
                    .cast(CANONICAL_TABLE_SCHEMAS[table])
                )
        return SnapshotRead(
            captured_at=cast(datetime, captured["stamp"]), tables=result
        )

    @contextmanager
    @override
    def stream_snapshot(self, tables: Sequence[str]) -> Generator[SnapshotStream]:
        """Hold one transactional read point while each Arrow stream is consumed.

        Yields:
            Lazy sequential streams from the owned connection.
        """
        from collections.abc import Iterable

        def batches(table: str) -> Iterable[pa.RecordBatch]:
            """Consume one canonical relation without whole-table materialization."""
            if table not in SNAPSHOT_TABLES:
                raise ValueError(f"unsupported snapshot table: {table}")
            relation = ("replay_" if table in DEBUG_TABLES else "current_") + table
            schema = CANONICAL_TABLE_SCHEMAS[table]
            reader = self.connection.execute(
                f"SELECT * FROM {_identifier(relation)}"
            ).to_arrow_reader(65536)
            for batch in reader:
                yield from (
                    pa.Table.from_batches([batch])
                    .select(schema.names)
                    .cast(schema)
                    .to_batches(max_chunksize=65536)
                )

        with self.consistent_read():
            captured = cast(
                datetime,
                self.query("SELECT CURRENT_TIMESTAMP AS stamp").to_pylist()[0]["stamp"],
            )
            yield SnapshotStream(captured, {table: batches(table) for table in tables})

    @override
    def check_restore_empty(self) -> None:
        """Inspect all destination base tables, including unexpected tables."""
        names = self.connection.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_catalog = current_database() AND table_type = 'BASE TABLE'"
        ).fetchall()
        for namespace, name in names:
            if namespace == "main" and name in {
                "schema_marker",
                "schema_migrations",
                "restore_receipts",
            }:
                continue
            quoted_schema = '"' + str(namespace).replace('"', '""') + '"'
            quoted_name = '"' + str(name).replace('"', '""') + '"'
            if (
                self.connection.execute(
                    f"SELECT 1 FROM {quoted_schema}.{quoted_name} LIMIT 1"
                ).fetchone()
                is not None
            ):
                raise ValueError(
                    f"restore requires an empty warehouse: populated {namespace}.{name}"
                )

    @override
    def prepare_recovery(self, *, notice: Callable[[str], None] | None = None) -> None:
        """Transactional upsert storage has no scheduled maintenance to disable."""
        del notice

    @override
    def snapshot_provenance(self) -> dict[str, object]:
        """Identify a DuckDB producer and its installed physical schema."""
        return {
            "source_backend": "duckdb",
            "backend_schema_version": SCHEMA_VERSION,
            "backend_schema_hash": schema_hash("duckdb"),
        }

    @override
    def configure_maintenance(self, *, enabled: bool) -> str | None:
        """Report inapplicability of maintenance for transactional upsert storage."""
        del enabled
        return None

    @override
    def maintenance_status(self) -> tuple[bool, str] | None:
        """DuckDB transactional upserts need no scheduled maintenance."""
        return None

    @override
    def restore_stages(self) -> list[dict[str, object]]:
        """DuckDB restores batches directly in a transaction without staging."""
        return []

    @override
    def cleanup_restore_stages(self) -> None:
        """DuckDB creates no persistent restore stages."""

    @override
    def restore_committed(self, operation_id: str) -> bool:
        """Read the atomic completion receipt for a restore operation."""
        return (
            self.connection.execute(
                "SELECT 1 FROM restore_receipts WHERE operation_id = ?", [operation_id]
            ).fetchone()
            is not None
        )

    @override
    def restore_snapshot(
        self, files: Mapping[str, Path], *, operation_id: str, snapshot_id: str
    ) -> None:
        """Insert verified canonical batches and a receipt in one transaction."""
        if set(files) - set(SNAPSHOT_TABLES):
            raise ValueError("unsupported restore tables")
        with self.transaction():
            if self.restore_committed(operation_id):
                return
            self.check_restore_empty()
            for table, path in files.items():
                for batch in pq.ParquetFile(path).iter_batches(batch_size=65536):
                    self.append(
                        table,
                        pa.Table.from_batches([batch]).cast(
                            CANONICAL_TABLE_SCHEMAS[table]
                        ),
                    )
            self.connection.execute(
                "INSERT INTO restore_receipts VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                ["00000000-0000-0000-0000-000000000000", operation_id, snapshot_id],
            )

    @override
    def restore_tables(self, tables: Mapping[str, pa.Table]) -> None:
        """Restore importable Arrow tables with the same complete emptiness check."""
        if set(tables) - set(SNAPSHOT_TABLES):
            raise ValueError("unsupported restore tables")
        with self.transaction():
            self.check_restore_empty()
            for table, data in tables.items():
                self.append(table, data)

    @override
    def upsert(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> UpsertResult:
        """Insert new facts and update changed current-state facts in place.

        Args:
            table: Target current-state table.
            data: Normalized Arrow batch.
            natural_keys: Columns identifying a current row.
            change_fields: Fields validated against the incoming table schema.

        Returns:
            Counts of rows inserted and updated.
        """
        if data.num_rows == 0:
            return UpsertResult()
        quoted_table = _identifier(table)
        self._validate_upsert(table, data, natural_keys, change_fields)
        columns = tuple(data.column_names)
        quoted_columns = ", ".join(map(_identifier, columns))
        join = " AND ".join(
            f"target.{_identifier(key)} = source.{_identifier(key)}"
            for key in natural_keys
        )
        change_predicate = newer_observation(table)
        assignments: list[str] = []
        for column in columns:
            quoted_column = _identifier(column)
            value = f"source.{quoted_column}"
            if column == "first_seen_at":
                value = f"COALESCE(target.{quoted_column}, {value})"
            assignments.append(f"{quoted_column} = {value}")
        source_values = ", ".join(f"source.{_identifier(column)}" for column in columns)

        self.connection.register("_usagebassoon_upsert_batch", data)
        try:
            counted = self.connection.execute(
                f"SELECT "
                f"count(*) FILTER (WHERE "
                f"target.{_identifier(natural_keys[0])} IS NULL), "
                f"count(*) FILTER (WHERE "
                f"target.{_identifier(natural_keys[0])} IS NOT NULL "
                f"AND ({change_predicate})) "
                f"FROM _usagebassoon_upsert_batch source "
                f"LEFT JOIN {quoted_table} target ON {join}"
            ).fetchone()
            if counted is None:
                return UpsertResult()
            inserted, updated = counted
            self.connection.execute(
                f"MERGE INTO {quoted_table} target "
                f"USING _usagebassoon_upsert_batch source ON {join} "
                f"WHEN MATCHED AND ({change_predicate}) THEN UPDATE SET "
                f"{', '.join(assignments)} "
                f"WHEN NOT MATCHED THEN INSERT ({quoted_columns}) "
                f"VALUES ({source_values})"
            )
            if table == "sessions":
                self.connection.execute(
                    f"UPDATE {quoted_table} AS target SET "
                    "first_seen_at = "
                    "LEAST(target.first_seen_at, source.first_seen_at), "
                    "last_seen_at = GREATEST(target.last_seen_at, source.last_seen_at) "
                    f"FROM _usagebassoon_upsert_batch AS source WHERE {join}"
                )
        finally:
            self.connection.unregister("_usagebassoon_upsert_batch")
        return UpsertResult(inserted=int(inserted), updated=int(updated))

    @override
    def append(self, table: str, data: pa.Table) -> None:
        """Append an Arrow batch to an append-only table.

        Args:
            table: Target append-only table.
            data: Normalized Arrow batch.
        """
        if data.num_rows == 0:
            return
        if table in DEBUG_TABLES:
            self.connection.execute(
                f"DELETE FROM {_identifier(table)} "
                "WHERE collected_at < CURRENT_TIMESTAMP - INTERVAL '90 days'"
            )
        quoted_columns = ", ".join(map(_identifier, data.column_names))
        self.connection.register("_usagebassoon_append_batch", data)
        try:
            self.connection.execute(
                f"INSERT INTO {_identifier(table)} ({quoted_columns}) "
                f"SELECT {quoted_columns} FROM _usagebassoon_append_batch"
            )
        finally:
            self.connection.unregister("_usagebassoon_append_batch")

    @override
    def has_committed_run(self, run_id: str) -> bool:
        """Return whether this run's collection summary was committed atomically."""
        return (
            self.connection.execute(
                "SELECT 1 FROM collection_ledger "
                "WHERE run_id = ? AND domain = 'collection'",
                [run_id],
            ).fetchone()
            is not None
        )

    @override
    def query(self, sql: str, parameters: Mapping[str, str] | None = None) -> pa.Table:
        """Execute DuckDB SQL and materialize the result as an Arrow table.

        Args:
            sql: DuckDB-dialect query.
            parameters: Named string values bound without SQL interpolation.

        Returns:
            Materialized Arrow result table.
        """
        bindings = parameters or {}
        statement = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", r"$\1", sql)
        return self.connection.execute(statement, bindings).arrow().read_all()

    @override
    def delete_curated(self, identity: CuratedIdentity) -> int:
        """Delete one completely identified notes or tags row."""
        predicates = " AND ".join(
            f"{_identifier(name)} = ${name}" for name, _ in identity.target_values
        )
        result = self.connection.execute(
            f"DELETE FROM {_identifier(identity.table)} WHERE {predicates} RETURNING 1",
            identity.parameters(),
        ).fetchall()
        return len(result)

    @override
    def rename_curated(
        self,
        source: CuratedIdentity,
        destination: CuratedIdentity,
        *,
        updated_at: datetime,
    ) -> CuratedRenameResult:
        """Rename one tag assignment in a transaction without duplication."""
        if source.table != "tags" or destination.table != "tags":
            raise ValueError("only tag assignments can be renamed")
        source_parameters = source.parameters(prefix="source_")
        destination_parameters = destination.parameters(prefix="destination_")
        insert_parameters = {
            **source_parameters,
            "destination_tag": destination_parameters["destination_tag"],
            "collected_at": updated_at,
            "event_id": str(uuid4()),
            "op_id": str(uuid4()),
            "source_id": source.source_id,
        }
        source_predicate = " AND ".join(
            f"{_identifier(name)} = $source_{name}" for name, _ in source.target_values
        )
        destination_predicate = " AND ".join(
            f"{_identifier(name)} = $destination_{name}"
            for name, _ in destination.target_values
        )
        with self.transaction():
            source_exists = self.connection.execute(
                f'SELECT EXISTS(SELECT 1 FROM "tags" WHERE {source_predicate})',
                source_parameters,
            ).fetchone()
            if source_exists is None or not source_exists[0]:
                return CuratedRenameResult(renamed=False)
            destination_exists = self.connection.execute(
                f'SELECT EXISTS(SELECT 1 FROM "tags" WHERE {destination_predicate})',
                destination_parameters,
            ).fetchone()
            if destination_exists is not None and destination_exists[0]:
                return CuratedRenameResult(renamed=False, destination_exists=True)
            insert_sql = (
                'INSERT INTO "tags" '
                "(source_id, scope, client, workspace, session_id, "
                "tag, created_at, updated_at, collected_at, event_id, op, op_id) "
                "SELECT $source_id, scope, client, workspace, session_id, "
                " $destination_tag, created_at, $collected_at, $collected_at, "
                "$event_id, 'upsert', $op_id FROM \"tags\" WHERE "
                f"{source_predicate}"
            )
            self.connection.execute(
                insert_sql,
                insert_parameters,
            )
            deleted = self.connection.execute(
                f'DELETE FROM "tags" WHERE {source_predicate} RETURNING 1',
                source_parameters,
            ).fetchall()
            if len(deleted) != 1:
                raise CuratedRenameError(
                    "tag rename deletion did not remove exactly one row"
                )
        return CuratedRenameResult(renamed=True)

    @contextmanager
    @override
    def transaction(self) -> Generator[None]:
        """Yield a DuckDB transaction that commits only if its body succeeds.

        Yields:
            No value.
        """
        self.connection.execute("BEGIN TRANSACTION")
        try:
            yield
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    @override
    def close(self) -> None:
        """Close the owned connection."""
        self.connection.close()


class DuckDBBackend(_DuckDBStorage):
    """StorageBackend backed by a local DuckDB database."""

    @contextmanager
    @override
    def consistent_read(self) -> Generator[StorageBackend]:
        """Keep diagnostic queries on the owned connection in one transaction."""
        with self.transaction():
            yield self

    @property
    @override
    def max_concurrent_queries(self) -> int:
        """Keep independent reads sequential on the owned DuckDB connection."""
        return 1

    @override
    def compaction_backlog(self) -> pa.Table | None:
        """Return None because transactional upserts require no compaction."""
        return None

    def __init__(self, database: str | Path) -> None:
        """Open a local database, creating parent directories as needed.

        Args:
            database: Database file path, or ':memory:'.
        """
        path = str(Path(database).expanduser()) if database != ":memory:" else database
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.database = path
        super().__init__(duckdb.connect(path))
