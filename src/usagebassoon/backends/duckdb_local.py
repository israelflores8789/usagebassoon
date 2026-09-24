# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""duckdb_local.py — Local-file DuckDB StorageBackend implementation."""

from __future__ import annotations

import re
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from importlib import resources
from pathlib import Path
from typing import override

import duckdb
import pyarrow as pa

from usagebassoon.backends.base import (
    AbstractStorageBackend,
    CuratedIdentity,
    CuratedRenameError,
    CuratedRenameResult,
    UpsertResult,
    is_simple_identifier,
)
from usagebassoon.schema_assets import RUNTIME_SCHEMA_ASSETS


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
        """Apply the shared DuckDB and MotherDuck DDL plus views."""
        package = resources.files("usagebassoon.sql.duckdb")
        for filename in RUNTIME_SCHEMA_ASSETS:
            self.connection.execute(package.joinpath(filename).read_text())

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
            change_fields: Columns compared null-safely for material changes.

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
        change_predicate = (
            " OR ".join(
                f"target.{_identifier(field)} IS DISTINCT FROM "
                f"source.{_identifier(field)}"
                for field in change_fields
            )
            or "FALSE"
        )
        if "updated_at" in columns:
            change_predicate = (
                f"source.updated_at >= target.updated_at AND ({change_predicate})"
            )
        assignments: list[str] = []
        for column in columns:
            quoted_column = _identifier(column)
            value = f"source.{quoted_column}"
            if table == "reconciliation_issues" and column == "message":
                value = (
                    'CASE WHEN source."resolved_at" IS NOT NULL '
                    f"THEN target.{quoted_column} ELSE {value} END"
                )
            elif column in {"created_at", "first_seen_at", "detected_run_id", "run_id"}:
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
        """Return whether an ingest audit row already owns a run identifier."""
        result = self.connection.execute(
            "SELECT 1 FROM ingest_runs WHERE run_id = ? LIMIT 1",
            [run_id],
        ).fetchone()
        return result is not None

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
            f"{_identifier(name)} = ${name}" for name, _ in identity.values
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
            "updated_at": updated_at,
        }
        source_predicate = " AND ".join(
            f"{_identifier(name)} = $source_{name}" for name, _ in source.values
        )
        destination_predicate = " AND ".join(
            f"{_identifier(name)} = $destination_{name}"
            for name, _ in destination.values
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
                "tag, created_at, updated_at) "
                "SELECT source_id, scope, client, workspace, session_id, "
                ' $destination_tag, created_at, $updated_at FROM "tags" WHERE '
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
