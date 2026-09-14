# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""duckdb_local.py — Local-file DuckDB StorageBackend implementation."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from importlib import resources
from pathlib import Path

import duckdb
import pyarrow as pa

from usagebassoon.backends.base import UpsertResult


def _identifier(value: str) -> str:
    """Return a safely quoted simple DuckDB identifier.

    Args:
        value: An internal table or column name.

    Returns:
        The quoted identifier.

    Raises:
        ValueError: If the value is not a simple identifier.
    """
    if not value.isidentifier():
        raise ValueError(f"expected a simple SQL identifier, got {value!r}")
    return f'"{value}"'


class _DuckDBStorage:
    """DuckDB SQL operations shared with the separate MotherDuck backend."""

    dialect = "duckdb"

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Take ownership of a DuckDB-compatible connection.

        Args:
            connection: Open connection used for backend operations.
        """
        self.connection = connection

    def apply_ddl(self) -> None:
        """Apply the shared DuckDB and MotherDuck DDL plus views."""
        package = resources.files("usagebassoon.sql.duckdb")
        for filename in ("ddl.sql", "views.sql"):
            self.connection.execute(package.joinpath(filename).read_text())

    @staticmethod
    def _validate_upsert(
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> None:
        """Validate the staged batch before building an upsert statement.

        Args:
            data: Staged Arrow batch.
            natural_keys: Current-state identity columns.
            change_fields: Logical columns used for change detection.

        Raises:
            ValueError: If the batch is malformed for a current-state upsert.
        """
        if not natural_keys:
            raise ValueError("at least one natural key is required")
        columns = set(data.column_names)
        required = set(natural_keys) | set(change_fields)
        if missing := required - columns:
            raise ValueError(f"staged data is missing columns: {sorted(missing)}")
        for column in (*natural_keys, *change_fields):
            _identifier(column)

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
        self._validate_upsert(data, natural_keys, change_fields)
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
            key_columns = ", ".join(map(_identifier, natural_keys))
            distinct_count = self.connection.execute(
                f"SELECT count(DISTINCT ({key_columns})) "
                f"FROM _usagebassoon_upsert_batch"
            ).fetchone()
            if distinct_count is None or int(distinct_count[0]) != data.num_rows:
                raise ValueError("staged data contains duplicate natural keys")
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

    def query(self, sql: str) -> pa.Table:
        """Execute DuckDB SQL and materialize the result as an Arrow table.

        Args:
            sql: DuckDB-dialect query.

        Returns:
            Materialized Arrow result table.
        """
        return self.connection.execute(sql).arrow().read_all()

    @contextmanager
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
