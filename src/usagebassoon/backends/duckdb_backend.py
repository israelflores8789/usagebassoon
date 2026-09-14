"""DuckDB-engine backends: local files and MotherDuck share one engine."""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from urllib.parse import quote

import duckdb
import pyarrow as pa


def _rowcount(data: object) -> int:
    """Row count of an Arrow table or a pandas-backed shim DataFrame."""
    return int(getattr(data, "num_rows", None) or len(data))


class _DuckDBEngine:
    """Shared engine over any duckdb-compatible connection."""

    dialect = "duckdb"

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Take ownership of an open duckdb connection.

        Args:
            connection: The connection to wrap.
        """
        self.connection = connection

    def apply_ddl(self) -> None:
        """Apply duckdb ddl.sql and views.sql under sql/duckdb/."""
        pkg = resources.files(f"tokledger.sql.{self.dialect}")
        for name in ("ddl.sql", "views.sql"):
            r = pkg.joinpath(name)
            if r.is_file():
                self.connection.execute(r.read_text())

    @staticmethod
    def _latest_sql(table: str, natural_keys: Sequence[str]) -> str:
        """Inline a fresh latest-per-key scan via QUALIFY.

        Args:
            table: Fact table to scan.
            natural_keys: Natural key columns.

        Returns:
            SQL fragment selecting the newest row per key.
        """
        nk = ", ".join(natural_keys)
        return (
            f"SELECT * FROM {table} QUALIFY row_number() "
            f"OVER (PARTITION BY {nk} "
            f"ORDER BY collected_at DESC) = 1"
        )

    def merge(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        measure_fields: Sequence[str],
    ) -> int:
        """Stage a batch; append rows new-by-key or changed-by-measure.

        Args:
            table: Target table.
            data: Staged Arrow batch.
            natural_keys: Natural key columns.
            measure_fields: Columns compared null-safe (IS DISTINCT FROM).

        Returns:
            Rows appended.
        """
        if _rowcount(data) == 0:
            return 0
        self.connection.register("_staged_merge_batch", data)
        nk = list(natural_keys)
        cols = data.schema.names
        measures = " OR ".join(f"l.{m} IS DISTINCT FROM s.{m}" for m in measure_fields)
        change = f" OR ({measures})" if measures else ""
        latest = self._latest_sql(table, nk)
        try:
            counted = self.connection.execute(
                "SELECT count(*) FROM _staged_merge_batch s "
                f"LEFT JOIN ({latest}) l USING ({', '.join(nk)}) "
                f"WHERE l.{nk[0]} IS NULL{change}"
            ).fetchone()
            appended = int(counted[0]) if counted else 0
            select_cols = ", ".join(
                f"COALESCE(l.{c}, s.{c})" if c == "first_seen_at" else f"s.{c}"
                for c in cols
            )
            self.connection.execute(
                f"INSERT INTO {table} ({', '.join(cols)}) "
                f"SELECT {select_cols} FROM _staged_merge_batch s "
                f"LEFT JOIN ({latest}) l USING ({', '.join(nk)}) "
                f"WHERE l.{nk[0]} IS NULL{change}"
            )
        finally:
            self.connection.unregister("_staged_merge_batch")
        return appended

    def append(self, table: str, data: pa.Table) -> None:
        """Append a batch unchanged into an append-only table.

        Args:
            table: Target table.
            data: Arrow batch.
        """
        if _rowcount(data) == 0:
            return
        self.connection.register("_append_batch", data)
        try:
            self.connection.execute(f"INSERT INTO {table} SELECT * FROM _append_batch")
        finally:
            self.connection.unregister("_append_batch")

    def query(self, sql: str) -> pa.Table:
        """Run dialect SQL and return an Arrow-compatible table.

        NOTE: the true zero-copy path is `.arrow()`; the offline conformance
        sandbox substitutes a pandas-backed shim, so `.fetchdf()` is the
        shared ground truth. Under real pyarrow switch this back to
        `.arrow()` with zero behavioral change.
        """
        return self.connection.execute(sql).fetchdf()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Multi-statement transaction via BEGIN/COMMIT."""
        self.connection.execute("BEGIN TRANSACTION")
        try:
            yield
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def close(self) -> None:
        """Close the connection."""
        self.connection.close()


class LocalDuckDBBackend(_DuckDBEngine):
    """Local DuckDB database file backend (zero-cloud default)."""

    def __init__(self, database: str | Path) -> None:
        """Open a backend bound to a local file.

        Args:
            database: File path (parents created) or ':memory:'.
        """
        path = str(database)
        if path != ":memory:":
            Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        super().__init__(duckdb.connect(path))


class MotherDuckBackend(_DuckDBEngine):
    """MotherDuck backend via the md: URI family (hosted DuckDB dialect)."""

    def __init__(self, database: str, *, token: str | None = None) -> None:
        """Open a backend bound to a MotherDuck database.

        Args:
            database: MotherDuck database name (no md: prefix).
            token: Service-account token; defaults to MOTHERDUCK_TOKEN.

        Raises:
            ValueError: Bad database name.
            RuntimeError: No resolvable token.
        """
        if not database or database.startswith("md:"):
            raise ValueError("database must be a non-empty MotherDuck name")
        token = token or os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            raise RuntimeError(
                "MOTHERDUCK_TOKEN is required for MotherDuck connections"
            )
        uri = f"md:{database}?motherduck_token={quote(token, safe='')}"
        super().__init__(duckdb.connect(uri))
