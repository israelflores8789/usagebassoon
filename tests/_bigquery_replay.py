# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_bigquery_replay.py — BigQuery writer and SQL replay in a local DuckDB engine.

This harness excludes provider time travel, progress accounting, and scheduling;
native BigQuery integration tests exercise those warehouse behaviors.
"""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from importlib import resources
from typing import cast, override
from unittest.mock import MagicMock

import pyarrow as pa
import sqlglot
from google.cloud import bigquery
from sqlglot import exp

from tests._sql_parity import statements
from usagebassoon.backends.base import SnapshotRead
from usagebassoon.backends.bigquery import BigQueryBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS
from usagebassoon.storage_model import DEBUG_TABLES


def install_bigquery_replay(backend: DuckDBBackend) -> None:
    """Install BigQuery physical tables and canonical views in the local engine."""
    for statement in statements("bigquery", "ddl.sql"):
        if isinstance(statement, exp.Create) and statement.kind == "TABLE":
            statement.set("properties", None)
            backend.connection.execute(statement.sql(dialect="duckdb"))
    for statement in statements("bigquery", "views.sql"):
        if statement.this.name != "compaction_backlog":
            backend.connection.execute(statement.sql(dialect="duckdb"))


class BigQueryReplayBackend(BigQueryBackend):
    """Execute BigQuery writers and replay its SQL using a local test engine."""

    def __init__(self) -> None:
        """Install the BigQuery physical schema and transpiled canonical views."""
        super().__init__(
            "usagebassoon-test",
            "usagebassoon_emulated",
            client=cast(bigquery.Client, MagicMock(spec=bigquery.Client)),
        )
        self.engine = DuckDBBackend(":memory:")
        install_bigquery_replay(self.engine)

    @override
    def query(self, sql: str, parameters: Mapping[str, str] | None = None) -> pa.Table:
        """Read the actual canonical views while avoiding cloud transport."""
        return self.engine.query(sql.replace("`", '"'), parameters)

    @override
    def _load(
        self,
        data: pa.Table,
        destination: str,
        *,
        disposition: str,
        schema: Sequence[bigquery.SchemaField] | None = None,
    ) -> None:
        """Record the canonical batch produced by the real BigQuery adapter."""
        assert disposition == "WRITE_APPEND"
        self.engine.append(destination.rsplit(".", 1)[-1], data)

    def compact(self) -> None:
        """Replay shipped curation winner SQL and replacements, retaining tombstones."""
        sql = (
            resources.files("usagebassoon.sql.bigquery")
            .joinpath("compaction.sql")
            .read_text()
        )
        parsed = [
            statement for statement in sqlglot.parse(sql, read="bigquery") if statement
        ]
        with self.engine.transaction():
            for table in ("tags", "notes"):
                self.engine.connection.execute(
                    f"DROP TABLE IF EXISTS candidates_{table}"
                )
                self.engine.connection.execute(f"DROP TABLE IF EXISTS winners_{table}")
                self.engine.connection.execute(
                    f"CREATE TEMP TABLE candidates_{table} AS "
                    f"SELECT DISTINCT source_id FROM raw_{table}"
                )
                self.engine.connection.execute(f"DROP TABLE IF EXISTS keys_{table}")
                for statement in parsed:
                    if isinstance(statement, exp.Create) and statement.this.name in {
                        f"keys_{table}",
                        f"winners_{table}",
                    }:
                        for relation in statement.find_all(exp.Table):
                            relation.set("version", None)
                        self.engine.connection.execute(statement.sql(dialect="duckdb"))
                    elif (
                        isinstance(statement, (exp.Delete, exp.Insert))
                        and (
                            statement.this.this.name
                            if isinstance(statement.this, exp.Schema)
                            else statement.this.name
                        )
                        == table
                    ):
                        self.engine.connection.execute(statement.sql(dialect="duckdb"))

    @override
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Capture canonical BigQuery views with the production portable schemas."""
        with self.engine.transaction():
            captured = datetime.now(UTC)
            materialized = {
                table: self.query(
                    f"SELECT * FROM "
                    f"{'replay_' if table in DEBUG_TABLES else 'current_'}{table}"
                )
                .select(CANONICAL_TABLE_SCHEMAS[table].names)
                .cast(CANONICAL_TABLE_SCHEMAS[table])
                for table in tables
            }
        return SnapshotRead(captured, materialized)

    @override
    def close(self) -> None:
        """Close the local replay engine."""
        self.engine.close()
