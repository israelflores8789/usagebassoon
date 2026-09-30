# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_sql_parity.py — Shared logical contracts with native ingestion layouts."""

from datetime import UTC, datetime, timedelta
from importlib import resources

import pyarrow as pa
import pytest
import sqlglot
from sqlglot import exp

from tests._bigquery_replay import BigQueryReplayBackend
from tests._sql_parity import (
    assert_view_results_match,
    normalized_records,
    seed_synthetic_data,
    statements,
    view_names,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS, normalize
from usagebassoon.storage_model import DEBUG_TABLES, EVENT_KEYS, STATE_KEYS

pytestmark = pytest.mark.sql_parity


def _tables(dialect: str) -> dict[str, exp.Create]:
    return {
        statement.this.this.name: statement
        for statement in statements(dialect, "ddl.sql")
        if isinstance(statement, exp.Create) and statement.kind == "TABLE"
    }


def _columns(table: exp.Create) -> dict[str, tuple[str, bool]]:
    result = {}
    for column in table.this.expressions:
        if not isinstance(column, exp.ColumnDef):
            continue
        kind = column.args["kind"].sql(dialect="duckdb")
        if kind == "INT":
            kind = "BIGINT"
        if kind in {"TIMESTAMP", "TIMESTAMPTZ"}:
            kind = "timestamp"
        constraints = [item.sql().upper() for item in column.args["constraints"]]
        result[column.name] = (
            kind,
            "NOT NULL" in constraints or "PRIMARY KEY" in constraints,
        )
    return result


def test_logical_gold_and_event_schemas_match() -> None:
    duckdb, bigquery = _tables("duckdb"), _tables("bigquery")
    for table in STATE_KEYS | EVENT_KEYS:
        physical = "raw_" + table if table in DEBUG_TABLES else table
        assert _columns(duckdb[table]) == _columns(bigquery[physical]), table
    for dialect in (duckdb, bigquery):
        for table in dialect.values():
            assert _columns(table)["source_id"][1]
    for table in STATE_KEYS:
        assert _columns(bigquery[table]) == _columns(bigquery["raw_" + table])
    logical = set(STATE_KEYS) | set(EVENT_KEYS)
    metadata = {"schema_marker", "schema_migrations"}
    assert set(duckdb) == logical | metadata
    assert set(bigquery) == (
        set(STATE_KEYS)
        | {"raw_" + table for table in set(STATE_KEYS) | DEBUG_TABLES}
        | {"collection_ledger", "compaction_ledger"}
        | metadata
    )


def test_packaged_asset_inventory_and_native_parsing() -> None:
    for dialect in ("duckdb", "bigquery"):
        package = resources.files(f"usagebassoon.sql.{dialect}")
        expected = {"ddl.sql", "views.sql"} | (
            {"compaction.sql"} if dialect == "bigquery" else set()
        )
        actual = {item.name for item in package.iterdir() if item.name.endswith(".sql")}
        assert actual == expected
        for filename in actual:
            assert sqlglot.parse(package.joinpath(filename).read_text(), read=dialect)


def test_report_view_asts_remain_equivalent() -> None:
    def logical_views(dialect: str) -> dict[str, str]:
        result = {}
        for statement in statements(dialect, "views.sql"):
            name = statement.this.name
            if name.startswith(("current_", "replay_")) or name in DEBUG_TABLES | {
                "compaction_backlog"
            }:
                continue
            for kind in statement.find_all(exp.DataType):
                if kind.this == exp.DataType.Type.TIMESTAMPTZ:
                    kind.set("this", exp.DataType.Type.TIMESTAMP)
            for ordered in statement.find_all(exp.Ordered):
                ordered.set("nulls_first", None)
            result[name] = statement.sql(dialect="duckdb", comments=False)
        return result

    assert logical_views("duckdb") == logical_views("bigquery")


def test_synthetic_raw_and_gold_replay_matches_shared_views() -> None:
    local = DuckDBBackend(":memory:")
    remote = BigQueryReplayBackend()
    try:
        local.apply_ddl()
        seed_synthetic_data(local)
        seed_synthetic_data(remote)
        seed_synthetic_data(remote)
        assert remote.query("SELECT * FROM daily_stats").num_rows == 0
        assert (
            remote.query("SELECT * FROM raw_daily_stats").num_rows
            > local.query("SELECT * FROM daily_stats").num_rows
        )
        assert_view_results_match(local, remote, view_names())
    finally:
        local.close()
        remote.close()


def test_logical_ties_beat_uuid_order_in_both_ingestion_models(
    collection_bundle: CollectionBundle,
) -> None:
    """Select domain-preferred values even when their UUID loses lexical ordering."""
    local = DuckDBBackend(":memory:")
    remote = BigQueryReplayBackend()
    try:
        local.apply_ddl()
        seed_synthetic_data(local)
        seed_synthetic_data(remote)
        stamp = datetime(2026, 10, 1, tzinfo=UTC)
        ledger = normalize(collection_bundle).tables["collection_ledger"]
        local.append("collection_ledger", ledger)
        remote.append("collection_ledger", ledger)
        for table in DEBUG_TABLES:
            schema = CANONICAL_TABLE_SCHEMAS[table]
            row: dict[str, object] = {field.name: "synthetic" for field in schema}
            row.update(
                source_id=collection_bundle.source_id,
                run_id=collection_bundle.run_id,
                event_id="00000000-0000-4000-8000-000000000002",
                created_at=stamp - timedelta(days=1),
                collected_at=stamp - timedelta(days=1),
                resolved=False,
                observation_count=1,
            )
            data = pa.Table.from_pylist([row], schema=schema)
            local.append(table, data)
            remote.append(table, data)
        for table in (
            "sessions",
            "daily_stats",
            "price_versions",
            "tags",
            "notes",
            "schema_drift_events",
            "reconciliation_issues",
            "collection_ledger",
        ):
            seed = local.query(f"SELECT * FROM current_{table} LIMIT 1").to_pylist()[0]
            preferred: dict[str, object] = dict(
                seed,
                collected_at=stamp,
                event_id="00000000-0000-4000-8000-000000000001",
            )
            other: dict[str, object] = dict(
                seed,
                collected_at=stamp,
                event_id="ffffffff-ffff-4fff-bfff-ffffffffffff",
            )
            if table == "sessions":
                preferred["event_id"], other["event_id"] = (
                    other["event_id"],
                    preferred["event_id"],
                )
                preferred["created_at"] = stamp
                preferred["first_seen_at"] = stamp + timedelta(days=1)
                preferred["last_seen_at"] = stamp + timedelta(days=1)
                other["last_seen_at"] = stamp + timedelta(days=2)
            elif table == "daily_stats":
                preferred["total_tokens"] = int(seed["total_tokens"]) + 100
                preferred["input_tokens"] = int(seed["input_tokens"]) + 100
            elif table == "price_versions":
                preferred["price_output_per_token"] = -1.0
                other["price_output_per_token"] = None
            elif table in {"tags", "notes"}:
                preferred["op"], other["op"] = "upsert", "delete"
                if table == "tags":
                    preferred["source_id"] = "preferred-source"
                    other["source_id"] = "other-source"
                preferred["created_at"] = stamp
                preferred["updated_at"] = stamp
                other["created_at"] = stamp - timedelta(days=10)
            elif table in DEBUG_TABLES:
                preferred["resolved"], other["resolved"] = True, False
                preferred["observation_count"] = 0
            else:
                preferred["finished_at"] = stamp + timedelta(seconds=1)
                other["finished_at"] = stamp
            for row in (preferred, other):
                data = pa.Table.from_pylist(
                    [row], schema=CANONICAL_TABLE_SCHEMAS[table]
                )
                if table in STATE_KEYS:
                    local.upsert(table, data, STATE_KEYS[table], ())
                else:
                    local.append(table, data)
                remote.append(table, data)
            assert normalized_records(
                local.query(f"SELECT * FROM current_{table}")
            ) == normalized_records(remote.query(f"SELECT * FROM current_{table}"))
            rows = local.query(f"SELECT * FROM current_{table}").to_pylist()
            assert any(row["event_id"] == preferred["event_id"] for row in rows), table
            assert not any(row["event_id"] == other["event_id"] for row in rows), table
    finally:
        local.close()
        remote.close()
