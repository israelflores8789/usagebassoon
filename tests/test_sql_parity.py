# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_sql_parity.py — Shared logical contracts with native ingestion layouts."""

from datetime import UTC, date, datetime, timedelta
from importlib import resources
from pathlib import Path
from string import Formatter
from uuid import uuid4

import pyarrow as pa
import pytest
import sqlglot
from sqlglot import exp

from tests._bigquery_replay import BigQueryReplayBackend
from tests._sql_parity import (
    assert_report_results_match,
    assert_view_results_match,
    normalized_records,
    seed_synthetic_data,
    statements,
    view_names,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.reports._common import (
    ReportFilters,
    SessionSort,
    load_session_usage,
)
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS, normalize
from usagebassoon.schema_assets import QUERY_ASSET, _query_templates, query_sql
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
    for dialect in (duckdb, bigquery):
        ledger_columns = _columns(dialect["collection_ledger"])
        assert "invoke_method" in ledger_columns
        assert "shell" not in ledger_columns
    for table in STATE_KEYS | EVENT_KEYS:
        physical = "raw_" + table if table in DEBUG_TABLES else table
        assert _columns(duckdb[table]) == _columns(bigquery[physical]), table
    for dialect in (duckdb, bigquery):
        for table in dialect.values():
            assert _columns(table)["source_id"][1]
    for table in STATE_KEYS:
        assert _columns(bigquery[table]) == _columns(bigquery["raw_" + table])
    logical = set(STATE_KEYS) | set(EVENT_KEYS)
    metadata = {"schema_marker", "schema_migrations", "restore_receipts"}
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
        expected = {"ddl.sql", "views.sql", QUERY_ASSET} | (
            {"compaction.sql"} if dialect == "bigquery" else set()
        )
        actual = {item.name for item in package.iterdir() if item.name.endswith(".sql")}
        assert actual == expected
        for filename in actual:
            if filename != QUERY_ASSET:
                assert sqlglot.parse(
                    package.joinpath(filename).read_text(), read=dialect
                )


def test_named_query_inventory_and_native_asts_match() -> None:
    """Keep named queries paired, read-only, and limited to controlled fragments."""
    names = set(_query_templates("duckdb"))
    assert names == set(_query_templates("bigquery"))
    assert names == {
        "report_daily",
        "report_models",
        "report_sessions",
        "report_session_models",
    }
    for name in names:
        asts = []
        for dialect in ("duckdb", "bigquery"):
            template = query_sql(dialect, name)
            fields = {
                field
                for _, field, _, _ in Formatter().parse(template)
                if field is not None
            }
            assert fields <= {"where", "ordering", "limit"}
            sql = template.format(
                where="WHERE facts.model = :model",
                ordering="activity_day DESC NULLS LAST",
                limit="LIMIT 2",
            )
            parsed = sqlglot.parse(sql, read=dialect)
            assert len(parsed) == 1 and isinstance(parsed[0], exp.Select)
            statement = parsed[0]
            for ordered in statement.find_all(exp.Ordered):
                ordered.set("nulls_first", None)
            asts.append(statement.sql(dialect="duckdb", comments=False))
        assert asts[0] == asts[1], name
    with pytest.raises(ValueError, match="unsupported SQL dialect"):
        query_sql("unsupported", "report_daily")
    with pytest.raises(ValueError, match="not defined"):
        query_sql("duckdb", "../ddl")


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("-- no named queries", "nonempty named SQL"),
        ("-- name: empty\n", "nonempty named SQL"),
        ("-- name: ../invalid\nSELECT 1;", "invalid packaged query name"),
        ("-- name: duplicate\nSELECT 1;\n-- name: duplicate\nSELECT 2;", "duplicate"),
    ],
)
def test_named_query_lookup_rejects_malformed_assets(
    sql: str, message: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject malformed markers rather than silently choosing an ambiguous query."""
    (tmp_path / QUERY_ASSET).write_text(sql)

    def package(_name: str) -> Path:
        return tmp_path

    _query_templates.cache_clear()
    monkeypatch.setattr("usagebassoon.schema_assets.resources.files", package)
    with pytest.raises(ValueError, match=message):
        query_sql("duckdb", "duplicate")


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
        assert_report_results_match(local, remote, exhaustive=True)
    finally:
        local.close()
        remote.close()


def test_compaction_deduplicates_usage_recollections_and_preserves_history(
    collection_bundle: CollectionBundle,
) -> None:
    """Compact repeated corrections without losing gold-only or other-source keys."""
    remote = BigQueryReplayBackend()
    schema = CANONICAL_TABLE_SCHEMAS["daily_stats"]
    seeds = normalize(collection_bundle).tables["daily_stats"].slice(0, 2).to_pylist()
    original: dict[str, object] = seeds[0]
    retained: dict[str, object] = seeds[1]
    stamp = datetime(2026, 9, 10, tzinfo=UTC)
    original["collected_at"] = retained["collected_at"] = stamp
    isolated = dict(
        original,
        source_id="22222222-2222-4222-8222-222222222222",
        event_id=str(uuid4()),
    )
    initial = pa.Table.from_pylist([original, retained, isolated], schema=schema)
    try:
        remote.append("daily_stats", initial)
        remote.append("daily_stats", initial)
        remote.compact()
        remote.compact()
        assert normalized_records(remote.query("SELECT * FROM daily_stats")) == (
            normalized_records(initial)
        )

        # Simulate raw expiry: retained usage must survive in gold alone.
        remote.engine.connection.execute("DELETE FROM raw_daily_stats")
        increased = dict(
            original,
            event_id=str(uuid4()),
            collected_at=stamp + timedelta(days=1),
            input_tokens=int(seeds[0]["input_tokens"]) + 10,
            total_tokens=int(seeds[0]["total_tokens"]) + 10,
        )
        data = pa.Table.from_pylist([increased], schema=schema)
        remote.append("daily_stats", data)
        remote.append("daily_stats", data)
        remote.compact()

        # A newer downward correction wins over the earlier larger count.
        corrected = dict(
            increased,
            event_id=str(uuid4()),
            collected_at=stamp + timedelta(days=2),
            input_tokens=int(seeds[0]["input_tokens"]) + 5,
            total_tokens=int(seeds[0]["total_tokens"]) + 5,
        )
        data = pa.Table.from_pylist([corrected], schema=schema)
        remote.append("daily_stats", data)
        remote.append("daily_stats", data)
        expected = normalized_records(
            pa.Table.from_pylist([corrected, retained, isolated], schema=schema)
        )
        assert (
            normalized_records(remote.query("SELECT * FROM current_daily_stats"))
            == expected
        )
        remote.compact()
        remote.compact()
        assert normalized_records(remote.query("SELECT * FROM daily_stats")) == expected
        assert (
            normalized_records(remote.query("SELECT * FROM current_daily_stats"))
            == expected
        )
    finally:
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
        stamp = datetime.now(UTC) + timedelta(days=1)
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


@pytest.mark.parametrize("by_model", [False, True])
def test_session_report_sorting_matches_both_sql_dialects(by_model: bool) -> None:
    """Keep generated report sorting, missing rates, and limits dialect agnostic."""
    local = DuckDBBackend(":memory:")
    remote = BigQueryReplayBackend()
    try:
        local.apply_ddl()
        seed_synthetic_data(local)
        seed_synthetic_data(remote)
        for sort in SessionSort:
            left = load_session_usage(
                local, ReportFilters(), by_model=by_model, sort=sort, limit=2
            )
            right = load_session_usage(
                remote, ReportFilters(), by_model=by_model, sort=sort, limit=2
            )
            assert normalized_records(
                pa.Table.from_pylist(left), preserve_order=True
            ) == (
                normalized_records(pa.Table.from_pylist(right), preserve_order=True)
            ), sort
    finally:
        local.close()
        remote.close()


def test_source_audit_view_deduplicates_runs_and_preserves_coherent_host_evidence(
    collection_bundle: CollectionBundle,
) -> None:
    """Both native view definitions preserve ties, run counts and ledgerless sources."""
    from dataclasses import replace

    from tests._observations import observations
    from usagebassoon.audit import audit_sources

    local = DuckDBBackend(":memory:")
    remote = BigQueryReplayBackend()
    source = collection_bundle.source_id
    other = "22222222-2222-4222-8222-222222222222"
    stamp = datetime.now(UTC)
    ledger_schema = CANONICAL_TABLE_SCHEMAS["collection_ledger"]
    bundle = normalize(
        replace(collection_bundle, run_id="11111111-1111-4111-8111-111111111111")
    )
    try:
        local.apply_ddl()
        for backend in (local, remote):
            for table, data in bundle.tables.items():
                if data.num_rows:
                    backend.append(table, data)
            rows = backend.query("SELECT * FROM current_collection_ledger").to_pylist()
            for row in rows:
                row.update(started_at=stamp, finished_at=stamp, collected_at=stamp)
                row["host"] = "one-host"
                row["cpu_count"] = 2
                if row["domain"] != "collection":
                    row["host"] = "another-host"
                    row["cpu_count"] = 64
            backend.append(
                "collection_ledger", pa.Table.from_pylist(rows, schema=ledger_schema)
            )
            # Duplicate ledger publication and multiple domains still describe one run.
            backend.append(
                "collection_ledger", pa.Table.from_pylist(rows, schema=ledger_schema)
            )
            backend.append(
                "notes",
                observations(
                    pa.table(
                        {
                            "source_id": [other],
                            "client": ["codex"],
                            "session_id": ["curation-only"],
                            "note": ["no ledger"],
                            "created_at": [stamp],
                            "collected_at": [stamp],
                        }
                    )
                ),
            )
        left = local.query("SELECT * FROM audit_sources")
        right = remote.query("SELECT * FROM audit_sources")
        assert normalized_records(left) == normalized_records(right)
        summaries = {row["source_id"]: row for row in left.to_pylist()}
        assert summaries[source]["run_count"] == 1
        assert summaries[source]["host"] == "one-host"
        assert summaries[source]["cpu_count"] == 2
        assert summaries[source]["invoke_method"] == "python"
        assert summaries[other]["run_count"] == 0
        assert summaries[other]["host"] is None
        assert summaries[other]["invoke_method"] is None
        assert normalized_records(
            pa.Table.from_pylist(audit_sources(local))
        ) == normalized_records(left)
    finally:
        local.close()
        remote.close()


def test_price_resolution_preserves_observation_dates_and_per_fact_fallbacks(
    collection_bundle: CollectionBundle,
) -> None:
    """Exact, preceding, later, zero and reported costs agree across dialects."""
    normalized = normalize(collection_bundle)
    fact_template = normalized.tables["daily_stats"].to_pylist()[0]
    price_template = normalized.tables["price_versions"].to_pylist()[0]
    stamp = datetime(2026, 10, 7, tzinfo=UTC)
    cases = [
        (
            "old",
            date(2026, 1, 7),
            "model",
            0,
            0,
            10.0,
            "historical_estimate",
            date(2026, 1, 8),
        ),
        (
            "carry",
            date(2026, 1, 9),
            "model",
            0,
            0,
            10.0,
            "carried_forward",
            date(2026, 1, 8),
        ),
        (
            "exact",
            date(2026, 1, 10),
            "model",
            0,
            0,
            20.0,
            "observed",
            date(2026, 1, 10),
        ),
        (
            "incomplete",
            date(2026, 1, 10),
            "model",
            1,
            0,
            11.0,
            "carried_forward",
            date(2026, 1, 8),
        ),
        ("reported", date(2026, 1, 10), "missing", 0, 0, 7.0, "tokscale", None),
        ("free", date(2026, 1, 10), "free", 0, 0, 0.0, "observed", date(2026, 1, 10)),
        ("cache", date(2026, 1, 10), "cache", 0, 5, 7.0, "tokscale", None),
        ("unknown", date(2026, 1, 10), "missing", 0, 0, None, "unknown", None),
    ]
    facts = [
        {
            **fact_template,
            "event_id": str(uuid4()),
            "session_id": session,
            "day": day,
            "model": model,
            "input_tokens": 10,
            "output_tokens": output,
            "cache_read": cache,
            "cache_write": 0,
            "reasoning": 0,
            "total_tokens": 10 + output + cache,
            "tokscale_cost_usd": None if session == "unknown" else 7.0,
            "collected_at": stamp,
        }
        for session, day, model, output, cache, *_expected in cases
    ]
    prices = [
        {
            **price_template,
            "event_id": str(uuid4()),
            "day": day,
            "model": model,
            "price_input_per_token": rate,
            "price_output_per_token": output,
            "price_cache_read_per_token": None,
            "price_cache_write_per_token": 0.0,
            "collected_at": stamp,
        }
        for day, model, rate, output in [
            (date(2026, 1, 8), "model", 1.0, 1.0),
            (date(2026, 1, 10), "model", 2.0, None),
            (date(2026, 1, 12), "model", 3.0, 3.0),
            (date(2026, 1, 10), "free", 0.0, 0.0),
            (date(2026, 1, 10), "cache", 1.0, 1.0),
        ]
    ]
    prices.append(
        {
            **prices[0],
            "event_id": str(uuid4()),
            "source_id": "other-source",
            "model": "missing",
            "price_input_per_token": 100.0,
        }
    )
    local, remote = DuckDBBackend(":memory:"), BigQueryReplayBackend()
    try:
        local.apply_ddl()
        for backend in (local, remote):
            backend.append(
                "daily_stats",
                pa.Table.from_pylist(
                    facts, schema=CANONICAL_TABLE_SCHEMAS["daily_stats"]
                ),
            )
            backend.append(
                "price_versions",
                pa.Table.from_pylist(
                    prices, schema=CANONICAL_TABLE_SCHEMAS["price_versions"]
                ),
            )
            rows = {
                row["session_id"]: row
                for row in backend.query("SELECT * FROM daily_cost").to_pylist()
            }
            for (
                session,
                _day,
                _model,
                _output,
                _cache,
                expected,
                basis,
                price_day,
            ) in cases:
                assert rows[session]["cost_usd"] == expected
                assert rows[session]["cost_basis"] == basis
                assert rows[session]["price_day"] == price_day
            # Aggregation must retain calculated rows when another row needs fallback.
            assert (
                sum(
                    row["cost_usd"]
                    for row in rows.values()
                    if row["cost_usd"] is not None
                )
                == 65.0
            )
            unknown = load_session_usage(backend, ReportFilters(), by_model=True)
            assert (
                next(
                    row["cost_usd"] for row in unknown if row["session_id"] == "unknown"
                )
                is None
            )
            assert backend.query("SELECT cost_usd FROM report_summary").to_pylist() == [
                {"cost_usd": None}
            ]
            assert backend.query(
                "SELECT * FROM current_price_versions"
            ).num_rows == len(prices)
        for view in ("daily_cost", "report_daily_usage", "session_model_stats"):
            left, right = (
                local.query(f"SELECT * FROM {view}"),
                remote.query(f"SELECT * FROM {view}"),
            )
            assert left.column_names == right.column_names
            assert normalized_records(left) == normalized_records(right)
    finally:
        local.close()
        remote.close()


@pytest.mark.parametrize("by_model", [False, True])
def test_stale_activity_uses_usage_day_then_reported_timestamp_for_ties(
    by_model: bool,
    collection_bundle: CollectionBundle,
) -> None:
    """Backfill observation times cannot make old usage appear newly active."""
    normalized = normalize(collection_bundle)
    fact = normalized.tables["daily_stats"].to_pylist()[0]
    session = next(
        row
        for row in normalized.tables["sessions"].to_pylist()
        if all(row[key] == fact[key] for key in ("source_id", "client", "session_id"))
    )
    usage_day = date(2026, 3, 1)
    stamps = {
        "stale-old": datetime(2026, 1, 1, tzinfo=UTC),
        "stale-new": datetime(2026, 2, 1, tzinfo=UTC),
        "same-day": datetime(2026, 3, 1, 12, tzinfo=UTC),
        "later-report": datetime(2026, 3, 2, tzinfo=UTC),
    }
    observed = datetime(2026, 10, 7, tzinfo=UTC)
    local, remote = DuckDBBackend(":memory:"), BigQueryReplayBackend()
    try:
        local.apply_ddl()
        for backend in (local, remote):
            backend.append(
                "daily_stats",
                pa.Table.from_pylist(
                    [
                        {
                            **fact,
                            "event_id": str(uuid4()),
                            "session_id": key,
                            "day": usage_day,
                            "collected_at": observed,
                        }
                        for key in stamps
                    ],
                    schema=CANONICAL_TABLE_SCHEMAS["daily_stats"],
                ),
            )
            backend.append(
                "sessions",
                pa.Table.from_pylist(
                    [
                        {
                            **session,
                            "event_id": str(uuid4()),
                            "session_id": key,
                            "last_active": value,
                            "collected_at": observed,
                            "last_seen_at": observed,
                        }
                        for key, value in stamps.items()
                    ],
                    schema=CANONICAL_TABLE_SCHEMAS["sessions"],
                ),
            )
            records = load_session_usage(backend, ReportFilters(), by_model=by_model)
            assert [row["session_id"] for row in records] == [
                "later-report",
                "same-day",
                "stale-new",
                "stale-old",
            ]
            assert records[-1]["activity_day"] == usage_day
            assert records[-1]["last_active"] == stamps["stale-old"]
            assert records[-1]["last_active_stale"]
            assert not records[1]["last_active_stale"]
            assert (
                len(
                    load_session_usage(
                        backend,
                        ReportFilters(),
                        by_model=by_model,
                        since=usage_day,
                        until=usage_day,
                    )
                )
                == 3
            )
    finally:
        local.close()
        remote.close()
