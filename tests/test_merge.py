# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_merge.py — Daily normalization, cost views, and persistence tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pyarrow as pa
import pytest

from tests.conftest import (
    EXPECTED_DAILY_STATS_ROWS,
    EXPECTED_DAYS,
    EXPECTED_REPORT_ROWS,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS, normalize
from usagebassoon.persistence import persist_run
from usagebassoon.system_metadata import SystemMetadata


def test_normalize_emits_daily_tables_and_ingest_status(
    collection_bundle: CollectionBundle,
) -> None:
    """Emit only base tables needed for daily facts and calculated views."""
    normalized = normalize(collection_bundle)
    assert "session_model_stats" not in normalized.tables
    assert normalized.tables["daily_stats"].column_names[-7:] == [
        "tokscale_cost_usd",
        "perf_duration_ms",
        "perf_timed_tokens",
        "perf_sample_count",
        "perf_token_coverage",
        "tokscale_ms_per_1k_tokens",
        "updated_at",
    ]
    assert normalized.tables["price_versions"].column_names[-2:] == [
        "observed_at",
        "updated_at",
    ]
    assert normalized.tables["ingest_status"].column_names[-4:] == [
        "last_attempted_run",
        "last_succeeded_run",
        "failure_code",
        "updated_at",
    ]
    assert normalized.tables["ingest_runs"].column_names[-3:] == [
        "rows_inserted",
        "rows_updated",
        "drift_events",
    ]


def test_normalize_preserves_canonical_nullable_types(
    collection_bundle: CollectionBundle,
) -> None:
    """Keep absent rate values typed rather than degrading to Arrow null."""
    normalized = normalize(collection_bundle)
    assert {name: table.schema for name, table in normalized.tables.items()} == {
        name: CANONICAL_TABLE_SCHEMAS[name] for name in normalized.tables
    }
    assert all(
        not pa.types.is_null(field.type)
        for table in normalized.tables.values()
        for field in table.schema
    )
    assert set(normalized.tables["price_versions"].column("model").to_pylist())


def test_current_state_freshness_uses_collection_start(
    collection_bundle: CollectionBundle,
) -> None:
    """Order overlapping runs by collection start while retaining audit times."""
    normalized = normalize(collection_bundle)
    for name in (
        "sessions",
        "daily_stats",
        "daily_activity",
        "price_versions",
        "ingest_status",
    ):
        assert set(normalized.tables[name].column("updated_at").to_pylist()) == {
            collection_bundle.started_at
        }
    assert set(
        normalized.tables["price_versions"].column("observed_at").to_pylist()
    ) == {collection_bundle.finished_at}


def test_persisted_daily_facts_drive_cost_and_all_time_aggregate(
    collection_bundle: CollectionBundle,
) -> None:
    """Calculate price-derived cost and cumulative session totals from daily rows."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        summary = persist_run(backend, normalize(collection_bundle))
        expected_prices = sum(
            len(prices) for prices in collection_bundle.pricing_by_day.values()
        )
        expected_status = len(collection_bundle.ingest_status)
        assert (summary.inserted, summary.updated) == (
            EXPECTED_REPORT_ROWS
            + EXPECTED_DAILY_STATS_ROWS
            + EXPECTED_DAYS
            + expected_prices
            + expected_status,
            0,
        )
        assert backend.query("SELECT count(*) AS n FROM daily_stats").to_pylist() == [
            {"n": EXPECTED_DAILY_STATS_ROWS}
        ]
        assert backend.query(
            "SELECT count(*) AS n FROM price_versions"
        ).to_pylist() == [{"n": expected_prices}]
        assert backend.query("SELECT count(*) AS n FROM ingest_status").to_pylist() == [
            {"n": expected_status}
        ]
        costs = backend.query(
            "SELECT count(*) AS rows, count(cost_usd) AS priced_rows FROM daily_cost"
        ).to_pylist()[0]
        assert costs == {
            "rows": EXPECTED_DAILY_STATS_ROWS,
            "priced_rows": EXPECTED_DAILY_STATS_ROWS,
        }
        aggregate = backend.query(
            "SELECT sum(total_tokens) AS total_tokens, "
            "sum(tokscale_cost_usd) AS tokscale_cost_usd, "
            "sum(cost_usd) AS cost_usd FROM session_model_stats"
        ).to_pylist()[0]
        daily = backend.query(
            "SELECT sum(total_tokens) AS total_tokens, "
            "sum(tokscale_cost_usd) AS tokscale_cost_usd, "
            "sum(cost_usd) AS cost_usd FROM daily_cost"
        ).to_pylist()[0]
        assert aggregate["total_tokens"] == daily["total_tokens"]
        assert aggregate["tokscale_cost_usd"] == pytest.approx(
            daily["tokscale_cost_usd"]
        )
        assert aggregate["cost_usd"] == pytest.approx(daily["cost_usd"])
        assert backend.query(
            "SELECT status, rows_inserted, rows_updated FROM ingest_runs"
        ).to_pylist() == [
            {"status": "ok", "rows_inserted": summary.inserted, "rows_updated": 0}
        ]
    finally:
        backend.close()


def test_reasoning_uses_the_output_price(
    collection_bundle: CollectionBundle,
) -> None:
    """Apply the output rate to reasoning tokens in the calculated daily cost."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(collection_bundle))
        row = backend.query(
            "SELECT output_tokens, reasoning, price_output_per_token, cost_usd "
            "FROM daily_cost JOIN price_versions USING (source_id, day, model) "
            "WHERE reasoning > 0 LIMIT 1"
        ).to_pylist()[0]
        assert row["cost_usd"] >= (
            (row["output_tokens"] + row["reasoning"]) * row["price_output_per_token"]
        )
    finally:
        backend.close()


def test_refreshed_daily_timing_replaces_the_prior_observation(
    collection_bundle: CollectionBundle,
) -> None:
    """Upsert the latest timing components at the existing daily natural key."""
    first = normalize(collection_bundle)
    refreshed = normalize(
        replace(
            collection_bundle,
            run_id=str(uuid4()),
            started_at=collection_bundle.started_at + timedelta(minutes=1),
            finished_at=collection_bundle.finished_at + timedelta(minutes=1),
        )
    )
    original = first.tables["daily_stats"].slice(0, 1).to_pylist()[0]
    refreshed_daily = refreshed.tables["daily_stats"]
    for name, value in (("perf_duration_ms", 1_234), ("perf_timed_tokens", 5_678)):
        index = refreshed_daily.schema.get_field_index(name)
        values = refreshed_daily.column(name).to_pylist()
        refreshed_daily = refreshed_daily.set_column(
            index,
            refreshed_daily.schema.field(index),
            pa.array(
                [value, *values[1:]], type=refreshed_daily.schema.field(index).type
            ),
        )
    refreshed = replace(
        refreshed, tables={**refreshed.tables, "daily_stats": refreshed_daily}
    )
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, first)
        persist_run(backend, refreshed)
        row = backend.query(
            "SELECT perf_duration_ms, perf_timed_tokens, ms_per_1k_tokens "
            "FROM daily_cost WHERE source_id = :source_id AND day = :day "
            "AND client = :client AND session_id = :session_id AND model = :model",
            {
                key: original[key]
                for key in ("source_id", "day", "client", "session_id", "model")
            },
        ).to_pylist()[0]
        assert row["perf_duration_ms"] == 1_234
        assert row["perf_timed_tokens"] == 5_678
        assert row["ms_per_1k_tokens"] == pytest.approx(1_000 * 1_234 / 5_678)
    finally:
        backend.close()


def test_ingest_status_is_unchanged_when_no_domains_are_refreshed(
    collection_bundle: CollectionBundle,
) -> None:
    """Skip completed historical targets while allowing explicit refreshes."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(collection_bundle))
        before = backend.query(
            "SELECT min(updated_at) AS stamp FROM ingest_status"
        ).to_pylist()[0]["stamp"]
        later = replace(
            collection_bundle,
            run_id=str(uuid4()),
            finished_at=collection_bundle.finished_at + timedelta(minutes=1),
            daily_models={},
            pricing_by_day={},
            ingest_status=(),
        )
        summary = persist_run(backend, normalize(later))
        after = backend.query(
            "SELECT min(updated_at) AS stamp FROM ingest_status"
        ).to_pylist()[0]["stamp"]
        assert (summary.inserted, summary.updated) == (0, 0)
        assert after == before
    finally:
        backend.close()


def test_older_same_source_run_cannot_regress_completed_status(
    collection_bundle: CollectionBundle,
) -> None:
    """Keep a completed target when an older overlapping run commits last."""
    status = next(
        item for item in collection_bundle.ingest_status if item.domain == "pricing"
    )
    older_run = str(uuid4())
    newer_run = str(uuid4())
    older = replace(
        collection_bundle,
        daily_models={},
        report_rows=[],
        pricing_by_day={},
        run_id=older_run,
        started_at=collection_bundle.started_at + timedelta(minutes=1),
        finished_at=collection_bundle.finished_at + timedelta(minutes=4),
        ingest_status=(
            replace(
                status,
                status="partial",
                succeeded_count=0,
                last_attempted_run=older_run,
                last_succeeded_run=None,
                failure_code="fetch",
            ),
        ),
    )
    newer = replace(
        collection_bundle,
        daily_models={},
        report_rows=[],
        pricing_by_day={},
        run_id=newer_run,
        started_at=collection_bundle.started_at + timedelta(minutes=2),
        finished_at=collection_bundle.finished_at + timedelta(minutes=3),
        ingest_status=(
            replace(
                status,
                last_attempted_run=newer_run,
                last_succeeded_run=newer_run,
            ),
        ),
    )
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(newer))
        persist_run(backend, normalize(older))
        rows = backend.query(
            "SELECT day, domain, status, last_succeeded_run FROM ingest_status"
        ).to_pylist()
        target = next(
            row
            for row in rows
            if row["day"] == status.day and row["domain"] == status.domain
        )
        assert (target["status"], target["last_succeeded_run"]) == (
            "complete",
            newer_run,
        )
    finally:
        backend.close()


def test_older_same_source_run_cannot_regress_daily_facts(
    collection_bundle: CollectionBundle,
) -> None:
    """Keep fresher token counts when an older overlapping run commits last."""
    older_run = str(uuid4())
    newer_run = str(uuid4())
    older = normalize(
        replace(
            collection_bundle,
            run_id=older_run,
            started_at=collection_bundle.started_at + timedelta(minutes=1),
            finished_at=collection_bundle.finished_at + timedelta(minutes=4),
        )
    )
    newer = normalize(
        replace(
            collection_bundle,
            run_id=newer_run,
            started_at=collection_bundle.started_at + timedelta(minutes=2),
            finished_at=collection_bundle.finished_at + timedelta(minutes=3),
        )
    )
    daily = newer.tables["daily_stats"]
    tokens = daily.column("input_tokens").to_pylist()
    assert isinstance(tokens[0], int)
    refreshed_tokens = [tokens[0] + 1, *tokens[1:]]
    index = daily.schema.get_field_index("input_tokens")
    newer_daily = daily.set_column(
        index,
        daily.schema.field(index),
        pa.array(refreshed_tokens, type=daily.schema.field(index).type),
    )
    newer = replace(newer, tables={**newer.tables, "daily_stats": newer_daily})
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, newer)
        persist_run(backend, older)
        rows = backend.query(
            "SELECT source_id, day, client, session_id, model, input_tokens "
            "FROM daily_stats"
        ).to_pylist()
        original = daily.slice(0, 1).to_pylist()[0]
        target = next(
            row
            for row in rows
            if all(
                row[key] == original[key]
                for key in ("source_id", "day", "client", "session_id", "model")
            )
        )
        assert target["input_tokens"] == refreshed_tokens[0]
    finally:
        backend.close()


def test_persist_run_records_collector_system_metadata(
    collection_bundle: CollectionBundle,
) -> None:
    """Record collector host metadata in the immutable run audit row."""
    metadata = SystemMetadata(
        os_name="Linux",
        os_version="6.16",
        architecture="x86_64",
        cpu_model="Example CPU",
        cpu_count=16,
        memory_bytes=68_719_476_736,
        shell="/bin/bash",
    )
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        normalized = normalize(replace(collection_bundle, system_metadata=metadata))
        persist_run(backend, normalized)
        assert backend.query(
            "SELECT os_name, architecture, cpu_count, memory_bytes, shell "
            "FROM ingest_runs"
        ).to_pylist() == [
            {
                "os_name": "Linux",
                "architecture": "x86_64",
                "cpu_count": 16,
                "memory_bytes": 68_719_476_736,
                "shell": "/bin/bash",
            }
        ]
    finally:
        backend.close()
