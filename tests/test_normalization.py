# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_normalization.py — Daily normalization, cost views, and persistence tests."""

from __future__ import annotations

import logging
import subprocess
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest

from tests.conftest import (
    EXPECTED_DAILY_STATS_ROWS,
    EXPECTED_REPORT_ROWS,
    EXPECTED_TOTAL_CACHE_READ,
    EXPECTED_TOTAL_CACHE_WRITE,
    EXPECTED_TOTAL_COST,
    EXPECTED_TOTAL_INPUT,
    EXPECTED_TOTAL_MESSAGES,
    EXPECTED_TOTAL_OUTPUT,
    EXPECTED_TOTAL_REASONING,
)
from usagebassoon import system_metadata
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS, normalize
from usagebassoon.persistence import persist_run
from usagebassoon.system_metadata import InvokeMethod, SystemMetadata


def test_normalize_emits_daily_tables_and_collection_status(
    collection_bundle: CollectionBundle,
) -> None:
    normalized = normalize(collection_bundle)
    assert set(normalized.tables) == {
        "sessions",
        "daily_stats",
        "price_versions",
        "collection_ledger",
    }
    ledger = normalized.tables["collection_ledger"]
    assert ledger.num_rows == 1 + len(collection_bundle.ingest_status)
    assert ledger.column("domain").to_pylist().count("collection") == 1
    assert ledger.schema == CANONICAL_TABLE_SCHEMAS["collection_ledger"]
    assert all(
        table.column("event_id").null_count == 0 for table in normalized.tables.values()
    )


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
    missing_write_rates = {
        day: {
            model: price.model_copy(
                update={
                    "pricing": price.pricing.model_copy(
                        update={"cache_write_input_token_cost": None}
                    )
                }
            )
            for model, price in prices.items()
        }
        for day, prices in collection_bundle.pricing_by_day.items()
    }
    prices = normalize(
        replace(collection_bundle, pricing_by_day=missing_write_rates)
    ).tables["price_versions"]
    assert prices.column("price_cache_write_per_token").null_count == prices.num_rows


def test_current_state_freshness_uses_collection_start(
    collection_bundle: CollectionBundle,
) -> None:
    normalized = normalize(collection_bundle)
    for name in ("sessions", "daily_stats", "price_versions", "collection_ledger"):
        assert set(normalized.tables[name].column("collected_at").to_pylist()) == {
            collection_bundle.started_at
        }


@pytest.mark.parametrize("activity_offset_days", [-30, 0, 30, None])
def test_session_visibility_uses_completion_independently_of_activity(
    collection_bundle: CollectionBundle, activity_offset_days: int | None
) -> None:
    """Preserve reported activity while recording when metadata was observed."""
    finished_at = collection_bundle.started_at + timedelta(minutes=10)
    last_active = (
        finished_at + timedelta(days=activity_offset_days)
        if activity_offset_days is not None
        else None
    )
    session = collection_bundle.report_rows[0].model_copy(
        update={"last_active": last_active}
    )
    bundle = replace(collection_bundle, finished_at=finished_at, report_rows=[session])
    row = normalize(bundle).tables["sessions"].to_pylist()[0]
    assert row["first_seen_at"] == row["last_seen_at"] == finished_at
    assert row["last_active"] == last_active
    assert row["collected_at"] == bundle.started_at


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
            EXPECTED_REPORT_ROWS + EXPECTED_DAILY_STATS_ROWS + expected_prices,
            0,
        )
        assert backend.query("SELECT count(*) AS n FROM daily_stats").to_pylist() == [
            {"n": EXPECTED_DAILY_STATS_ROWS}
        ]
        assert backend.query(
            "SELECT count(*) AS n FROM price_versions"
        ).to_pylist() == [{"n": expected_prices}]
        assert backend.query(
            "SELECT count(*) AS n FROM collection_status"
        ).to_pylist() == [{"n": expected_status}]
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
        components = backend.query(
            "SELECT SUM(input_tokens) AS input, SUM(output_tokens) AS output, "
            "SUM(cache_read) AS cache_read, SUM(cache_write) AS cache_write, "
            "SUM(reasoning) AS reasoning, SUM(message_count) AS messages "
            "FROM daily_stats"
        ).to_pylist()[0]
        assert components == {
            "input": EXPECTED_TOTAL_INPUT,
            "output": EXPECTED_TOTAL_OUTPUT,
            "cache_read": EXPECTED_TOTAL_CACHE_READ,
            "cache_write": EXPECTED_TOTAL_CACHE_WRITE,
            "reasoning": EXPECTED_TOTAL_REASONING,
            "messages": EXPECTED_TOTAL_MESSAGES,
        }
        assert daily["total_tokens"] == (
            EXPECTED_TOTAL_INPUT
            + EXPECTED_TOTAL_OUTPUT
            + EXPECTED_TOTAL_CACHE_READ
            + EXPECTED_TOTAL_CACHE_WRITE
            + EXPECTED_TOTAL_REASONING
        )
        assert daily["tokscale_cost_usd"] == pytest.approx(EXPECTED_TOTAL_COST)
        assert aggregate["total_tokens"] == daily["total_tokens"]
        assert aggregate["tokscale_cost_usd"] == pytest.approx(
            daily["tokscale_cost_usd"]
        )
        assert aggregate["cost_usd"] == pytest.approx(daily["cost_usd"])
        assert backend.query("SELECT status FROM collection_runs").to_pylist() == [
            {"status": "ok"}
        ]
    finally:
        backend.close()


def test_linux_cpu_model_reads_the_native_description(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read the Linux CPU brand rather than its numeric processor index."""
    cpuinfo = tmp_path / "cpuinfo"
    cpuinfo.write_text("processor : 0\nmodel name : Example CPU\n")

    def cpuinfo_path(_name: str) -> Path:
        return cpuinfo

    monkeypatch.setattr(system_metadata, "Path", cpuinfo_path)
    assert system_metadata._linux_cpu_model() == "Example CPU"


@pytest.mark.parametrize(
    ("system", "executable", "timeout"),
    [("Darwin", "/usr/sbin/sysctl", 2), ("Windows", "powershell.exe", 5)],
)
def test_native_cpu_queries_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
    system: str,
    executable: str,
    timeout: int,
) -> None:
    """Query macOS and Windows CPU descriptions with finite process deadlines."""

    def query(
        command: list[str], **options: object
    ) -> subprocess.CompletedProcess[str]:
        assert command[0] == executable
        assert options["timeout"] == timeout
        assert options["check"] is True
        assert options["stdin"] == subprocess.DEVNULL
        if system == "Darwin":
            assert command[1:] == ["-n", "machdep.cpu.brand_string"]
        else:
            assert "Win32_Processor" in command[-1]
            assert "-NoProfile" in command
        return subprocess.CompletedProcess(command, 0, stdout=" Example CPU\n")

    monkeypatch.setattr(system_metadata.subprocess, "run", query)
    assert system_metadata._cpu_model(system) == "Example CPU"


def test_cpu_probe_failure_preserves_metadata(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A CPU query failure preserves the other available host attributes."""

    def fail() -> str | None:
        raise PermissionError("CPU metadata unavailable")

    monkeypatch.setattr(system_metadata.platform, "system", lambda: "Linux")
    monkeypatch.setattr(system_metadata, "_linux_cpu_model", fail)
    monkeypatch.setattr(system_metadata.platform, "processor", lambda: "Fallback CPU")
    monkeypatch.setattr(system_metadata.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(system_metadata, "_physical_memory_bytes", lambda: 1024)
    with caplog.at_level(logging.ERROR, logger="usagebassoon"):
        metadata = system_metadata.capture_system_metadata()
    assert metadata.cpu_model == "Fallback CPU"
    assert metadata.os_name == "Linux"
    assert metadata.cpu_count == 4 and metadata.memory_bytes == 1024
    assert any(record.exc_info for record in caplog.records)
    assert "platform CPU model capture failed" in caplog.text


def test_collection_run_view_omits_target_counts(
    collection_bundle: CollectionBundle,
) -> None:
    """Run summaries omit mixed target counts while preflight retains their evidence."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(collection_bundle))
        runs = backend.query("SELECT * FROM collection_runs")
        assert {"expected_count", "succeeded_count"}.isdisjoint(runs.column_names)
        statuses = backend.query(
            "SELECT expected_count, succeeded_count FROM collection_status"
        )
        assert statuses.num_rows == len(collection_bundle.ingest_status)
        assert all(
            row["expected_count"] == row["succeeded_count"]
            for row in statuses.to_pylist()
        )
    finally:
        backend.close()


def test_reasoning_uses_the_output_price() -> None:
    """Price every token component exactly, with reasoning at the output rate."""
    from datetime import UTC, date, datetime

    from tests._observations import observations

    stamp = datetime(2026, 9, 10, tzinfo=UTC)
    identity = {"source_id": "source", "day": date(2026, 9, 10), "model": "model"}
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        backend.append(
            "daily_stats",
            observations(
                pa.Table.from_pylist(
                    [
                        {
                            **identity,
                            "client": "codex",
                            "session_id": "session",
                            "input_tokens": 2,
                            "output_tokens": 3,
                            "reasoning": 5,
                            "cache_read": 7,
                            "cache_write": 11,
                            "total_tokens": 28,
                            "collected_at": stamp,
                        }
                    ],
                    schema=CANONICAL_TABLE_SCHEMAS["daily_stats"],
                )
            ),
        )
        backend.append(
            "price_versions",
            observations(
                pa.Table.from_pylist(
                    [
                        {
                            **identity,
                            "source": "synthetic",
                            "price_input_per_token": 0.01,
                            "price_output_per_token": 0.02,
                            "price_cache_read_per_token": 0.03,
                            "price_cache_write_per_token": 0.04,
                            "collected_at": stamp,
                        }
                    ],
                    schema=CANONICAL_TABLE_SCHEMAS["price_versions"],
                )
            ),
        )
        assert backend.query("SELECT cost_usd FROM daily_cost").to_pylist() == [
            {"cost_usd": pytest.approx(0.83)}
        ]
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


def test_collection_status_is_unchanged_when_no_domains_are_refreshed(
    collection_bundle: CollectionBundle,
) -> None:
    """Skip completed historical targets while allowing explicit refreshes."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(collection_bundle))
        before = backend.query(
            "SELECT * FROM collection_status ORDER BY source_id, day, domain"
        ).to_pylist()
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
            "SELECT * FROM collection_status ORDER BY source_id, day, domain"
        ).to_pylist()
        assert summary.inserted == 0
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
                run_id=older_run,
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
                run_id=newer_run,
            ),
        ),
    )
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(newer))
        persist_run(backend, normalize(older))
        rows = backend.query(
            "SELECT day, domain, status, run_id FROM collection_status"
        ).to_pylist()
        target = next(
            row
            for row in rows
            if row["day"] == status.day and row["domain"] == status.domain
        )
        assert (target["status"], target["run_id"]) == (
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


@pytest.mark.parametrize("invoke_method", list(InvokeMethod))
def test_persist_run_records_collector_system_metadata(
    collection_bundle: CollectionBundle,
    invoke_method: InvokeMethod,
) -> None:
    """Record host metadata and the explicit invocation in collection audit views."""
    metadata = SystemMetadata(
        os_name="Linux",
        os_version="6.16",
        architecture="x86_64",
        cpu_model="Example CPU",
        cpu_count=16,
        memory_bytes=68_719_476_736,
    )
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        normalized = normalize(
            replace(
                collection_bundle,
                system_metadata=metadata,
                invoke_method=invoke_method,
            )
        )
        ledger = normalized.tables["collection_ledger"]
        assert set(ledger.column("invoke_method").to_pylist()) == {invoke_method.value}
        assert "shell" not in ledger.column_names
        persist_run(backend, normalized)
        assert backend.query(
            "SELECT os_name, architecture, cpu_count, memory_bytes, invoke_method "
            "FROM collection_runs"
        ).to_pylist() == [
            {
                "os_name": "Linux",
                "architecture": "x86_64",
                "cpu_count": 16,
                "memory_bytes": 68_719_476_736,
                "invoke_method": invoke_method.value,
            }
        ]
    finally:
        backend.close()
