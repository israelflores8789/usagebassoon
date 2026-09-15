# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_merge.py — Normalization and persistence tests over the shipped DuckDB DDL."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

from tests.conftest import (
    EXPECTED_DAILY_ROWS,
    EXPECTED_DAYS,
    EXPECTED_MODELS_ENTRIES,
    EXPECTED_REPORT_ROWS,
    EXPECTED_TOKSCALE_VERSION,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.merge import persist_run
from usagebassoon.normalizer import CollectionBundle, normalize
from usagebassoon.system_metadata import SystemMetadata


def test_normalize_emits_the_current_state_ddl_columns(
    collection_bundle: CollectionBundle,
) -> None:
    """Assert normalization emits the DDL-owned freshness and audit columns."""
    normalized = normalize(collection_bundle)
    assert normalized.tables["sessions"].column_names[-3:] == [
        "first_seen_at",
        "last_seen_at",
        "last_updated_at",
    ]
    assert normalized.tables["session_model_stats"].column_names[-3:] == [
        "first_seen_at",
        "last_seen_at",
        "last_updated_at",
    ]
    assert normalized.tables["daily_stats"].column_names[-1] == "last_updated_at"
    assert normalized.tables["daily_activity"].column_names[-1] == "last_updated_at"
    assert normalized.tables["ingest_runs"].column_names[-3:] == [
        "rows_inserted",
        "rows_updated",
        "drift_events",
    ]


def test_persist_run_populates_ddl_tables(
    collection_bundle: CollectionBundle,
) -> None:
    """Assert one normalized collection writes all fact and audit tables."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        summary = persist_run(backend, normalize(collection_bundle))
        assert (summary.inserted, summary.updated) == (
            EXPECTED_REPORT_ROWS
            + EXPECTED_MODELS_ENTRIES
            + EXPECTED_DAILY_ROWS
            + EXPECTED_DAYS,
            0,
        )
        counts = {
            table: backend.query(f"SELECT count(*) AS n FROM {table}").to_pylist()[0][
                "n"
            ]
            for table in (
                "ingest_runs",
                "sessions",
                "session_model_stats",
                "daily_stats",
                "daily_activity",
                "pricing_snapshots",
                "run_metrics",
                "reconciliation_issues",
            )
        }
        assert counts == {
            "ingest_runs": 1,
            "sessions": EXPECTED_REPORT_ROWS,
            "session_model_stats": EXPECTED_MODELS_ENTRIES,
            "daily_stats": EXPECTED_DAILY_ROWS,
            "daily_activity": EXPECTED_DAYS,
            "pricing_snapshots": 1,
            "run_metrics": 1,
            "reconciliation_issues": 10,
        }
        assert backend.query(
            "SELECT tokscale_ver, status, rows_inserted, rows_updated FROM ingest_runs"
        ).to_pylist() == [
            {
                "tokscale_ver": EXPECTED_TOKSCALE_VERSION,
                "status": "partial",
                "rows_inserted": summary.inserted,
                "rows_updated": 0,
            }
        ]
    finally:
        backend.close()


def test_persist_run_records_collector_system_metadata(
    collection_bundle: CollectionBundle,
) -> None:
    """Assert normalized ingest audit rows carry collector-host metadata."""
    backend = DuckDBBackend(":memory:")
    metadata = SystemMetadata(
        os_name="Linux",
        os_version="6.16",
        architecture="x86_64",
        cpu_model="Example CPU",
        cpu_count=16,
        memory_bytes=68_719_476_736,
        shell="/bin/bash",
    )
    try:
        backend.apply_ddl()
        persist_run(
            backend,
            normalize(replace(collection_bundle, system_metadata=metadata)),
        )
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


def test_unchanged_rows_keep_their_last_updated_at(
    collection_bundle: CollectionBundle,
) -> None:
    """Assert a later unchanged collection does not refresh current-state rows."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        first = normalize(collection_bundle)
        persist_run(backend, first)
        before = backend.query(
            "SELECT min(last_updated_at) AS stamp FROM daily_activity"
        ).to_pylist()[0]["stamp"]
        later_bundle = replace(
            collection_bundle,
            run_id=str(uuid4()),
            finished_at=collection_bundle.finished_at + timedelta(minutes=1),
        )
        summary = persist_run(backend, normalize(later_bundle))
        after = backend.query(
            "SELECT min(last_updated_at) AS stamp FROM daily_activity"
        ).to_pylist()[0]["stamp"]
        assert (summary.inserted, summary.updated) == (0, 0)
        assert after == before
    finally:
        backend.close()


def test_distinct_sources_do_not_share_current_state_keys(
    collection_bundle: CollectionBundle,
) -> None:
    """Assert identical tokscale keys remain distinct across source namespaces."""
    backend = DuckDBBackend(":memory:")
    alternate_source = "22222222-2222-4222-8222-222222222222"
    try:
        backend.apply_ddl()
        first = persist_run(backend, normalize(collection_bundle))
        second = persist_run(
            backend,
            normalize(
                replace(
                    collection_bundle,
                    run_id=str(uuid4()),
                    source_id=alternate_source,
                )
            ),
        )
        assert (first.inserted, second.inserted) == (
            EXPECTED_REPORT_ROWS
            + EXPECTED_MODELS_ENTRIES
            + EXPECTED_DAILY_ROWS
            + EXPECTED_DAYS,
            EXPECTED_REPORT_ROWS
            + EXPECTED_MODELS_ENTRIES
            + EXPECTED_DAILY_ROWS
            + EXPECTED_DAYS,
        )
        assert backend.query(
            "SELECT count(DISTINCT source_id) AS sources, count(*) AS sessions "
            "FROM sessions"
        ).to_pylist() == [{"sources": 2, "sessions": EXPECTED_REPORT_ROWS * 2}]
    finally:
        backend.close()


def test_persistence_never_touches_user_curation(
    collection_bundle: CollectionBundle,
) -> None:
    """Assert ingestion leaves tags and notes owned solely by the user."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        backend.connection.execute(
            "INSERT INTO tags VALUES "
            "('session', 'source', 'codex', '', 'ses_1', 'investigate', now())"
        )
        backend.connection.execute(
            "INSERT INTO notes VALUES "
            "('source', 'codex', 'ses_1', 'spike here', now(), now())"
        )
        persist_run(backend, normalize(collection_bundle))
        assert backend.query("SELECT count(*) AS n FROM tags").to_pylist() == [{"n": 1}]
        assert backend.query("SELECT count(*) AS n FROM notes").to_pylist() == [
            {"n": 1}
        ]
    finally:
        backend.close()
