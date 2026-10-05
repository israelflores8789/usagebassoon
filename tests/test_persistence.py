# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_persistence.py — Transactional writes, append publication, and retries."""

from __future__ import annotations

import logging
from pathlib import Path
from threading import Lock

import duckdb
import pyarrow as pa
import pytest
from google.api_core.exceptions import ServiceUnavailable

from tests._bigquery_replay import BigQueryReplayBackend
from usagebassoon.backends.base import CurrentStateWrite, PersistenceBatch
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.config import CollectionConfig, UsageBassoonConfig
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import NormalizedBundle, normalize
from usagebassoon.persistence import PersistSummary, persist_run, persist_with_retries


def test_duckdb_replaying_a_committed_run_is_an_idempotent_no_op(
    collection_bundle: CollectionBundle,
) -> None:
    """Keep current and append-only facts unchanged when a run is retried."""
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        normalized = normalize(collection_bundle)
        first = persist_run(backend, normalized)
        retry = persist_run(backend, normalized)
        assert first.inserted > 0
        assert (retry.inserted, retry.updated, retry.per_table) == (0, 0, {})
        assert backend.query(
            "SELECT count(*) AS n FROM collection_runs"
        ).to_pylist() == [{"n": 1}]
        price_count = sum(
            len(prices) for prices in collection_bundle.pricing_by_day.values()
        )
        assert backend.query(
            "SELECT count(*) AS n FROM price_versions"
        ).to_pylist() == [{"n": price_count}]
    finally:
        backend.close()


def test_duckdb_batch_rolls_back_every_write_when_a_later_append_fails(
    collection_bundle: CollectionBundle,
) -> None:
    bundle = normalize(collection_bundle)
    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()
    batch = PersistenceBatch(
        run_id=bundle.run_id,
        current_state=(
            CurrentStateWrite(
                "daily_stats",
                bundle.tables["daily_stats"],
                ("source_id", "day", "client", "session_id", "model"),
                ("total_tokens",),
            ),
        ),
        append_only={"missing_history": bundle.tables["collection_ledger"]},
        collection_ledger=bundle.tables["collection_ledger"],
    )
    try:
        with pytest.raises(duckdb.CatalogException, match="missing_history"):
            backend.persist_batch(batch)
        assert backend.query("SELECT * FROM daily_stats").num_rows == 0
        assert backend.query("SELECT * FROM collection_ledger").num_rows == 0
    finally:
        backend.close()


def test_persistence_retries_one_normalized_run_without_recollection(
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Reuse one run UUID and Arrow bundle until persistence succeeds."""
    config = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=collection_bundle.source_id,
        backend="duckdb",
        local_database=Path(":memory:"),
    )
    attempts: list[str] = []
    schema_attempts: list[None] = []
    published: list[NormalizedBundle] = []
    closed: list[None] = []

    class _Backend:
        """Minimal retry target that never reaches a real database."""

        def apply_ddl(self) -> None:
            """Satisfy collection setup."""
            schema_attempts.append(None)

        def close(self) -> None:
            """Satisfy collection teardown."""
            closed.append(None)

        def is_retryable_error(self, _: Exception) -> bool:
            """Classify the controlled first failure as a transaction conflict."""
            return True

    def open_backend(_: UsageBassoonConfig) -> object:
        """Return the controlled retry target instead of a real backend."""
        return _Backend()

    monkeypatch.setattr("usagebassoon.persistence.open_backend", open_backend)

    def persist(_: object, bundle: NormalizedBundle) -> PersistSummary:
        """Fail once, then record the same normalized bundle run identity."""
        run_id = bundle.run_id
        attempts.append(run_id)
        published.append(bundle)
        if len(attempts) == 1:
            raise RuntimeError("transient warehouse error")
        return PersistSummary(inserted=1, updated=0, per_table={})

    monkeypatch.setattr("usagebassoon.persistence.persist_run", persist)

    def sleep(_: float) -> None:
        """Avoid a real retry delay in this deterministic unit test."""

    def uniform(_: float, maximum: float) -> float:
        """Use the upper backoff bound in this deterministic unit test."""
        return maximum

    monkeypatch.setattr("usagebassoon.persistence.time.sleep", sleep)
    monkeypatch.setattr("usagebassoon.persistence.random.uniform", uniform)
    normalized = normalize(collection_bundle)
    result = persist_with_retries(
        config,
        normalized,
        logging.getLogger("usagebassoon-test"),
    )
    assert result.inserted == 1
    assert attempts == [normalized.run_id, normalized.run_id]
    assert all(bundle is normalized for bundle in published)
    assert len(closed) == 2
    assert schema_attempts == []
    retry = next(
        record
        for record in caplog.records
        if "retryable backend error" in record.getMessage()
    )
    assert retry.exc_info is not None
    assert "transient warehouse error" in caplog.text


@pytest.mark.parametrize(
    ("failure_mode", "max_retries", "expected_attempts"),
    [
        ("retryable", 3, 4),
        ("retryable", 0, 1),
        ("permanent", 3, 1),
        ("classification", 3, 1),
        ("open", 3, 1),
    ],
)
def test_persistence_retry_limits_preserve_failure_and_close_each_backend(
    collection_bundle: CollectionBundle,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure_mode: str,
    max_retries: int,
    expected_attempts: int,
) -> None:
    """Bound retries, stop permanent failures, and retain the original exception."""
    failure = RuntimeError("primary publication failure")
    bundle = normalize(collection_bundle)
    config = UsageBassoonConfig(
        tmp_path / "config.toml",
        collection_bundle.source_id,
        "duckdb",
        collection=CollectionConfig(max_retries=max_retries, retry_initial_seconds=1),
    )
    opened: list[None] = []
    closed: list[None] = []
    published: list[NormalizedBundle] = []
    delays: list[float] = []

    class Backend:
        """Classify controlled failures and expose resource cleanup."""

        def is_retryable_error(self, error: Exception) -> bool:
            assert error is failure
            if failure_mode == "classification":
                raise ValueError("classification unavailable")
            return failure_mode == "retryable"

        def close(self) -> None:
            closed.append(None)

    def open_backend(_config: UsageBassoonConfig) -> Backend:
        opened.append(None)
        if failure_mode == "open":
            raise failure
        return Backend()

    def persist(_backend: object, current: NormalizedBundle) -> PersistSummary:
        published.append(current)
        raise failure

    def uniform(_low: float, high: float) -> float:
        return high

    monkeypatch.setattr("usagebassoon.persistence.open_backend", open_backend)
    monkeypatch.setattr("usagebassoon.persistence.persist_run", persist)
    monkeypatch.setattr("usagebassoon.persistence.time.sleep", delays.append)
    monkeypatch.setattr("usagebassoon.persistence.random.uniform", uniform)
    with pytest.raises(RuntimeError) as caught:
        persist_with_retries(config, bundle, logging.getLogger("usagebassoon-test"))
    assert caught.value is failure
    assert len(opened) == expected_attempts
    assert len(closed) == (0 if failure_mode == "open" else expected_attempts)
    assert len(published) == len(closed)
    assert all(current is bundle for current in published)
    assert delays == ([1.0, 2.0, 4.0] if expected_attempts == 4 else [])
    assert any(record.exc_info is not None for record in caplog.records)
    if failure_mode == "classification":
        assert "classification unavailable" in caplog.text


def test_bigquery_partial_publication_never_marks_missing_facts_complete(
    collection_bundle: CollectionBundle, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fact and ledger failures leave useful data visible and safe to recollect."""
    backend = BigQueryReplayBackend()
    bundle = normalize(collection_bundle)
    original_append = backend.append
    lock = Lock()
    failed_table = "daily_stats"

    def append(table: str, data: pa.Table) -> None:
        """Serialize the local replay engine while preserving independent loads."""
        with lock:
            if table == failed_table:
                raise ServiceUnavailable("controlled publication failure")
            original_append(table, data)

    monkeypatch.setattr(backend, "append", append)
    try:
        with pytest.raises(ServiceUnavailable):
            persist_run(backend, bundle)
        assert backend.query("SELECT * FROM current_sessions").num_rows > 0
        assert backend.query("SELECT * FROM current_daily_stats").num_rows == 0
        assert backend.query("SELECT * FROM collection_status").num_rows == 0
        failed_table = "collection_ledger"
        with pytest.raises(ServiceUnavailable):
            persist_run(backend, bundle)
        assert backend.query("SELECT * FROM current_daily_stats").num_rows > 0
        assert backend.query("SELECT * FROM collection_status").num_rows == 0
        failed_table = ""
        persist_run(backend, bundle)
        assert backend.query("SELECT * FROM collection_runs").num_rows == 1
        assert backend.query("SELECT * FROM collection_status").num_rows > 0
        for table in ("sessions", "daily_stats", "price_versions"):
            assert backend.query(f"SELECT * FROM current_{table}").num_rows == (
                bundle.tables[table].num_rows
            )
    finally:
        backend.close()
