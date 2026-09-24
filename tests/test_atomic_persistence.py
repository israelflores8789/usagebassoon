# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_atomic_persistence.py — Cross-backend batch persistence invariants."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest

from usagebassoon.backends.base import CurrentStateWrite, PersistenceBatch
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.config import UsageBassoonConfig
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import NormalizedBundle, normalize
from usagebassoon.persistence import PersistSummary, persist_run, persist_with_retries


def test_replaying_a_committed_run_is_an_idempotent_no_op(
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
        assert backend.query("SELECT count(*) AS n FROM ingest_runs").to_pylist() == [
            {"n": 1}
        ]
        price_count = sum(
            len(prices) for prices in collection_bundle.pricing_by_day.values()
        )
        assert backend.query(
            "SELECT count(*) AS n FROM price_versions"
        ).to_pylist() == [{"n": price_count}]
    finally:
        backend.close()


def test_batch_rolls_back_every_write_when_a_later_append_fails() -> None:
    """Reject a partial cycle instead of committing its current-state mutation."""
    backend = DuckDBBackend(":memory:")
    run_id = str(uuid4())
    data = pa.table(
        {
            "source_id": ["source"],
            "day": [date(2026, 9, 16)],
            "intensity": [1],
            "active_time_ms": [100],
            "updated_at": [datetime(2026, 9, 16, tzinfo=UTC)],
        }
    )
    batch = PersistenceBatch(
        run_id=run_id,
        current_state=(
            CurrentStateWrite(
                "daily_activity",
                data,
                ("source_id", "day"),
                ("intensity", "active_time_ms"),
            ),
        ),
        append_only={"missing_history": pa.table({"run_id": [run_id]})},
        ingest_runs=pa.table({"run_id": [run_id]}),
    )
    try:
        backend.apply_ddl()
        with pytest.raises(Exception, match="missing_history"):
            backend.persist_batch(batch)
        assert backend.query(
            "SELECT count(*) AS n FROM daily_activity"
        ).to_pylist() == [{"n": 0}]
        assert backend.query("SELECT count(*) AS n FROM ingest_runs").to_pylist() == [
            {"n": 0}
        ]
    finally:
        backend.close()


def test_persistence_retries_one_normalized_run_without_recollection(
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
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

    class _Backend:
        """Minimal retry target that never reaches a real database."""

        def apply_ddl(self) -> None:
            """Satisfy collection setup."""
            schema_attempts.append(None)

        def close(self) -> None:
            """Satisfy collection teardown."""

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
    assert schema_attempts == [None]
