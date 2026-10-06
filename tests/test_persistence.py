# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_persistence.py — Shared retry budgets, batch identity, and cleanup."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import override

import pytest

from usagebassoon.config import CollectionConfig, UsageBassoonConfig
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import NormalizedBundle, normalize
from usagebassoon.persistence import PersistSummary, persist_with_retries


def test_attempt_deadline_includes_open_and_retries_with_stable_batch(
    collection_bundle: CollectionBundle,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup and persistence share a budget; retry backoff consumes neither."""
    import usagebassoon.deadlines as deadlines

    class Config(UsageBassoonConfig):
        """Supply a controlled budget independently of provider configuration."""

        @property
        @override
        def backend_timeout_seconds(self) -> float:
            """Use one ten-second allowance per complete attempt."""
            return 10.0

    clock = [0.0]
    monkeypatch.setattr(deadlines, "monotonic", lambda: clock[0])
    config = Config(
        tmp_path / "config.toml",
        collection_bundle.source_id,
        "duckdb",
        collection=CollectionConfig(max_retries=2, retry_initial_seconds=1),
    )
    bundle = normalize(collection_bundle)
    scopes: list[deadlines.Deadline] = []
    closed: list[None] = []
    attempts = 0

    class Backend:
        """Record cleanup without opening a database."""

        def close(self) -> None:
            """Release the fake backend after a returned operation."""
            closed.append(None)

    backend = Backend()

    def open_backend(_config: UsageBassoonConfig) -> Backend:
        nonlocal attempts
        deadline = deadlines.current_deadline()
        assert deadline is not None
        scopes.append(deadline)
        attempts += 1
        clock[0] += 11 if attempts == 1 else 4
        deadline.remaining()
        return backend

    def persist(_backend: object, current: NormalizedBundle) -> PersistSummary:
        assert current is bundle
        deadline = deadlines.current_deadline()
        assert deadline is scopes[-1]
        assert deadline.remaining() == 6
        clock[0] += 7 if attempts == 2 else 2
        deadline.remaining()
        return PersistSummary(1, 0, {})

    def backoff(_delay: float) -> None:
        assert deadlines.current_deadline() is None
        clock[0] += 100

    monkeypatch.setattr("usagebassoon.persistence.open_backend", open_backend)
    monkeypatch.setattr("usagebassoon.persistence.persist_run", persist)
    monkeypatch.setattr("usagebassoon.persistence.time.sleep", backoff)
    assert persist_with_retries(config, bundle, logging.getLogger("test")).inserted == 1
    assert attempts == 3 and len(closed) == 2
    assert len({id(scope) for scope in scopes}) == 3


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
