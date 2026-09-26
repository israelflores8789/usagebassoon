# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_motherduck_live.py — Opt-in MotherDuck integration tests.

Run with ``just test-md-live`` against the disposable ``usagebassoon_it`` database.
Or, run with ``USAGEBASSOON_MOTHERDUCK_LIVE=1 uv run pytest -m motherduck_live``.

The ``motherduck_live`` marker selects these tests, and
``USAGEBASSOON_MOTHERDUCK_LIVE=1`` enables access to a
preconfigured MotherDuck database called ``usagebassoon_it``.

Set ``USAGEBASSOON_MOTHERDUCK_LIVE_RESET=1`` to enable the
final snapshot/restore test, which deletes and recreates tables
in the preconfigured, disposable database.
"""

from __future__ import annotations

import logging
import os
import signal
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pyarrow as pa
import pytest
from sqlglot import exp

from tests._sql_parity import (
    assert_view_results_match,
    normalized_records,
    seed_synthetic_data,
    statements,
    view_names,
)
from tests.conftest import (
    EXPECTED_DAILY_STATS_ROWS,
    EXPECTED_DAYS,
    EXPECTED_REPORT_ROWS,
)
from usagebassoon.backends.base import (
    CurrentStateWrite,
    PersistenceBatch,
    SourceLeaseBusy,
    SourceLeaseLost,
    SourceLeaseToken,
    StorageBackend,
    is_simple_identifier,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.backends.motherduck import MotherDuckBackend
from usagebassoon.config import ConfigurationManager, UsageBassoonConfig
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import NormalizedBundle, normalize
from usagebassoon.persistence import PersistSummary, persist_run, persist_with_retries
from usagebassoon.source_leases import source_lease

pytestmark = [pytest.mark.motherduck_live, pytest.mark.usefixtures("live_settings")]

_DATABASE = "usagebassoon_it"
_LIVE_TEST_TIMEOUT_SECONDS = 300
_LOG = logging.getLogger("usagebassoon-motherduck-live")


@pytest.fixture(autouse=True)
def _bound_live_test_duration() -> Iterator[None]:
    """Fail a blocked live test within the per-test time limit."""
    if not hasattr(signal, "SIGALRM"):
        yield
        return

    def fail_timeout(_: int, __: object) -> None:
        """Raise from the main pytest thread when a live call stalls."""
        raise TimeoutError("live MotherDuck test exceeded 300 seconds")

    previous_handler = signal.signal(signal.SIGALRM, fail_timeout)
    signal.setitimer(signal.ITIMER_REAL, _LIVE_TEST_TIMEOUT_SECONDS)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


@dataclass(frozen=True, slots=True)
class LiveSettings:
    """Configuration for one isolated run in the dedicated test database."""

    source_id: str
    config_path: Path


def _require_live_access() -> None:
    """Require explicit access and reset authorization before remote writes."""
    if os.environ.get("USAGEBASSOON_MOTHERDUCK_LIVE") != "1":
        pytest.skip("set USAGEBASSOON_MOTHERDUCK_LIVE=1 for live MotherDuck tests")
    if os.environ.get("USAGEBASSOON_MOTHERDUCK_LIVE_RESET") != "1":
        pytest.fail("set USAGEBASSOON_MOTHERDUCK_LIVE_RESET=1 for schema reset")
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        pytest.fail("MOTHERDUCK_TOKEN is required for live MotherDuck tests")


def _reset_test_schema(backend: MotherDuckBackend) -> None:
    """Drop only packaged relations in the verified disposable database."""
    current = backend.query("SELECT current_database() AS name").to_pylist()
    if current != [{"name": _DATABASE}]:
        pytest.fail(f"refusing schema reset outside {_DATABASE!r}: {current!r}")

    for name in reversed(view_names()):
        if not is_simple_identifier(name):
            pytest.fail(f"invalid packaged view name {name!r}")
        backend.connection.execute(f'DROP VIEW IF EXISTS "{name}"')
    for statement in reversed(statements("duckdb", "ddl.sql")):
        if not isinstance(statement, exp.Create) or statement.kind != "TABLE":
            pytest.fail("DuckDB DDL must contain only CREATE TABLE statements")
        table = statement.this.this.name
        if not is_simple_identifier(table):
            pytest.fail(f"invalid packaged table name {table!r}")
        backend.connection.execute(f'DROP TABLE IF EXISTS "{table}"')


@pytest.fixture(scope="module")
def live_settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[LiveSettings]:
    """Reset the dedicated schema and write a token-free backend config."""
    _require_live_access()
    source_id = str(uuid4())
    config_path = tmp_path_factory.mktemp("motherduck-live") / "config.toml"
    log_directory = config_path.parent / "logs"
    config_path.write_text(
        f'source_id = "{source_id}"\n'
        'backend = "motherduck"\n'
        "[motherduck]\n"
        f'database = "{_DATABASE}"\n'
        "[collection]\n"
        "max_retries = 3\n"
        "retry_initial_seconds = 0.1\n"
        "[logging]\n"
        f'directory = "{log_directory}"\n'
    )
    backend = MotherDuckBackend(_DATABASE)
    try:
        _reset_test_schema(backend)
        backend.apply_ddl()
    finally:
        backend.close()
    yield LiveSettings(source_id, config_path)


def _backend() -> MotherDuckBackend:
    """Open a separate service-account connection to the test database."""
    return MotherDuckBackend(_DATABASE)


def _normalized_bundle(
    collection_bundle: CollectionBundle, source_id: str
) -> NormalizedBundle:
    """Give golden fixture facts a unique run and source identity."""
    return normalize(
        replace(collection_bundle, run_id=str(uuid4()), source_id=source_id)
    )


def _rows_for_source(
    backend: StorageBackend, table: str, source_id: str
) -> list[dict[str, object]]:
    """Read comparison-safe current-state rows for one source."""
    if not is_simple_identifier(table):
        raise ValueError(f"invalid table name {table!r}")
    return normalized_records(
        backend.query(
            f'SELECT * FROM "{table}" WHERE source_id = :source_id',
            {"source_id": source_id},
        )
    )


def _audit_only_batch(
    source_id: str, run_id: str, *, lease: SourceLeaseToken | None = None
) -> PersistenceBatch:
    """Build a minimal valid batch for live lease and ledger checks."""
    return PersistenceBatch(
        run_id=run_id,
        current_state=(),
        append_only={},
        ingest_runs=pa.table(
            {
                "run_id": [run_id],
                "source_id": [source_id],
                "started_at": [datetime.now(UTC)],
                "rows_inserted": [0],
                "rows_updated": [0],
            }
        ),
        lease=lease,
    )


def test_live_synthetic_views_match_duckdb() -> None:
    """Replay every shipped DuckDB view on the MotherDuck service."""
    local = DuckDBBackend(":memory:")
    remote = _backend()
    try:
        local.apply_ddl()
        seed_synthetic_data(local)
        seed_synthetic_data(remote)
        assert_view_results_match(local, remote, right_label="MotherDuck")
    finally:
        local.close()
        remote.close()


def test_live_batch_matches_duckdb_and_retries_idempotently(
    live_settings: LiveSettings, collection_bundle: CollectionBundle
) -> None:
    """Compare a full remote ingest with DuckDB and retry its run ID."""
    normalized = _normalized_bundle(collection_bundle, live_settings.source_id)
    configuration = ConfigurationManager(live_settings.config_path).load()
    local = DuckDBBackend(":memory:")
    remote = _backend()
    expected_inserted = (
        EXPECTED_REPORT_ROWS
        + EXPECTED_DAILY_STATS_ROWS
        + EXPECTED_DAYS
        + sum(len(prices) for prices in collection_bundle.pricing_by_day.values())
        + len(collection_bundle.ingest_status)
    )
    try:
        local.apply_ddl()
        expected = persist_run(local, normalized)
        actual = persist_with_retries(configuration, normalized, _LOG)
        assert actual == expected
        assert (actual.inserted, actual.updated) == (expected_inserted, 0)
        for table in (
            "sessions",
            "daily_stats",
            "daily_activity",
            "price_versions",
            "ingest_status",
        ):
            assert _rows_for_source(remote, table, live_settings.source_id) == (
                _rows_for_source(local, table, live_settings.source_id)
            ), table
        retried = persist_with_retries(configuration, normalized, _LOG)
        assert (retried.inserted, retried.updated) == (0, 0)
        assert remote.query(
            "SELECT count(*) AS n FROM ingest_runs WHERE run_id = :run_id",
            {"run_id": normalized.run_id},
        ).to_pylist() == [{"n": 1}]
    finally:
        local.close()
        remote.close()


def test_live_source_lease_context_releases_for_next_collection(
    live_settings: LiveSettings,
) -> None:
    """Hold a configured source lease through the collection context."""
    configuration = ConfigurationManager(live_settings.config_path).load()
    contender = _backend()
    try:
        with source_lease(configuration, str(uuid4()), _LOG) as active:
            active.check()
            assert (
                contender.claim_source_lease(
                    configuration.source_id, str(uuid4()), str(uuid4())
                )
                is None
            )
            other_source = str(uuid4())
            contender.ensure_source_lease(other_source)
            independent = contender.claim_source_lease(
                other_source, str(uuid4()), str(uuid4())
            )
            assert independent is not None
            contender.release_source_lease(independent)
        next_token = contender.claim_source_lease(
            configuration.source_id, str(uuid4()), str(uuid4())
        )
        assert next_token is not None
        assert next_token.fence > active.token.fence
        contender.release_source_lease(next_token)
    finally:
        contender.close()


def test_live_distinct_sources_claim_concurrently(
    live_settings: LiveSettings,
) -> None:
    """Initialize and claim independent sources from simultaneous collectors."""
    configuration = ConfigurationManager(live_settings.config_path).load()
    sources = (str(uuid4()), str(uuid4()))
    start = Barrier(2)

    def claim(source_id: str) -> SourceLeaseToken:
        """Start each configured collection after both workers are ready."""
        config = replace(configuration, source_id=source_id)
        start.wait()
        with source_lease(config, str(uuid4()), _LOG) as active:
            active.check()
            return active.token

    with ThreadPoolExecutor(max_workers=2) as executor:
        tokens = list(executor.map(claim, sources))
    assert {token.source_id for token in tokens} == set(sources)


def test_live_simultaneous_source_claims_have_one_owner() -> None:
    """Keep one remote owner when separate connections race to claim."""
    first = _backend()
    second = _backend()
    source_id = str(uuid4())
    start = Barrier(2)
    winner: SourceLeaseToken | None = None

    def claim(backend: MotherDuckBackend) -> SourceLeaseToken | None:
        """Start both claims after the two remote connections are ready."""
        start.wait()
        try:
            return backend.claim_source_lease(source_id, str(uuid4()), str(uuid4()))
        except Exception as error:
            if backend.is_retryable_error(error):
                return None
            raise

    try:
        first.ensure_source_lease(source_id)
        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(claim, (first, second)))
        owned = [token for token in claims if token is not None]
        assert len(owned) == 1
        winner = owned[0]
        assert winner.fence == 1
        assert first.query(
            "SELECT fence, owner_id FROM source_leases WHERE source_id = :source_id",
            {"source_id": source_id},
        ).to_pylist() == [{"fence": winner.fence, "owner_id": winner.owner_id}]
    finally:
        if winner is not None:
            first.release_source_lease(winner)
        first.close()
        second.close()


def test_live_source_lease_takeover_fences_stale_batch() -> None:
    """Reject an expired owner's transaction after another owner takes over."""
    first = _backend()
    second = _backend()
    source_id = str(uuid4())
    run_id = str(uuid4())
    stale: SourceLeaseToken | None = None
    successor: SourceLeaseToken | None = None
    try:
        first.ensure_source_lease(source_id)
        stale = first.claim_source_lease(source_id, run_id, str(uuid4()))
        assert stale is not None
        assert first.renew_source_lease(stale)
        first.connection.execute(
            "UPDATE source_leases SET lease_expires_at = "
            "CURRENT_TIMESTAMP - INTERVAL '1 second' "
            "WHERE source_id = ? AND fence = ?",
            [source_id, stale.fence],
        )
        successor = second.claim_source_lease(source_id, str(uuid4()), str(uuid4()))
        assert successor is not None
        assert successor.fence == stale.fence + 1
        assert not first.renew_source_lease(stale)
        first.release_source_lease(stale)
        assert second.renew_source_lease(successor)
        with pytest.raises(SourceLeaseLost):
            first.persist_batch(_audit_only_batch(source_id, run_id, lease=stale))
        assert first.query(
            "SELECT count(*) AS n FROM ingest_runs WHERE run_id = :run_id",
            {"run_id": run_id},
        ).to_pylist() == [{"n": 0}]
    finally:
        if stale is not None:
            first.release_source_lease(stale)
        if successor is not None:
            second.release_source_lease(successor)
        first.close()
        second.close()


def test_live_same_run_lease_and_ledger_prevent_duplicate_audit_rows() -> None:
    """Block a competing write and make a later retry a no-op."""
    first = _backend()
    second = _backend()
    source_id = str(uuid4())
    run_id = str(uuid4())
    token: SourceLeaseToken | None = None
    try:
        first.ensure_source_lease(source_id)
        token = first.claim_source_lease(source_id, run_id, str(uuid4()))
        assert token is not None
        batch = _audit_only_batch(source_id, run_id)
        with pytest.raises(SourceLeaseBusy):
            second.persist_batch(batch)
        committed = first.persist_batch(replace(batch, lease=token))
        assert not committed.already_committed
        first.release_source_lease(token)
        retried = second.persist_batch(batch)
        assert retried.already_committed
        assert second.query(
            "SELECT count(*) AS n FROM ingest_runs WHERE run_id = :run_id",
            {"run_id": run_id},
        ).to_pylist() == [{"n": 1}]
    finally:
        if token is not None:
            first.release_source_lease(token)
        first.close()
        second.close()


def test_live_concurrent_sources_persist_independently(
    live_settings: LiveSettings, collection_bundle: CollectionBundle
) -> None:
    """Commit two source namespaces through concurrent remote transactions."""
    first_source = str(uuid4())
    second_source = str(uuid4())
    first = _normalized_bundle(collection_bundle, first_source)
    second = _normalized_bundle(collection_bundle, second_source)
    configuration = ConfigurationManager(live_settings.config_path).load()
    configs: tuple[UsageBassoonConfig, UsageBassoonConfig] = (
        replace(configuration, source_id=first_source),
        replace(configuration, source_id=second_source),
    )

    def persist(item: tuple[UsageBassoonConfig, NormalizedBundle]) -> PersistSummary:
        """Persist one source using its own configured identity."""
        return persist_with_retries(item[0], item[1], _LOG)

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(
            executor.map(
                persist,
                zip(configs, (first, second), strict=True),
            )
        )
    assert all(outcome.inserted > 0 for outcome in outcomes)
    backend = _backend()
    try:
        assert backend.query(
            "SELECT count(*) AS n FROM ingest_runs "
            "WHERE run_id IN (:first_run, :second_run)",
            {"first_run": first.run_id, "second_run": second.run_id},
        ).to_pylist() == [{"n": 2}]
        assert backend.query(
            "SELECT count(DISTINCT source_id) AS n FROM sessions "
            "WHERE source_id IN (:first_source, :second_source)",
            {"first_source": first_source, "second_source": second_source},
        ).to_pylist() == [{"n": 2}]
    finally:
        backend.close()


def test_live_batch_rolls_back_after_late_failure() -> None:
    """Rollback fact writes when a later append target is missing."""
    backend = _backend()
    source_id = str(uuid4())
    run_id = str(uuid4())
    current = pa.table(
        {
            "source_id": [source_id],
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
                current,
                ("source_id", "day"),
                ("intensity", "active_time_ms"),
            ),
        ),
        append_only={"missing_history": pa.table({"run_id": [run_id]})},
        ingest_runs=_audit_only_batch(source_id, run_id).ingest_runs,
    )
    try:
        with pytest.raises(Exception, match="missing_history"):
            backend.persist_batch(batch)
        assert backend.query(
            "SELECT count(*) AS n FROM daily_activity WHERE source_id = :source_id",
            {"source_id": source_id},
        ).to_pylist() == [{"n": 0}]
        assert backend.query(
            "SELECT count(*) AS n FROM ingest_runs WHERE run_id = :run_id",
            {"run_id": run_id},
        ).to_pylist() == [{"n": 0}]
    finally:
        backend.close()


def test_live_snapshot_reads_one_transaction_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exclude a note committed between two remote snapshot table reads."""
    reader = _backend()
    writer = _backend()
    source_id = str(uuid4())
    original_query = reader.query
    reads = 0

    def read_then_write(
        sql: str, parameters: Mapping[str, str] | None = None
    ) -> pa.Table:
        """Commit a note after the first table read has materialized."""
        nonlocal reads
        result = original_query(sql, parameters)
        reads += 1
        if reads == 1:
            stamp = datetime.now(UTC)
            writer.append(
                "notes",
                pa.table(
                    {
                        "source_id": [source_id],
                        "client": ["codex"],
                        "session_id": ["between-reads"],
                        "note": ["committed after capture"],
                        "created_at": [stamp],
                        "updated_at": [stamp],
                    }
                ),
            )
        return result

    monkeypatch.setattr(reader, "query", read_then_write)
    try:
        snapshot = reader.read_snapshot_tables(("sessions", "notes"))
        assert snapshot.captured_at.tzinfo is not None
        assert reads == 2
        assert all(
            row["source_id"] != source_id
            for row in snapshot.tables["notes"].to_pylist()
        )
        assert writer.query(
            "SELECT count(*) AS n FROM notes WHERE source_id = :source_id",
            {"source_id": source_id},
        ).to_pylist() == [{"n": 1}]
    finally:
        reader.close()
        writer.close()
