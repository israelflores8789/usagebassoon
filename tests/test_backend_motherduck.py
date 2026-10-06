# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_motherduck.py — Local tests for native MotherDuck lifecycle policy."""

from __future__ import annotations

import time
from pathlib import Path
from threading import Event
from typing import cast

import duckdb
import pyarrow as pa
import pytest

from usagebassoon.backends.duckdb_local import _DuckDBStorage
from usagebassoon.backends.motherduck import MotherDuckBackend
from usagebassoon.deadlines import OperationTimeout
from usagebassoon.ingest import CollectionBundle


def _native_motherduck(timeout_seconds: float = 120.0) -> MotherDuckBackend:
    """Exercise MotherDuck deadline policy on a real offline native connection."""
    backend = object.__new__(MotherDuckBackend)
    _DuckDBStorage.__init__(backend, duckdb.connect(":memory:"))
    backend.database = "usagebassoon_it"
    backend.timeout_seconds = timeout_seconds
    backend._watchdog = None
    return backend


@pytest.mark.parametrize("interrupt_attach", [False, True])
def test_motherduck_startup_attaches_through_owned_native_handle(
    monkeypatch: pytest.MonkeyPatch,
    interrupt_attach: bool,
) -> None:
    """Keep startup interruptible and credentials separate from generated SQL."""
    statements: list[tuple[str, object]] = []
    signal = Event()

    class NativeHandle:
        closed = False

        def execute(self, sql: str, parameters: object = None) -> NativeHandle:
            statements.append((sql, parameters))
            if interrupt_attach and sql.startswith("ATTACH"):
                assert signal.wait(timeout=2)
                raise duckdb.InterruptException("controlled startup interruption")
            return self

        def interrupt(self) -> None:
            signal.set()

        def close(self) -> None:
            self.closed = True

    handle = NativeHandle()

    def connect(database: str) -> duckdb.DuckDBPyConnection:
        assert database == ":memory:"
        return cast(duckdb.DuckDBPyConnection, handle)

    monkeypatch.setattr(duckdb, "connect", connect)
    token = "offline'&token"
    if interrupt_attach:
        with pytest.raises(OperationTimeout):
            MotherDuckBackend("usagebassoon_it", token=token, timeout_seconds=0.05)
        assert handle.closed
    else:
        backend = MotherDuckBackend("usagebassoon_it", token=token)
        assert backend.connection is handle and backend._watchdog is None
        assert [sql for sql, _ in statements] == [
            "INSTALL motherduck",
            "LOAD motherduck",
            "SET motherduck_token = ?",
            "SET motherduck_attach_mode = 'single'",
            "ATTACH 'md:usagebassoon_it'",
            'USE "usagebassoon_it"',
        ]
        assert statements[2][1] == [token]
        assert not any(token in sql for sql, _ in statements)
        backend.close()
        assert handle.closed


@pytest.mark.parametrize("failure", ["timeout", "sql"])
def test_native_motherduck_rolls_back_and_reuses_connection(failure: str) -> None:
    """Interrupt native work and preserve ordinary error recovery on the same handle."""
    backend = _native_motherduck()
    connection = backend.connection
    try:
        backend.query("CREATE TABLE changes (value INTEGER)")
        backend.timeout_seconds = 0.1 if failure == "timeout" else 5
        expected = OperationTimeout if failure == "timeout" else duckdb.CatalogException
        started = time.monotonic()
        with pytest.raises(expected), backend.transaction():
            backend.query("INSERT INTO changes VALUES (1)")
            backend.query(
                "SELECT sum(x::DOUBLE * y::DOUBLE) "
                "FROM range(1000000) a(x), range(1000000) b(y)"
                if failure == "timeout"
                else "SELECT * FROM missing_relation"
            )
        assert time.monotonic() - started < 2
        assert backend.connection is connection and backend._watchdog is None
        assert backend.query("SELECT * FROM changes").num_rows == 0
        with backend.transaction():
            backend.query("INSERT INTO changes VALUES (42)")
        assert backend.query("SELECT * FROM changes").to_pylist() == [{"value": 42}]
    finally:
        backend.close()


def test_native_motherduck_preserves_persistence_snapshot_and_restore(
    tmp_path: Path,
    collection_bundle: CollectionBundle,
) -> None:
    """Keep native transaction and streaming semantics without a connection proxy."""
    from usagebassoon.archiver import SnapshotArchiver
    from usagebassoon.normalizer import normalize
    from usagebassoon.persistence import persist_run

    store = SnapshotArchiver(str(tmp_path / "archive"))
    bundle = normalize(collection_bundle)
    backend = _native_motherduck()
    try:
        backend.apply_ddl()
        persist_run(backend, bundle)
        assert backend.has_committed_run(bundle.run_id)
        uri = store.write(backend, run_id=bundle.run_id, manual=True)
        assert uri is not None
    finally:
        backend.close()
    destination = _native_motherduck()
    try:
        destination.apply_ddl()
        rows = store.restore(destination, uri)
        assert rows["daily_stats"] == bundle.tables["daily_stats"].num_rows
        assert destination.has_committed_run(bundle.run_id)
    finally:
        destination.close()


def test_native_motherduck_stops_expired_batch_before_commit(
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deadline between statements cannot commit facts or certify collection."""
    import usagebassoon.deadlines as deadlines
    from usagebassoon.normalizer import normalize
    from usagebassoon.persistence import persist_run

    clock = [0.0]
    monkeypatch.setattr(deadlines, "monotonic", lambda: clock[0])
    backend = _native_motherduck(10)
    original_append = backend.append
    bundle = normalize(collection_bundle)

    def append(table: str, data: pa.Table) -> None:
        original_append(table, data)
        if table == "collection_ledger":
            clock[0] = 11

    try:
        backend.apply_ddl()
        with monkeypatch.context() as patch:
            patch.setattr(backend, "append", append)
            with pytest.raises(OperationTimeout):
                persist_run(backend, bundle)
        assert not backend.has_committed_run(bundle.run_id)
        assert backend.query("SELECT * FROM daily_stats").num_rows == 0
        persist_run(backend, bundle)
        assert backend.has_committed_run(bundle.run_id)
    finally:
        backend.close()


def test_native_motherduck_lost_commit_reply_does_not_republish(
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve uncertain completion from the atomic ledger before replaying a batch."""
    from usagebassoon.normalizer import normalize
    from usagebassoon.persistence import persist_run

    backend = _native_motherduck()
    original_execute = backend._execute
    bundle = normalize(collection_bundle)

    def execute(sql: str, parameters: object = None) -> duckdb.DuckDBPyConnection:
        result = original_execute(sql, parameters)
        if sql == "COMMIT":
            raise OperationTimeout("commit acknowledgement lost")
        return result

    try:
        backend.apply_ddl()
        with monkeypatch.context() as patch:
            patch.setattr(backend, "_execute", execute)
            with pytest.raises(OperationTimeout, match="acknowledgement lost"):
                persist_run(backend, bundle)
        assert backend.has_committed_run(bundle.run_id)
        before = backend.query("SELECT * FROM collection_ledger").to_pylist()
        assert persist_run(backend, bundle).inserted == 0
        assert backend.query("SELECT * FROM collection_ledger").to_pylist() == before
        assert backend._watchdog is None
    finally:
        backend.close()


def test_motherduck_rejects_invalid_database_name() -> None:
    """Assert MotherDuck rejects empty and already-prefixed database names."""
    with pytest.raises(ValueError, match="database name"):
        MotherDuckBackend("")
    with pytest.raises(ValueError, match="database name"):
        MotherDuckBackend("md:usagebassoon")


def test_motherduck_requires_token_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assert a missing MotherDuck token fails without contacting the service."""
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="MOTHERDUCK_TOKEN"):
        MotherDuckBackend("usagebassoon")
