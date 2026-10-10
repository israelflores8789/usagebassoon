# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_motherduck.py — Native storage and supervised proxy lifecycle tests."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path
from threading import Event
from typing import cast, override

import duckdb
import pyarrow as pa
import pytest

from usagebassoon.backends.base import (
    BatchPersistResult,
    PersistenceBatch,
    UpsertResult,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend, _DuckDBStorage
from usagebassoon.backends.motherduck import (
    MotherDuckBackend,
    _NativeMotherDuck,
    _NativeResource,
)
from usagebassoon.config import MotherDuckConfig, UsageBassoonConfig
from usagebassoon.deadlines import OperationTimeout, ResourceUnavailable
from usagebassoon.ingest import CollectionBundle


class _UnresponsiveBackend(_NativeMotherDuck):
    """Use real native transactions with one deliberately uncancellable wait."""

    def __init__(
        self, database: Path, phase: str, marker: Path, records: Path | None = None
    ) -> None:
        """Recreate the owned native handle inside the supervised interpreter."""
        self.phase = phase
        self.marker = marker
        self.records = records
        if phase == "open":
            self._block_once()
        _DuckDBStorage.__init__(self, duckdb.connect(str(database)))
        self.database = "usagebassoon_it"
        self.timeout_seconds = 120.0
        self._watchdog = None

    def _block_once(self) -> None:
        """Simulate a native driver that ignores cooperative cancellation."""
        if not self.marker.exists():
            self.marker.write_text(str(os.getpid()))
            time.sleep(120)

    @override
    def _execute(
        self, sql: str, parameters: object = None
    ) -> duckdb.DuckDBPyConnection:
        if self.phase == "steps" and (
            sql.startswith("INSERT INTO changes") or '"current_' in sql
        ):
            time.sleep(0.15)
        if (
            self.phase == "exit"
            and sql == "SELECT 42 AS value"
            and not self.marker.exists()
        ):
            self.marker.write_text(str(os.getpid()))
            os._exit(71)
        if (
            (self.phase == "query" and sql == "SELECT 42 AS value")
            or (self.phase == "capture" and '"current_sessions"' in sql)
            or (self.phase == "restore" and sql == "COMMIT")
        ):
            self._block_once()
        result = super()._execute(sql, parameters)
        if self.phase in {"restore_commit", "commit"} and sql == "COMMIT":
            self._block_once()
        return result

    @override
    def persist_batch(self, batch: PersistenceBatch) -> BatchPersistResult:
        """Record complete batch identities and inject a native transaction conflict."""
        if self.records is not None:
            events = {
                write.table: write.data["event_id"].to_pylist()
                for write in batch.current_state
            }
            events.update(
                {
                    table: data["event_id"].to_pylist()
                    for table, data in batch.append_only.items()
                }
            )
            events["collection_ledger"] = batch.collection_ledger[
                "event_id"
            ].to_pylist()
            with self.records.open("a") as stream:
                stream.write(
                    json.dumps({"run_id": batch.run_id, "events": events}) + "\n"
                )
        if self.phase == "conflict" and not self.marker.exists():
            self.marker.touch()
            raise duckdb.TransactionException("Conflict on controlled publication")
        return super().persist_batch(batch)

    @override
    def upsert(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> UpsertResult:
        """Hang after a real state write, leaving the transaction uncommitted."""
        result = super().upsert(table, data, natural_keys, change_fields)
        if self.phase == "write":
            self._block_once()
        return result

    @override
    def close(self) -> None:
        """Hang during teardown even after a successful commit or query."""
        if self.phase == "close":
            self._block_once()
        super().close()


def _test_resource(
    database: Path, phase: str, marker: Path, records: Path | None
) -> _NativeResource:
    """Build the controlled resource exclusively inside the child."""
    return _NativeResource(_UnresponsiveBackend(database, phase, marker, records))


def _install_factory(
    monkeypatch: pytest.MonkeyPatch,
    database: Path,
    phase: str,
    marker: Path,
    records: Path | None = None,
) -> None:
    """Keep the public adapter and supervisor real with a controlled resource."""

    def factory(
        _database: str, _token: str, _timeout: float
    ) -> Callable[[], _NativeResource]:
        return partial(_test_resource, database, phase, marker, records)

    monkeypatch.setattr("usagebassoon.backends.motherduck._resource_factory", factory)
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "offline-token")


def _seed(database: Path) -> None:
    """Prepare a disposable native database without adding collection DDL."""
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.close()


def _blocked_interrupt(resource: _NativeResource) -> None:
    """Record a cancellation request that fails to return on the native side."""
    assert isinstance(resource.backend, _UnresponsiveBackend)
    resource.backend.marker.with_suffix(".interrupt").write_text(str(os.getpid()))
    time.sleep(120)


@pytest.mark.parametrize("phase", ["open", "query", "close", "exit", "interrupt"])
def test_proxy_kills_stalled_resource_and_invalidates_old_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """Bound startup, execution, teardown, and unexpected exit on the public adapter."""
    import usagebassoon.deadlines as deadlines

    database = tmp_path / "native.duckdb"
    _seed(database)
    marker = tmp_path / "blocked"
    _install_factory(
        monkeypatch, database, "query" if phase == "interrupt" else phase, marker
    )
    if phase == "interrupt":
        monkeypatch.setattr(
            "usagebassoon.backends.motherduck._interrupt_resource", _blocked_interrupt
        )
    monkeypatch.setattr(deadlines, "CLEANUP_SECONDS", 0.5)
    processes: list[subprocess.Popen[bytes]] = []
    original = subprocess.Popen

    def launch(
        args: Sequence[str], *, stdin: int, stdout: int, bufsize: int
    ) -> subprocess.Popen[bytes]:
        process = original(args, stdin=stdin, stdout=stdout, bufsize=bufsize)
        processes.append(process)
        return process

    monkeypatch.setattr(deadlines.subprocess, "Popen", launch)
    backend: MotherDuckBackend | None = None
    started = time.monotonic()
    with pytest.raises(ResourceUnavailable):
        backend = MotherDuckBackend("usagebassoon_it", timeout_seconds=2)
        backend.query("SELECT 42 AS value")
        backend.close()
    assert time.monotonic() - started < 4
    assert processes[0].returncode is not None
    assert int(marker.read_text()) != os.getpid()
    if phase == "interrupt":
        assert marker.with_suffix(".interrupt").exists()
    if backend is not None:
        with pytest.raises(ResourceUnavailable):
            backend.query("SELECT 42 AS value")
        backend.close()
    if phase == "interrupt":
        # Restore the normal hook before constructing the next independent child.
        monkeypatch.undo()
        _install_factory(monkeypatch, database, "query", marker)
    monkeypatch.setattr(deadlines, "CLEANUP_SECONDS", 2.0)
    following = MotherDuckBackend("usagebassoon_it", timeout_seconds=2)
    try:
        assert following.query("SELECT 42 AS value").to_pylist() == [{"value": 42}]
    finally:
        following.close()


@pytest.mark.parametrize("kind", ["transaction", "snapshot"])
def test_proxy_scope_shares_deadline_and_releases_same_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """Several requests cannot extend a scope or commit work after its deadline."""
    from usagebassoon.deadlines import operation

    database = tmp_path / "native.duckdb"
    _seed(database)
    _install_factory(monkeypatch, database, "steps", tmp_path / "unused")
    backend = MotherDuckBackend("usagebassoon_it", timeout_seconds=2)
    process = backend._resource._process
    backend.query("CREATE TABLE changes (value INTEGER)")
    try:
        with pytest.raises(OperationTimeout), operation(0.4):
            if kind == "transaction":
                with backend.transaction():
                    for _ in range(3):
                        backend.query("INSERT INTO changes VALUES (1)")
            else:
                with backend.stream_snapshot(
                    ("sessions", "daily_stats", "notes")
                ) as stream:
                    for table in stream.tables:
                        list(stream.tables[table])
        assert backend._resource._process is process and process.poll() is None
        assert backend.query("SELECT * FROM changes").num_rows == 0
        with backend.transaction():
            backend.query("INSERT INTO changes VALUES (42)")
        assert backend.query("SELECT * FROM changes").to_pylist() == [{"value": 42}]
    finally:
        backend.close()


@pytest.mark.parametrize("phase", ["conflict", "write", "commit"])
def test_proxy_persistence_preserves_batch_ids_and_resolves_uncertain_commit(
    collection_bundle: CollectionBundle,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    """Retry complete batches through the factory and persistence interface."""
    import usagebassoon.deadlines as deadlines
    from usagebassoon.config import CollectionConfig
    from usagebassoon.normalizer import normalize
    from usagebassoon.persistence import persist_with_retries

    database = tmp_path / "native.duckdb"
    _seed(database)
    records = tmp_path / "attempts.jsonl"
    _install_factory(monkeypatch, database, phase, tmp_path / "blocked", records)
    monkeypatch.setattr(deadlines, "CLEANUP_SECONDS", 0.5)
    config = UsageBassoonConfig(
        tmp_path / "config.toml",
        collection_bundle.source_id,
        "motherduck",
        motherduck=MotherDuckConfig("usagebassoon_it", timeout_seconds=2),
        collection=CollectionConfig(max_retries=1),
    )
    bundle = normalize(collection_bundle)
    result = persist_with_retries(config, bundle, logging.getLogger("usagebassoon"))
    attempts = [json.loads(line) for line in records.read_text().splitlines()]
    assert len(attempts) == 2 and attempts[0] == attempts[1]
    assert attempts[0]["run_id"] == bundle.run_id
    assert (result.inserted == 0) == (phase == "commit")
    check = DuckDBBackend(database)
    try:
        assert check.has_committed_run(bundle.run_id)
        assert (
            check.query("SELECT * FROM collection_ledger").num_rows
            == bundle.tables["collection_ledger"].num_rows
        )
        assert (
            check.query("SELECT * FROM daily_stats").num_rows
            == bundle.tables["daily_stats"].num_rows
        )
    finally:
        check.close()


def test_proxy_capture_releases_failed_snapshot_and_streams_next_attempt(
    collection_bundle: CollectionBundle, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep lazy readers in the child and Parquet publication in the archiver."""
    import usagebassoon.deadlines as deadlines
    from usagebassoon.archiver import SnapshotArchiver
    from usagebassoon.normalizer import normalize
    from usagebassoon.persistence import persist_run

    database = tmp_path / "native.duckdb"
    _seed(database)
    seed = DuckDBBackend(database)
    persist_run(seed, normalize(collection_bundle))
    seed.close()
    _install_factory(monkeypatch, database, "capture", tmp_path / "blocked")
    monkeypatch.setattr(deadlines, "CLEANUP_SECONDS", 0.5)
    store = SnapshotArchiver(str(tmp_path / "archive"), timeout_seconds=2)
    backend = MotherDuckBackend("usagebassoon_it", timeout_seconds=2)
    try:
        with pytest.raises(ResourceUnavailable):
            store.write(backend, run_id="failed", manual=True)
        assert store.list_snapshots() == []
    finally:
        backend.close()
    backend = MotherDuckBackend("usagebassoon_it", timeout_seconds=2)
    try:
        assert store.write(backend, run_id="next", manual=True) is not None
        assert len(store.list_snapshots()) == 1
    finally:
        backend.close()


@pytest.mark.parametrize("phase", ["restore", "restore_commit"])
def test_proxy_restore_replays_verified_files_and_inspects_atomic_receipt(
    collection_bundle: CollectionBundle,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    """Invalidate killed restores and recover through their stable receipt."""
    import usagebassoon.deadlines as deadlines
    from usagebassoon.archiver import SnapshotArchiver
    from usagebassoon.normalizer import normalize
    from usagebassoon.persistence import persist_run
    from usagebassoon.snapshot.restore import restore_operation_id, restore_prepared

    source = DuckDBBackend(":memory:")
    source.apply_ddl()
    bundle = normalize(collection_bundle)
    persist_run(source, bundle)
    store = SnapshotArchiver(str(tmp_path / "archive"))
    uri = store.write(source, run_id=bundle.run_id, manual=True)
    source.close()
    assert uri is not None
    database = tmp_path / "destination.duckdb"
    _seed(database)
    _install_factory(monkeypatch, database, phase, tmp_path / "blocked")
    monkeypatch.setattr(deadlines, "CLEANUP_SECONDS", 0.5)
    destination = MotherDuckBackend("usagebassoon_it", timeout_seconds=2)
    with store.reader.prepare(uri) as prepared:
        try:
            with (
                pytest.raises(
                    RuntimeError, match="completion could not be determined"
                ) as failed,
                deadlines.operation(2),
            ):
                restore_prepared(destination, prepared)
            assert isinstance(failed.value.__cause__, OperationTimeout)
        finally:
            destination.close()
        assert all(path.exists() for path in prepared.files.values())
        destination = MotherDuckBackend("usagebassoon_it", timeout_seconds=2)
        try:
            rows = restore_prepared(destination, prepared)
            receipt = restore_operation_id(prepared)
            assert destination.restore_committed(receipt)
        finally:
            destination.close()
    check = DuckDBBackend(database)
    try:
        assert rows["daily_stats"] == bundle.tables["daily_stats"].num_rows
        assert check.query("SELECT * FROM restore_receipts").num_rows == 1
    finally:
        check.close()


def _native_motherduck(timeout_seconds: float = 120.0) -> _NativeMotherDuck:
    """Exercise MotherDuck deadline policy on a real offline native connection."""
    backend = object.__new__(_NativeMotherDuck)
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
            _NativeMotherDuck("usagebassoon_it", token=token, timeout_seconds=0.05)
        assert handle.closed
    else:
        backend = _NativeMotherDuck("usagebassoon_it", token=token)
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
    """Preserve the native transaction and canonical streaming semantics."""
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
