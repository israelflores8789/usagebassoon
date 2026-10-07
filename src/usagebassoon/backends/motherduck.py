# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""motherduck.py — Native MotherDuck connections and cooperative operation deadlines."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial, wraps
from pathlib import Path
from threading import Event, Timer
from typing import Literal, cast, override

import duckdb
import pyarrow as pa

from usagebassoon.backends.base import (
    AbstractStorageBackend,
    BatchPersistResult,
    CuratedIdentity,
    CuratedRenameResult,
    PersistenceBatch,
    SnapshotRead,
    SnapshotStream,
    StorageBackend,
    UpsertResult,
)
from usagebassoon.backends.duckdb_local import _DuckDBStorage
from usagebassoon.deadlines import (
    RESTORE_SECONDS,
    SNAPSHOT_SECONDS,
    OperationTimeout,
    ResourceUnavailable,
    SupervisedResource,
    cleanup_budget,
    operation,
)

_LOG = logging.getLogger("usagebassoon")

type _ScopeKind = Literal["transaction", "read", "snapshot"]


@dataclass
class _NativeResource:
    """Keep native contexts and Arrow readers inside their owning child."""

    backend: _NativeMotherDuck
    scope: AbstractContextManager[object] | None = None
    kind: _ScopeKind | None = None
    stream: SnapshotStream | None = None
    iterators: dict[str, Iterator[pa.RecordBatch]] = field(default_factory=dict)


# AGENT: do not remove
# Keep handlers module-level to send importable names without binding native state.
# The child supplies _NativeResource as the first argument.


def _open_resource(database: str, token: str, timeout: float) -> _NativeResource:
    """Create the native MotherDuck resource only in its supervised process."""
    return _NativeResource(
        _NativeMotherDuck(database, token=token, timeout_seconds=timeout)
    )


def _resource_factory(
    database: str, token: str, timeout: float
) -> Callable[[], _NativeResource]:
    """Supply an importable constructor without opening a parent connection."""
    return partial(_open_resource, database, token, timeout)


def _interrupt_resource(resource: _NativeResource) -> None:
    """Request DuckDB interruption without executing SQL from another thread."""
    resource.backend.connection.interrupt()


def _finish_scope(resource: _NativeResource, error: BaseException | None) -> None:
    """Release readers and finish the same native context that opened the scope."""
    scope = resource.scope
    resource.scope = None
    resource.kind = None
    try:
        for iterator in resource.iterators.values():
            if isinstance(iterator, Generator):
                iterator.close()
        resource.iterators.clear()
        resource.stream = None
    finally:
        if scope is not None:
            scope.__exit__(type(error) if error is not None else None, error, None)


def _close_resource(resource: _NativeResource) -> None:
    """Roll back unfinished scopes before closing the native connection."""
    try:
        _finish_scope(
            resource, ResourceUnavailable("connection closed during active scope")
        )
    finally:
        resource.backend.close()


def _begin_scope(
    resource: _NativeResource, argument: tuple[_ScopeKind, tuple[str, ...]]
) -> datetime | None:
    """Enter one transaction, diagnostic read, or lazy snapshot on the native handle."""
    kind, tables = argument
    if resource.scope is not None:
        raise RuntimeError("nested MotherDuck connection scopes are not supported")
    if kind == "transaction":
        context = resource.backend.transaction()
        context.__enter__()
        resource.scope = context
    elif kind == "read":
        context_read = resource.backend.consistent_read()
        context_read.__enter__()
        resource.scope = context_read
    else:
        context_stream = resource.backend.stream_snapshot(tables)
        resource.stream = context_stream.__enter__()
        resource.scope = context_stream
    resource.kind = kind
    return resource.stream.captured_at if resource.stream is not None else None


def _end_scope(resource: _NativeResource, error: BaseException | None) -> None:
    """Commit successful scopes or unwind them with the original failure."""
    _finish_scope(resource, error)


def _next_batch(resource: _NativeResource, table: str) -> pa.RecordBatch | None:
    """Return one bounded batch while retaining the live Arrow reader in the child."""
    if resource.kind != "snapshot" or resource.stream is None:
        raise ResourceUnavailable("snapshot scope is no longer active")
    iterator = resource.iterators.get(table)
    if iterator is None:
        iterator = iter(resource.stream.tables[table])
        resource.iterators[table] = iterator
    return next(iterator, None)


def _query(
    resource: _NativeResource, argument: tuple[str, Mapping[str, str] | None]
) -> pa.Table:
    """Execute a native query and return its protocol-defined Arrow result."""
    return resource.backend.query(*argument)


def _apply_ddl(resource: _NativeResource, _argument: None) -> None:
    """Apply native schema provisioning only when explicitly requested."""
    resource.backend.apply_ddl()


def _preflight(resource: _NativeResource, _argument: None) -> None:
    """Validate schema readiness on the owned native connection."""
    resource.backend.preflight()


def _persist_batch(
    resource: _NativeResource, batch: PersistenceBatch
) -> BatchPersistResult:
    """Keep facts, diagnostics, the ledger, and commit in one native request."""
    return resource.backend.persist_batch(batch)


def _upsert(
    resource: _NativeResource,
    argument: tuple[str, pa.Table, tuple[str, ...], tuple[str, ...]],
) -> UpsertResult:
    """Apply the declared native current-state write."""
    return resource.backend.upsert(*argument)


def _append(resource: _NativeResource, argument: tuple[str, pa.Table]) -> None:
    """Append through the native adapter's validated operation."""
    resource.backend.append(*argument)


def _committed(resource: _NativeResource, run_id: str) -> bool:
    """Inspect the native collection ledger before replay."""
    return resource.backend.has_committed_run(run_id)


def _check_restore_empty(resource: _NativeResource, _argument: None) -> None:
    """Inspect all native destination tables before restore."""
    resource.backend.check_restore_empty()


def _restore_committed(resource: _NativeResource, operation_id: str) -> bool:
    """Inspect the atomic native restore receipt."""
    return resource.backend.restore_committed(operation_id)


def _restore_snapshot(
    resource: _NativeResource, argument: tuple[dict[str, Path], str, str]
) -> None:
    """Restore verified parent-owned files and commit their receipt atomically."""
    files, operation_id, snapshot_id = argument
    resource.backend.restore_snapshot(
        files, operation_id=operation_id, snapshot_id=snapshot_id
    )


def _restore_tables(resource: _NativeResource, tables: dict[str, pa.Table]) -> None:
    """Apply the native compatibility import operation atomically."""
    resource.backend.restore_tables(tables)


def _delete_curated(resource: _NativeResource, identity: CuratedIdentity) -> int:
    """Execute the native curated delete with its complete identity."""
    return resource.backend.delete_curated(identity)


def _rename_curated(
    resource: _NativeResource,
    argument: tuple[CuratedIdentity, CuratedIdentity, datetime],
) -> CuratedRenameResult:
    """Execute a complete native curated rename on one connection."""
    source, destination, updated_at = argument
    return resource.backend.rename_curated(source, destination, updated_at=updated_at)


class MotherDuckBackend(AbstractStorageBackend):
    """StorageBackend proxy owning one supervised native MotherDuck connection.

    The child retains transactions and lazy readers across typed requests.
    Process termination invalidates the connection; callers resolve uncertain
    writes through collection ledgers or restore receipts on a new connection.
    """

    dialect = "duckdb"

    def __init__(
        self, database: str, *, token: str | None = None, timeout_seconds: float = 120.0
    ) -> None:
        """Validate credentials and supervise native startup under one deadline."""
        if not database or database.startswith("md:"):
            raise ValueError("database must be a non-empty MotherDuck database name")
        resolved = token or os.environ.get("MOTHERDUCK_TOKEN")
        if not resolved:
            raise RuntimeError(
                "MOTHERDUCK_TOKEN is required for MotherDuck connections"
            )
        self.database = database
        self.timeout_seconds = timeout_seconds
        with operation(timeout_seconds):
            self._resource = SupervisedResource(
                _resource_factory(database, resolved, timeout_seconds),
                interrupt=_interrupt_resource,
                close=_close_resource,
            )

    def _call[A, T](
        self,
        execute: Callable[[_NativeResource, A], T],
        argument: A,
        *,
        seconds: float | None = None,
    ) -> T:
        """Reuse the enclosing logical budget for every explicit native request.

        The callable must be importable by name, take the resource first, and
        never be a bound method.
        """
        with operation(self.timeout_seconds if seconds is None else seconds):
            return self._resource.call(execute, argument)

    @property
    @override
    def max_concurrent_queries(self) -> int:
        """Keep all independent queries sequential on the same child connection."""
        return 1

    @override
    def apply_ddl(self) -> None:
        """Provision the native schema through the explicit initialization path."""
        self._call(_apply_ddl, None)

    @override
    def preflight(self) -> None:
        """Inspect schema readiness on the existing child-owned connection."""
        self._call(_preflight, None)

    @override
    def query(self, sql: str, parameters: Mapping[str, str] | None = None) -> pa.Table:
        """Return the native Arrow result under the caller's operation budget."""
        return self._call(_query, (sql, parameters))

    @override
    def persist_batch(self, batch: PersistenceBatch) -> BatchPersistResult:
        """Publish a stable batch as one native transaction and ledger check."""
        return self._call(_persist_batch, batch)

    @override
    def upsert(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> UpsertResult:
        """Apply one native upsert within the active logical scope."""
        return self._call(
            _upsert, (table, data, tuple(natural_keys), tuple(change_fields))
        )

    @override
    def append(self, table: str, data: pa.Table) -> None:
        """Append through the native adapter on the same connection."""
        self._call(_append, (table, data))

    @override
    def has_committed_run(self, run_id: str) -> bool:
        """Resolve uncertain collection completion through the ledger."""
        return self._call(_committed, run_id)

    @override
    def delete_curated(self, identity: CuratedIdentity) -> int:
        """Perform the native curated delete with its complete identity."""
        return self._call(_delete_curated, identity)

    @override
    def rename_curated(
        self,
        source: CuratedIdentity,
        destination: CuratedIdentity,
        *,
        updated_at: datetime,
    ) -> CuratedRenameResult:
        """Run the entire curated rename transaction in the child."""
        return self._call(_rename_curated, (source, destination, updated_at))

    @contextmanager
    def _scope(
        self, kind: _ScopeKind, tables: tuple[str, ...] = ()
    ) -> Generator[datetime | None]:
        """Keep a native context open until the matching parent scope finishes."""
        stamp = self._call(_begin_scope, (kind, tables))
        try:
            yield stamp
            self._call(_end_scope, None)
        except BaseException as error:
            try:
                with cleanup_budget():
                    self._call(_end_scope, error)
            except Exception:
                _LOG.exception("could not unwind failed MotherDuck %s scope", kind)
            raise

    @contextmanager
    @override
    def transaction(self) -> Generator[None]:
        """Share one deadline across statements, commit, and bounded rollback."""
        with operation(self.timeout_seconds), self._scope("transaction"):
            yield

    @contextmanager
    @override
    def consistent_read(self) -> Generator[StorageBackend]:
        """Pin diagnostic reads at the first remote query on the same connection."""
        with operation(self.timeout_seconds), self._scope("read"):
            yield self

    @contextmanager
    @override
    def stream_snapshot(self, tables: Sequence[str]) -> Generator[SnapshotStream]:
        """Transfer bounded Arrow batches while the child retains one read point."""
        with (
            operation(SNAPSHOT_SECONDS),
            self._scope("snapshot", tuple(tables)) as stamp,
        ):
            assert stamp is not None

            def batches(table: str) -> Generator[pa.RecordBatch]:
                """Fetch the next batch under the shared snapshot deadline."""
                while (batch := self._call(_next_batch, table)) is not None:
                    yield batch

            yield SnapshotStream(stamp, {table: batches(table) for table in tables})

    @override
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Materialize the protocol's tables from one bounded native stream."""
        from usagebassoon.storage_model import CANONICAL_TABLE_SCHEMAS

        with self.stream_snapshot(tables) as stream:
            return SnapshotRead(
                stream.captured_at,
                {
                    table: pa.Table.from_batches(
                        list(stream.tables[table]),
                        schema=CANONICAL_TABLE_SCHEMAS[table],
                    )
                    for table in tables
                },
            )

    @override
    def check_restore_empty(self) -> None:
        """Inspect destination emptiness on the native connection."""
        self._call(_check_restore_empty, None)

    @override
    def restore_committed(self, operation_id: str) -> bool:
        """Inspect the native receipt without assuming a terminated write failed."""
        return self._call(_restore_committed, operation_id)

    @override
    def restore_snapshot(
        self, files: Mapping[str, Path], *, operation_id: str, snapshot_id: str
    ) -> None:
        """Restore verified files atomically under the shared recovery deadline."""
        self._call(
            _restore_snapshot,
            (dict(files), operation_id, snapshot_id),
            seconds=RESTORE_SECONDS,
        )

    @override
    def restore_tables(self, tables: Mapping[str, pa.Table]) -> None:
        """Import canonical Arrow tables atomically on the native connection."""
        self._call(_restore_tables, dict(tables), seconds=RESTORE_SECONDS)

    @override
    def is_retryable_error(self, error: Exception) -> bool:
        """Preserve timeout and transaction-conflict retries for stable batches."""
        return isinstance(error, OperationTimeout) or (
            isinstance(error, duckdb.TransactionException)
            and "conflict" in str(error).casefold()
        )

    @override
    def snapshot_provenance(self) -> dict[str, object]:
        """Record MotherDuck's shared physical DuckDB schema contract."""
        from usagebassoon.schema_assets import SCHEMA_VERSION, schema_hash

        return {
            "source_backend": "motherduck",
            "backend_schema_version": SCHEMA_VERSION,
            "backend_schema_hash": schema_hash("duckdb"),
        }

    @override
    def compaction_backlog(self) -> pa.Table | None:
        """MotherDuck transactional upserts require no compaction."""
        return None

    @override
    def prepare_recovery(self, *, notice: Callable[[str], None] | None = None) -> None:
        """MotherDuck has no scheduled compaction to pause."""
        del notice

    @override
    def configure_maintenance(self, *, enabled: bool) -> str | None:
        """MotherDuck requires no native scheduled maintenance."""
        del enabled
        return None

    @override
    def maintenance_status(self) -> tuple[bool, str] | None:
        """Report native maintenance inapplicability explicitly."""
        return None

    @override
    def restore_stages(self) -> list[dict[str, object]]:
        """Native atomic restores create no disposable stages."""
        return []

    @override
    def cleanup_restore_stages(self) -> None:
        """MotherDuck has no staged restore jobs to clean."""

    @override
    def close(self) -> None:
        """Close or forcibly terminate the child within one cleanup allowance."""
        with cleanup_budget():
            self._resource.close()


def _interruptible[**P, T](method: Callable[P, T]) -> Callable[P, T]:
    """Scope a synchronous backend operation to its native interruption watchdog."""

    @wraps(method)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        with cast(_NativeMotherDuck, args[0])._native_operation():
            return method(*args, **kwargs)

    return wrapped


class _NativeMotherDuck(_DuckDBStorage):
    """StorageBackend backed by a MotherDuck database.

    MotherDuck uses the DuckDB SQL schema but has distinct credential and URI
    handling, which is kept here rather than in the local DuckDB module.
    """

    @contextmanager
    @override
    def consistent_read(self) -> Generator[StorageBackend]:
        """Pin the remote database before yielding transactional diagnostic reads."""
        with self.transaction():
            # Constant queries can execute locally without starting a remote snapshot.
            self.query("SELECT version FROM schema_marker LIMIT 1")
            yield self

    @property
    @override
    def max_concurrent_queries(self) -> int:
        """Keep independent reads sequential on the owned DuckDB connection."""
        return 1

    @override
    def compaction_backlog(self) -> pa.Table | None:
        """Return None because transactional upserts require no compaction."""
        return None

    @override
    def prepare_recovery(self, *, notice: Callable[[str], None] | None = None) -> None:
        """MotherDuck transactional upserts have no scheduled compaction."""
        del notice

    @override
    def snapshot_provenance(self) -> dict[str, object]:
        """Identify MotherDuck while recording its shared DuckDB SQL contract."""
        return {**super().snapshot_provenance(), "source_backend": "motherduck"}

    @override
    def configure_maintenance(self, *, enabled: bool) -> str | None:
        """Explicitly report that MotherDuck requires no native maintenance."""
        del enabled
        return None

    @override
    def maintenance_status(self) -> tuple[bool, str] | None:
        """MotherDuck transactional upserts need no scheduled maintenance."""
        return None

    @override
    def restore_stages(self) -> list[dict[str, object]]:
        """MotherDuck restores directly in a transaction without staging."""
        return []

    @override
    def cleanup_restore_stages(self) -> None:
        """MotherDuck creates no persistent restore stages."""

    def __init__(
        self,
        database: str,
        *,
        token: str | None = None,
        timeout_seconds: float = 120.0,
    ) -> None:
        """Open a native local handle, then attach the configured MotherDuck database.

        Args:
            database: MotherDuck database name without the ``md:`` prefix.
            token: Service token, or ``MOTHERDUCK_TOKEN`` when omitted.
            timeout_seconds: Complete operation budget, including commit.
        """
        if not database or database.startswith("md:"):
            raise ValueError("database must be a non-empty MotherDuck database name")
        resolved_token = token or os.environ.get("MOTHERDUCK_TOKEN")
        if not resolved_token:
            raise RuntimeError(
                "MOTHERDUCK_TOKEN is required for MotherDuck connections"
            )
        self.database = database
        self.timeout_seconds = timeout_seconds
        self._watchdog: Timer | None = None
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            with operation(timeout_seconds):
                connection = duckdb.connect(":memory:")
                super().__init__(connection)
                with self._native_operation():
                    self._execute("INSTALL motherduck")
                    self._execute("LOAD motherduck")
                    self._execute("SET motherduck_token = ?", [resolved_token])
                    self._execute("SET motherduck_attach_mode = 'single'")
                    location = ("md:" + database).replace("'", "''")
                    self._execute(f"ATTACH '{location}'")
                    quoted = database.replace('"', '""')
                    self._execute(f'USE "{quoted}"')
        except BaseException:
            if connection is not None:
                try:
                    self.close()
                except Exception:
                    _LOG.exception("could not close failed MotherDuck startup")
            raise

    @override
    @_interruptible
    def apply_ddl(self) -> None:
        """Share one deadline across the complete apply ddl operation."""
        return super().apply_ddl()

    @override
    @_interruptible
    def preflight(self) -> None:
        """Share one deadline across the complete preflight operation."""
        return super().preflight()

    @override
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Share one deadline across the complete read snapshot tables operation."""
        with self._native_operation(SNAPSHOT_SECONDS):
            return super().read_snapshot_tables(tables)

    @override
    @_interruptible
    def check_restore_empty(self) -> None:
        """Share one deadline across the complete check restore empty operation."""
        return super().check_restore_empty()

    @override
    @_interruptible
    def restore_committed(self, operation_id: str) -> bool:
        """Share one deadline across the complete restore committed operation."""
        return super().restore_committed(operation_id)

    @override
    def restore_snapshot(
        self, files: Mapping[str, Path], *, operation_id: str, snapshot_id: str
    ) -> None:
        """Share one deadline across the complete restore snapshot operation."""
        with self._native_operation(RESTORE_SECONDS):
            return super().restore_snapshot(
                files, operation_id=operation_id, snapshot_id=snapshot_id
            )

    @override
    def restore_tables(self, tables: Mapping[str, pa.Table]) -> None:
        """Share one deadline across the complete restore tables operation."""
        with self._native_operation(RESTORE_SECONDS):
            return super().restore_tables(tables)

    @override
    @_interruptible
    def upsert(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> UpsertResult:
        """Share one deadline across the complete upsert operation."""
        return super().upsert(table, data, natural_keys, change_fields)

    @override
    @_interruptible
    def append(self, table: str, data: pa.Table) -> None:
        """Share one deadline across the complete append operation."""
        return super().append(table, data)

    @override
    @_interruptible
    def has_committed_run(self, run_id: str) -> bool:
        """Share one deadline across the complete has committed run operation."""
        return super().has_committed_run(run_id)

    @override
    @_interruptible
    def query(self, sql: str, parameters: Mapping[str, str] | None = None) -> pa.Table:
        """Share one deadline across the complete query operation."""
        return super().query(sql, parameters)

    @override
    @_interruptible
    def delete_curated(self, identity: CuratedIdentity) -> int:
        """Share one deadline across the complete delete curated operation."""
        return super().delete_curated(identity)

    @override
    @_interruptible
    def rename_curated(
        self,
        source: CuratedIdentity,
        destination: CuratedIdentity,
        *,
        updated_at: datetime,
    ) -> CuratedRenameResult:
        """Share one deadline across the complete rename curated operation."""
        return super().rename_curated(source, destination, updated_at=updated_at)

    @override
    @_interruptible
    def persist_batch(self, batch: PersistenceBatch) -> BatchPersistResult:
        """Bound all facts, diagnostics and commit by one attempt deadline."""
        return super().persist_batch(batch)

    @contextmanager
    @override
    def transaction(self) -> Generator[None]:
        """Keep statements and commit acknowledgement in one operation scope."""
        with self._native_operation(), super().transaction():
            yield

    @override
    def is_retryable_error(self, error: Exception) -> bool:
        """Replay stable batches after timeouts or native transaction conflicts."""
        return isinstance(error, OperationTimeout) or super().is_retryable_error(error)

    @contextmanager
    @override
    def stream_snapshot(self, tables: Sequence[str]) -> Generator[SnapshotStream]:
        """Let a streamed capture consume its longer snapshot operation budget."""
        with (
            self._native_operation(SNAPSHOT_SECONDS),
            super().stream_snapshot(tables) as stream,
        ):
            yield stream

    def _stop_watchdog(self) -> None:
        """Cancel and join the timer before cleanup or another operation can start."""
        timer = self._watchdog
        if timer is not None:
            timer.cancel()
            if timer.ident is not None:
                timer.join(timeout=1.0)
                if timer.is_alive():
                    raise ResourceUnavailable("native interruption did not return")
            self._watchdog = None

    @contextmanager
    def _native_operation(self, seconds: float | None = None) -> Generator[None]:
        """Signal expiry through DuckDB and await actual native completion.

        Interruption is cooperative, including during ATTACH and commit; it does
        not guarantee a hard wall-clock limit during an unresponsive native call.
        """
        with operation(
            self.timeout_seconds if seconds is None else seconds
        ) as deadline:
            assert deadline is not None
            if self._watchdog is not None:
                yield
                return
            expired = Event()

            def interrupt() -> None:
                """Request cancellation on the operation's exclusively owned handle."""
                expired.set()
                try:
                    self.connection.interrupt()
                except Exception:
                    _LOG.exception("could not interrupt expired MotherDuck operation")

            timer = Timer(deadline.remaining(), interrupt)
            timer.daemon = True
            self._watchdog = timer
            timer.start()
            try:
                yield
            except duckdb.Error as error:
                if expired.is_set():
                    raise OperationTimeout(
                        "MotherDuck operation deadline exceeded; "
                        "completion may be uncertain"
                    ) from error
                raise
            finally:
                self._stop_watchdog()

    @contextmanager
    @override
    def _transaction_cleanup(self) -> Generator[None]:
        """Stop foreground interruption before rollback under its own grace."""
        self._stop_watchdog()
        with cleanup_budget(), self._native_operation():
            yield

    @override
    def close(self) -> None:
        """Stop the operation watchdog before closing the owned native connection."""
        self._stop_watchdog()
        with cleanup_budget():
            self.connection.close()
