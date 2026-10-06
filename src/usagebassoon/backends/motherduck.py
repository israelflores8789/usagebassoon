# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""motherduck.py — Native MotherDuck connections and cooperative operation deadlines."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path
from threading import Event, Timer
from typing import cast, override

import duckdb
import pyarrow as pa

from usagebassoon.backends.base import (
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
    cleanup_budget,
    operation,
)

_LOG = logging.getLogger("usagebassoon")


def _interruptible[**P, T](method: Callable[P, T]) -> Callable[P, T]:
    """Scope a synchronous backend operation to its native interruption watchdog."""

    @wraps(method)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        with cast(MotherDuckBackend, args[0])._native_operation():
            return method(*args, **kwargs)

    return wrapped


class MotherDuckBackend(_DuckDBStorage):
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
                timer.join()
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
