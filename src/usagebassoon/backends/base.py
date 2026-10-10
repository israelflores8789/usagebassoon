# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""base.py — Shared atomic-persistence contracts for Arrow storage backends."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol
from uuid import UUID

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class UpsertResult:
    """Counts produced while applying a current-state Arrow batch.

    Attributes:
        inserted: Rows whose natural key was not yet present.
        updated: Existing rows whose logical values changed.
    """

    inserted: int = 0
    updated: int = 0

    @property
    def affected(self) -> int:
        """Return the total number of rows inserted or updated."""
        return self.inserted + self.updated


@dataclass(frozen=True, slots=True)
class CurrentStateWrite:
    """One current-state table included in an atomic persistence batch.

    Attributes:
        table: DDL-defined target table.
        data: Canonical Arrow rows for the target.
        natural_keys: Columns that uniquely identify current rows.
        change_fields: Columns compared null-safely during an update.
    """

    table: str
    data: pa.Table
    natural_keys: tuple[str, ...]
    change_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PersistenceBatch:
    """Stable observations belonging to one client-side collection run."""

    run_id: str
    current_state: tuple[CurrentStateWrite, ...]
    append_only: Mapping[str, pa.Table]
    collection_ledger: pa.Table

    @property
    def source_id(self) -> str:
        """Return the sole source namespace of this collection's ledger."""
        if not self.collection_ledger.num_rows:
            raise ValueError("collection_ledger requires at least one row")
        values = self.collection_ledger.column("source_id").to_pylist()
        if (
            not isinstance(values[0], str)
            or not values[0]
            or any(value != values[0] for value in values)
        ):
            raise ValueError("collection_ledger requires one nonempty source_id")
        return values[0]


@dataclass(frozen=True, slots=True)
class SnapshotRead:
    """Warehouse tables observed at one consistent read point.

    Attributes:
        captured_at: Warehouse timestamp of the read point.
        tables: Materialized canonical Arrow tables by name.
    """

    captured_at: datetime
    tables: Mapping[str, pa.Table]


@dataclass(frozen=True, slots=True)
class SnapshotStream:
    """Canonical Arrow batches consumed while a consistent read remains open."""

    captured_at: datetime
    tables: Mapping[str, Iterable[pa.RecordBatch]]


@dataclass(frozen=True, slots=True)
class BatchPersistResult:
    """Outcome of applying one atomic persistence batch.

    Attributes:
        per_table: Current-state results keyed by table name.
        already_committed: True when the same run ID was committed earlier.
    """

    per_table: Mapping[str, UpsertResult]
    already_committed: bool = False

    @property
    def inserted(self) -> int:
        """Return the total number of inserted current-state rows."""
        return sum(result.inserted for result in self.per_table.values())

    @property
    def updated(self) -> int:
        """Return the total number of updated current-state rows."""
        return sum(result.updated for result in self.per_table.values())


@dataclass(frozen=True, slots=True)
class ActiveTransaction:
    """One active backend transaction relevant to warehouse diagnostics.

    Attributes:
        job_id: Backend-assigned job identifier.
        transaction_id: Backend-assigned transaction identifier.
    """

    job_id: str
    transaction_id: str


type CuratedTable = Literal["notes", "tags"]


@dataclass(frozen=True, slots=True)
class CuratedIdentity:
    """Target selector for a global tag or a source-scoped session note.

    Attributes:
        table: Curation table containing the row.
        values: Ordered target fields, including source provenance for tags.
    """

    table: CuratedTable
    values: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        """Require the exact identity columns for the declared curation table."""
        expected = {
            "notes": ("source_id", "client", "session_id"),
            "tags": ("source_id", "scope", "client", "workspace", "session_id", "tag"),
        }[self.table]
        names = tuple(name for name, _ in self.values)
        if names != expected:
            raise ValueError(f"{self.table} identity must contain {expected!r}")
        required = (
            ("source_id", "client", "session_id")
            if self.table == "notes"
            else (
                "source_id",
                "scope",
                "tag",
            )
        )
        values = dict(self.values)
        if any(not values[name].strip() for name in required):
            raise ValueError("curated identity has an empty required value")

    @property
    def target_values(self) -> tuple[tuple[str, str], ...]:
        """Return target fields, excluding source provenance only for global tags."""
        if self.table == "notes":
            return self.values
        return tuple(
            (name, value) for name, value in self.values if name != "source_id"
        )

    @property
    def source_id(self) -> str:
        """Return tag mutation provenance or the note's session namespace."""
        return dict(self.values)["source_id"]

    def parameters(self, *, prefix: str = "") -> dict[str, str]:
        """Return uniquely named query parameters for the identity values."""
        return {f"{prefix}{name}": value for name, value in self.target_values}


@dataclass(frozen=True, slots=True)
class CuratedRenameResult:
    """Outcome of an atomic curation rename.

    Attributes:
        renamed: Whether the source assignment changed its label.
        destination_exists: Whether an existing destination prevented the rename.
    """

    renamed: bool
    destination_exists: bool = False


class CuratedRenameError(RuntimeError):
    """Raised when an atomic curated rename could not remove its source row."""


def is_simple_identifier(value: str) -> bool:
    """Return whether a value is a portable unquoted internal identifier."""
    return value.isascii() and value.isidentifier()


class StorageBackend(Protocol):
    """Public capability contract for a dialect-specific Arrow warehouse."""

    dialect: str

    @property
    def max_concurrent_queries(self) -> int:
        """Return the safe concurrency limit for independent read queries."""
        ...

    def consistent_read(self) -> AbstractContextManager[StorageBackend]:
        """Open reads at the first query's snapshot; job metadata stays live.

        The returned backend pins all table reads and freshness calculations to
        one instant. Callers must finish every worker before leaving the scope.
        """
        ...

    def apply_ddl(self) -> None:
        """Create the backend's dialect-native schema and views idempotently."""
        ...

    def persist_batch(self, batch: PersistenceBatch) -> BatchPersistResult:
        """Publish replay-safe observations using the backend's native strategy."""
        ...

    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Read all requested tables from one warehouse state."""
        ...

    def stream_snapshot(
        self, tables: Sequence[str]
    ) -> AbstractContextManager[SnapshotStream]:
        """Yield canonical Arrow batches pinned to one backend read point."""
        ...

    def check_restore_empty(self) -> None:
        """Reject populated managed or unexpected destination base tables."""
        ...

    def snapshot_provenance(self) -> dict[str, object]:
        """Return logical provider and physical schema provenance for an archive."""
        ...

    def prepare_recovery(self, *, notice: Callable[[str], None] | None = None) -> None:
        """Disable applicable maintenance and await its bounded completion."""
        ...

    def configure_maintenance(self, *, enabled: bool) -> str | None:
        """Provision native maintenance in the explicit initialization path."""
        ...

    def maintenance_status(self) -> tuple[bool, str] | None:
        """Inspect scheduled maintenance, or report explicit inapplicability."""
        ...

    def restore_snapshot(
        self, files: Mapping[str, Path], *, operation_id: str, snapshot_id: str
    ) -> None:
        """Atomically restore verified files and record a completion receipt."""
        ...

    def restore_committed(self, operation_id: str) -> bool:
        """Resolve an ambiguous restore acknowledgement from its atomic receipt."""
        ...

    def restore_stages(self) -> list[dict[str, object]]:
        """Inspect provider-owned restore stages, excluding unrelated tables."""
        ...

    def cleanup_restore_stages(self) -> None:
        """Discard owned stages only after their jobs are terminal."""
        ...

    def is_retryable_error(self, error: Exception) -> bool:
        """Return whether an error permits retrying the same collection run."""
        ...

    def compaction_backlog(self) -> pa.Table | None:
        """Return overdue arrival buckets, or None when compaction is inapplicable.

        Rows contain domain, arrival_day, pending_rows, and age_days. Return all
        buckets at least two days old so report limits cannot hide retention risk.
        """
        ...

    def active_transactions(self, limit: int) -> tuple[ActiveTransaction, ...]:
        """Return active transactions relevant to this backend, if supported."""
        ...

    def upsert(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> UpsertResult:
        """Publish observations at the declared natural key.

        Args:
            table: Current-state table named by the active dialect DDL.
            data: Normalized Arrow batch with columns matching that table.
            natural_keys: Columns that uniquely identify a current row.
            change_fields: Fields validated against the incoming table schema.

        Returns:
            Backend write counts; BigQuery counts appended observations.
        """
        ...

    def append(self, table: str, data: pa.Table) -> None:
        """Append events, publish BigQuery raw state, or restore empty local state."""
        ...

    def query(self, sql: str, parameters: Mapping[str, str] | None = None) -> pa.Table:
        """Execute SQL with named string parameters and return an Arrow table."""
        ...

    def delete_curated(self, identity: CuratedIdentity) -> int:
        """Delete one fully identified user-curated row and return its count."""
        ...

    def rename_curated(
        self,
        source: CuratedIdentity,
        destination: CuratedIdentity,
        *,
        updated_at: datetime,
    ) -> CuratedRenameResult:
        """Atomically rename a curated assignment while preserving its creation time."""
        ...

    def transaction(self) -> AbstractContextManager[None]:
        """Return a native transaction context for one persistence batch."""
        ...

    def close(self) -> None:
        """Release backend resources."""
        ...

    def preflight(self) -> None:
        """Validate initialization and reconcile registered schema upgrades."""
        ...

    def restore_tables(self, tables: Mapping[str, pa.Table]) -> None:
        """Restore into an empty destination while all its writers are stopped."""
        ...


def close_backend(
    backend: StorageBackend,
    *,
    context: str,
    logger: logging.Logger | None = None,
) -> None:
    """Close a backend without allowing cleanup to mask the real outcome.

    Args:
        backend: Open backend to close.
        context: Operation after which the backend is being closed.
        logger: Optional logger used for cleanup diagnostics.
    """
    active_logger = logger or logging.getLogger("usagebassoon")
    try:
        backend.close()
    except Exception:
        active_logger.exception("could not close backend after %s", context)


class AbstractStorageBackend(ABC):
    """Shared invariant enforcement for concrete storage backends."""

    @property
    @abstractmethod
    def max_concurrent_queries(self) -> int:
        """Return the safe concurrency limit for independent read queries."""

    @abstractmethod
    def consistent_read(self) -> AbstractContextManager[StorageBackend]:
        """Return a scoped backend with one snapshot for all table reads."""

    @abstractmethod
    def apply_ddl(self) -> None:
        """Create the backend's dialect-native schema and views idempotently."""

    def is_retryable_error(self, error: Exception) -> bool:
        """Return whether an error permits retrying the current collection run.

        Args:
            error: Failure raised while persisting the atomic batch.

        Returns:
            False unless a concrete backend recognizes a safe retry condition.
        """
        del error
        return False

    @abstractmethod
    def compaction_backlog(self) -> pa.Table | None:
        """Return overdue arrival buckets, or None for direct upsert backends."""

    def active_transactions(self, limit: int) -> tuple[ActiveTransaction, ...]:
        """Return active write transactions when the backend can inspect them.

        Args:
            limit: Maximum diagnostic rows to return.

        Returns:
            An empty tuple for backends without transaction-job observability.
        """
        if limit < 1:
            raise ValueError("limit must be positive")
        return ()

    @abstractmethod
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Return tables materialized at one consistent read point."""

    @abstractmethod
    def has_committed_run(self, run_id: str) -> bool:
        """Return whether a complete collection cycle has this run identifier."""

    def stream_snapshot(
        self, tables: Sequence[str]
    ) -> AbstractContextManager[SnapshotStream]:
        """Require a consistent provider streaming read."""
        raise NotImplementedError

    def check_restore_empty(self) -> None:
        """Require native inspection of every destination base table."""
        raise NotImplementedError

    def snapshot_provenance(self) -> dict[str, object]:
        """Require the provider to identify its physical installation contract."""
        raise NotImplementedError

    def prepare_recovery(self, *, notice: Callable[[str], None] | None = None) -> None:
        """Require explicit provider maintenance behavior."""
        raise NotImplementedError

    def configure_maintenance(self, *, enabled: bool) -> str | None:
        """Require explicit provisioning or documented inapplicability."""
        raise NotImplementedError

    def maintenance_status(self) -> tuple[bool, str] | None:
        """Require explicit provider maintenance inspection behavior."""
        raise NotImplementedError

    def restore_snapshot(
        self, files: Mapping[str, Path], *, operation_id: str, snapshot_id: str
    ) -> None:
        """Require atomic file-based restore and completion receipts."""
        raise NotImplementedError

    def restore_committed(self, operation_id: str) -> bool:
        """Require provider-specific receipt inspection."""
        raise NotImplementedError

    def restore_stages(self) -> list[dict[str, object]]:
        """Require explicit provider stage inspection behavior."""
        raise NotImplementedError

    def cleanup_restore_stages(self) -> None:
        """Require explicit provider stage cleanup behavior."""
        raise NotImplementedError

    @abstractmethod
    def upsert(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> UpsertResult:
        """Apply a single current-state mutation."""

    @abstractmethod
    def append(self, table: str, data: pa.Table) -> None:
        """Append a single DDL-defined Arrow table."""

    @abstractmethod
    def query(self, sql: str, parameters: Mapping[str, str] | None = None) -> pa.Table:
        """Execute dialect-native SQL and materialize Arrow results."""

    @abstractmethod
    def delete_curated(self, identity: CuratedIdentity) -> int:
        """Delete one fully identified user-curated row."""

    @abstractmethod
    def rename_curated(
        self,
        source: CuratedIdentity,
        destination: CuratedIdentity,
        *,
        updated_at: datetime,
    ) -> CuratedRenameResult:
        """Atomically rename a curated assignment while retaining its creation time."""

    @abstractmethod
    def transaction(self) -> AbstractContextManager[None]:
        """Return a native transaction context for one persistence batch."""

    @abstractmethod
    def close(self) -> None:
        """Release backend resources."""

    @staticmethod
    def _validate_upsert(
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        change_fields: Sequence[str],
    ) -> None:
        """Validate one current-state write before generating backend SQL.

        Args:
            table: Internal target table name.
            data: Canonical Arrow rows.
            natural_keys: Current-state identity columns.
            change_fields: Material-change columns.

        Raises:
            ValueError: If the table, columns, or keys are malformed.
        """
        if not is_simple_identifier(table):
            raise ValueError(f"expected a simple table identifier, got {table!r}")
        if not natural_keys:
            raise ValueError("at least one natural key is required")
        columns = set(data.column_names)
        required = set(natural_keys) | set(change_fields)
        if missing := required - columns:
            raise ValueError(f"staged data is missing columns: {sorted(missing)}")
        invalid = [
            name
            for name in (*natural_keys, *change_fields)
            if not is_simple_identifier(name)
        ]
        if invalid:
            raise ValueError(f"expected simple column identifiers, got {invalid!r}")
        seen: set[tuple[object | None, ...]] = set()
        for row in data.select(natural_keys).to_pylist():
            key = tuple(row[name] for name in natural_keys)
            if any(value is None for value in key):
                raise ValueError("natural key columns must not contain null values")
            if key in seen:
                raise ValueError("staged data contains duplicate natural keys")
            seen.add(key)

    def _validate_batch(self, batch: PersistenceBatch) -> None:
        """Validate run, source, and non-null observation identities before writing."""
        UUID(batch.run_id)
        source_id = batch.source_id
        names: set[str] = set()
        writes = [(write.table, write.data) for write in batch.current_state]
        writes.extend(batch.append_only.items())
        writes.append(("collection_ledger", batch.collection_ledger))
        for table, data in writes:
            if table in names or not is_simple_identifier(table):
                raise ValueError(f"invalid or repeated batch table {table!r}")
            names.add(table)
            for required in ("source_id", "event_id", "collected_at"):
                if (
                    required not in data.column_names
                    or data.column(required).null_count
                ):
                    raise ValueError(f"{table} requires non-null {required}")
            if any(
                value != source_id for value in data.column("source_id").to_pylist()
            ):
                raise ValueError(f"{table} has a mismatched source_id")
            for value in data.column("event_id").to_pylist():
                UUID(str(value))
            if "run_id" in data.column_names and any(
                value != batch.run_id for value in data.column("run_id").to_pylist()
            ):
                raise ValueError(f"{table} has a mismatched run_id")
        for write in batch.current_state:
            self._validate_upsert(
                write.table, write.data, write.natural_keys, write.change_fields
            )

    def persist_batch(self, batch: PersistenceBatch) -> BatchPersistResult:
        """Apply DuckDB-compatible state upserts and append events atomically."""
        self._validate_batch(batch)
        with self.transaction():
            if self.has_committed_run(batch.run_id):
                return BatchPersistResult({}, already_committed=True)
            per_table: dict[str, UpsertResult] = {}
            for write in batch.current_state:
                per_table[write.table] = self.upsert(
                    write.table, write.data, write.natural_keys, write.change_fields
                )
            for table, data in batch.append_only.items():
                self.append(table, data)
            self.append("collection_ledger", batch.collection_ledger)
            return BatchPersistResult(per_table)

    @abstractmethod
    def preflight(self) -> None:
        """Validate initialization and reconcile registered schema upgrades."""

    def restore_tables(self, tables: Mapping[str, pa.Table]) -> None:
        """Atomically restore canonical rows into an empty destination."""
        from usagebassoon.storage_model import SNAPSHOT_TABLES

        unknown = set(tables) - set(SNAPSHOT_TABLES)
        if unknown:
            raise ValueError(f"unsupported restore tables: {sorted(unknown)}")
        with self.transaction():
            for table in SNAPSHOT_TABLES:
                if self.query(f"SELECT * FROM {table} LIMIT 1").num_rows:
                    raise ValueError("restore requires an empty warehouse")
            for table, data in tables.items():
                self.append(table, data)
