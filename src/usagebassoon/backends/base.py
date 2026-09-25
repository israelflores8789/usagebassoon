# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""base.py — Shared atomic-persistence contracts for Arrow storage backends."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Generator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID, uuid4

import pyarrow as pa

SOURCE_LEASE_SECONDS = 120


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
    """All writes belonging to one idempotent collection cycle.

    Attributes:
        run_id: UUID identifying this collection cycle and retry unit.
        current_state: Current-state tables updated in this cycle.
        append_only: Audit and history tables excluding ``ingest_runs``.
        ingest_runs: Exactly one audit row, written last within the batch.
        lease: Source lease held from collection planning through persistence.
    """

    run_id: str
    current_state: tuple[CurrentStateWrite, ...]
    append_only: Mapping[str, pa.Table]
    ingest_runs: pa.Table
    lease: SourceLeaseToken | None = None

    @property
    def source_id(self) -> str:
        """Return the source namespace of the single ingest audit row."""
        if (
            self.ingest_runs.num_rows != 1
            or "source_id" not in self.ingest_runs.column_names
        ):
            raise ValueError("ingest_runs requires one source_id")
        value = self.ingest_runs.column("source_id").to_pylist()[0]
        if not isinstance(value, str) or not value:
            raise ValueError("ingest_runs source_id must be nonempty text")
        return value


@dataclass(frozen=True, slots=True)
class SourceLeaseToken:
    """Identity and generation of one active source lease.

    Attributes:
        source_id: Source namespace protected by the lease.
        run_id: Collection run using the lease.
        owner_id: Unique identifier for this lease attempt.
        fence: Monotonically increasing source generation.
    """

    source_id: str
    run_id: str
    owner_id: str
    fence: int


class SourceLeaseBusy(RuntimeError):
    """Raised when another collection owns an unexpired source lease."""


class SourceLeaseLost(RuntimeError):
    """Raised when a collection no longer owns its source lease."""


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
    """Complete identity for one user-curated row.

    Attributes:
        table: Curation table containing the row.
        values: Ordered identity fields and their values.
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

    def parameters(self, *, prefix: str = "") -> dict[str, str]:
        """Return uniquely named query parameters for the identity values."""
        return {f"{prefix}{name}": value for name, value in self.values}


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


def with_ingest_counts(ingest_runs: pa.Table, inserted: int, updated: int) -> pa.Table:
    """Return an ingest audit row populated with final current-state counts.

    Args:
        ingest_runs: Single normalized ingest-runs row.
        inserted: Total newly inserted current-state rows.
        updated: Total materially updated current-state rows.

    Returns:
        The run table with final inserted and updated counts.

    Raises:
        ValueError: If the audit row lacks required columns.
    """
    result = ingest_runs
    for name, value in (("rows_inserted", inserted), ("rows_updated", updated)):
        index = result.schema.get_field_index(name)
        if index < 0:
            raise ValueError(f"ingest_runs is missing required column {name!r}")
        result = result.set_column(index, name, pa.array([value], type=pa.int64()))
    return result


class StorageBackend(Protocol):
    """Public capability contract for a dialect-specific Arrow warehouse."""

    def apply_ddl(self) -> None:
        """Create the backend's dialect-native schema and views idempotently."""
        ...

    def persist_batch(self, batch: PersistenceBatch) -> BatchPersistResult:
        """Atomically commit or reject every write in one collection cycle."""
        ...

    def ensure_source_lease(self, source_id: str) -> None:
        """Provision exactly one dormant lease row for a source."""
        ...

    def claim_source_lease(
        self, source_id: str, run_id: str, owner_id: str
    ) -> SourceLeaseToken | None:
        """Claim an available source lease and advance its fence."""
        ...

    def renew_source_lease(self, lease: SourceLeaseToken) -> bool:
        """Extend an owned lease before its warehouse expiry."""
        ...

    def release_source_lease(self, lease: SourceLeaseToken) -> None:
        """Release only the matching owner and fence."""
        ...

    def guard_source_lease(self, lease: SourceLeaseToken) -> bool:
        """Mutate and verify the lease inside a persistence transaction."""
        ...

    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Read all requested tables from one warehouse state."""
        ...

    def is_retryable_error(self, error: Exception) -> bool:
        """Return whether an error permits retrying the same collection run."""
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
        """Insert new current-state rows and update materially changed ones.

        Args:
            table: Current-state table named by the active dialect DDL.
            data: Normalized Arrow batch with columns matching that table.
            natural_keys: Columns that uniquely identify a current row.
            change_fields: Logical fields used for null-safe change detection.

        Returns:
            Separate inserted and updated row counts.
        """
        ...

    def append(self, table: str, data: pa.Table) -> None:
        """Append rows to a DDL-defined append-only table.

        Callers must not use this for a current-state table unless restoring
        into a known-empty database.
        """
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
    def ensure_source_lease(self, source_id: str) -> None:
        """Provision the source row before an atomic conditional claim."""

    @abstractmethod
    def claim_source_lease(
        self, source_id: str, run_id: str, owner_id: str
    ) -> SourceLeaseToken | None:
        """Claim an available source row and return its new fence."""

    @abstractmethod
    def renew_source_lease(self, lease: SourceLeaseToken) -> bool:
        """Renew a live matching source lease."""

    @abstractmethod
    def release_source_lease(self, lease: SourceLeaseToken) -> None:
        """Clear a matching source lease without resetting its fence."""

    @abstractmethod
    def guard_source_lease(self, lease: SourceLeaseToken) -> bool:
        """Update a matching lease within the caller's write transaction."""

    @abstractmethod
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Return tables materialized at one consistent read point."""

    @abstractmethod
    def has_committed_run(self, run_id: str) -> bool:
        """Return whether a complete collection cycle has this run identifier."""

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
        """Validate the shared persistence invariants for one cycle.

        Args:
            batch: Complete collection-cycle data prepared by the merge layer.

        Raises:
            ValueError: If the batch cannot safely be applied atomically.
        """
        try:
            UUID(batch.run_id)
        except ValueError as error:
            raise ValueError("persistence batch run_id must be a UUID") from error
        if batch.ingest_runs.num_rows != 1:
            raise ValueError("persistence batches require exactly one ingest_runs row")
        if "run_id" not in batch.ingest_runs.column_names:
            raise ValueError("ingest_runs is missing required column 'run_id'")
        ingest_run_id = batch.ingest_runs.column("run_id").to_pylist()[0]
        if ingest_run_id != batch.run_id:
            raise ValueError("ingest_runs run_id does not match persistence batch")
        source_id = batch.source_id
        if batch.lease is not None and (
            batch.lease.run_id != batch.run_id or batch.lease.source_id != source_id
        ):
            raise ValueError("source lease does not match persistence batch")
        names: set[str] = set()
        for write in batch.current_state:
            if write.table in names:
                raise ValueError(f"persistence batch repeats table {write.table!r}")
            names.add(write.table)
            self._validate_upsert(
                write.table,
                write.data,
                write.natural_keys,
                write.change_fields,
            )
            if "source_id" not in write.data.column_names or any(
                value != source_id
                for value in write.data.column("source_id").to_pylist()
            ):
                raise ValueError(f"{write.table} has a mismatched source_id")
        for table, data in batch.append_only.items():
            if table in names or not is_simple_identifier(table):
                raise ValueError(f"invalid append-only table {table!r}")
            names.add(table)
            if "run_id" not in data.column_names:
                raise ValueError(f"append-only table {table!r} is missing run_id")
            if any(
                value != batch.run_id for value in data.column("run_id").to_pylist()
            ):
                raise ValueError(f"append-only table {table!r} has a mismatched run_id")
            if "source_id" in data.column_names and any(
                value != source_id for value in data.column("source_id").to_pylist()
            ):
                raise ValueError(
                    f"append-only table {table!r} has a mismatched source_id"
                )

    @contextmanager
    def _batch_lease(self, batch: PersistenceBatch) -> Generator[SourceLeaseToken]:
        """Use the collection lease or claim one for a direct batch call.

        Yields:
            A source lease token to validate inside the batch transaction.
        """
        if batch.lease is not None:
            yield batch.lease
            return
        self.ensure_source_lease(batch.source_id)
        lease = self.claim_source_lease(batch.source_id, batch.run_id, str(uuid4()))
        if lease is None:
            raise SourceLeaseBusy(f"source {batch.source_id} already has a collection")
        try:
            yield lease
        finally:
            try:
                self.release_source_lease(lease)
            except Exception:
                logging.getLogger("usagebassoon").exception(
                    "could not release source lease for %s", batch.source_id
                )

    def persist_batch(self, batch: PersistenceBatch) -> BatchPersistResult:
        """Atomically persist one validated collection cycle.

        Concrete backends may override this method for a native bulk path,
        such as BigQuery remote staging plus a multi-statement transaction.

        Args:
            batch: Complete normalized writes for one collection cycle.

        Returns:
            Per-table current-state outcomes or an idempotent no-op result.
        """
        self._validate_batch(batch)
        with self._batch_lease(batch) as lease, self.transaction():
            if self.has_committed_run(batch.run_id):
                return BatchPersistResult({}, already_committed=True)
            if not self.guard_source_lease(lease):
                raise SourceLeaseLost(f"source lease was lost for {lease.source_id}")
            per_table: dict[str, UpsertResult] = {}
            for write in batch.current_state:
                per_table[write.table] = self.upsert(
                    write.table,
                    write.data,
                    write.natural_keys,
                    write.change_fields,
                )
            for table, data in batch.append_only.items():
                self.append(table, data)
            self.append(
                "ingest_runs",
                with_ingest_counts(
                    batch.ingest_runs,
                    sum(result.inserted for result in per_table.values()),
                    sum(result.updated for result in per_table.values()),
                ),
            )
            return BatchPersistResult(per_table)
