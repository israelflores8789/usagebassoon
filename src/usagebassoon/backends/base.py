# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""base.py — Shared atomic-persistence contracts for Arrow storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol
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
    """All writes belonging to one idempotent collection cycle.

    Attributes:
        run_id: UUID identifying this collection cycle and retry unit.
        current_state: Current-state tables updated in this cycle.
        append_only: Audit and history tables excluding ``ingest_runs``.
        ingest_runs: Exactly one audit row, written last within the batch.
    """

    run_id: str
    current_state: tuple[CurrentStateWrite, ...]
    append_only: Mapping[str, pa.Table]
    ingest_runs: pa.Table


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

    def query(self, sql: str) -> pa.Table:
        """Execute SQL in the configured dialect and return an Arrow table."""
        ...

    def transaction(self) -> AbstractContextManager[None]:
        """Return a native transaction context for one persistence batch."""
        ...

    def close(self) -> None:
        """Release backend resources."""
        ...


class AbstractStorageBackend(ABC):
    """Shared invariant enforcement for concrete storage backends."""

    @abstractmethod
    def apply_ddl(self) -> None:
        """Create the backend's dialect-native schema and views idempotently."""

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
    def query(self, sql: str) -> pa.Table:
        """Execute dialect-native SQL and materialize Arrow results."""

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
        if self.has_committed_run(batch.run_id):
            return BatchPersistResult({}, already_committed=True)
        per_table: dict[str, UpsertResult] = {}
        with self.transaction():
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
