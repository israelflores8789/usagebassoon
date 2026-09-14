# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""base.py — StorageBackend protocol for canonical Arrow tables."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol

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


class StorageBackend(Protocol):
    """A dialect-specific warehouse exchanging normalized Arrow tables."""

    def apply_ddl(self) -> None:
        """Create the backend's dialect-native schema and views idempotently."""
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
        """Return a best-effort multi-statement transaction context."""
        ...

    def close(self) -> None:
        """Release backend resources."""
        ...
