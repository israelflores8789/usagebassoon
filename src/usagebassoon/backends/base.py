# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""base.py — StorageBackend protocol.

Arrow contract shared by all backends. Implementations own their
dialect SQL and callers never see engine handles.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractContextManager
from typing import Protocol

import pyarrow as pa


class StorageBackend(Protocol):
    """A warehouse backend that exchanges data as Arrow tables."""

    def apply_ddl(self) -> None:
        """Create (idempotently) the backend's dialect-native schema."""
        ...

    def merge(
        self,
        table: str,
        data: pa.Table,
        natural_keys: Sequence[str],
        measure_fields: Sequence[str],
    ) -> int:
        """Append rows whose key is new or whose measures changed.

        Args:
            table: Target fact table.
            data: Staged batch carrying derived columns and collected_at.
            natural_keys: Natural identity columns of a logical row.
            measure_fields: Columns compared null-safe to detect change.

        Returns:
            Rows appended (additions plus superseding updates).
        """
        ...

    def append(self, table: str, data: pa.Table) -> None:
        """Append rows to a non-delta append-only table."""
        ...

    def query(self, sql: str) -> pa.Table:
        """Execute SQL in the configured dialect and return Arrow."""
        ...

    def transaction(self) -> AbstractContextManager[None]:
        """Best-effort multi-statement transaction context."""
        ...

    def close(self) -> None:
        """Release the backend connection."""
        ...
