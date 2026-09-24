# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_snapshot_fakes.py — Shared storage doubles for snapshot tests."""

from __future__ import annotations

import pyarrow as pa


class TableBackend:
    """Minimal Arrow reader used to force snapshot capture outcomes."""

    dialect = "test"

    def __init__(self, *, failed_table: str | None = None) -> None:
        """Create fixed one-row Arrow tables, optionally failing one query."""
        self.failed_table = failed_table
        self.table = pa.table({"value": [1]})
        self.queries = 0

    def query(self, sql: str) -> pa.Table:
        """Return an Arrow table or simulate a failed expected-table capture."""
        self.queries += 1
        table = sql.removeprefix("SELECT * FROM ").removesuffix(" LIMIT 0")
        if table == self.failed_table:
            raise RuntimeError("capture failed")
        return self.table
