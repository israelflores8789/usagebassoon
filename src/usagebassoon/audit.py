# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""audit.py — Importable source and run evidence from backends or snapshots."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from socket import gethostname
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa

from usagebassoon.backends.base import StorageBackend
from usagebassoon.schema_assets import view_sql
from usagebassoon.snapshot.reader import PreparedSnapshot
from usagebassoon.storage_model import CANONICAL_TABLE_SCHEMAS, SNAPSHOT_TABLES
from usagebassoon.system_metadata import capture_system_metadata


def snapshot_runs(prepared: PreparedSnapshot) -> Iterable[dict[str, object]]:
    """Yield collection-domain run records from verified ledger batches."""
    for batch in prepared.batches("collection_ledger"):
        for row in batch.to_pylist():
            if row["domain"] == "collection":
                yield row


def audit_runs(
    backend: StorageBackend | None = None,
    *,
    prepared: PreparedSnapshot | None = None,
    limit: int = 20,
) -> list[dict[str, object]]:
    """Return recent runs without opening a backend for snapshot input."""
    if limit < 1:
        raise ValueError("limit must be positive")
    if prepared is not None:
        # Bound retained rows even when the permanent ledger is large.
        rows: list[dict[str, object]] = []
        for row in snapshot_runs(prepared):
            rows.append(row)
            rows.sort(key=_run_order, reverse=True)
            del rows[limit:]
        return rows
    if backend is None:
        raise ValueError("a backend or prepared snapshot is required")
    return backend.query(
        "SELECT * FROM collection_runs ORDER BY finished_at DESC, run_id DESC "
        f"LIMIT {limit}"
    ).to_pylist()


def _run_order(row: dict[str, object]) -> tuple[str, str, bool, str]:
    """Apply a deterministic order to metadata from one complete run."""
    return (
        str(row.get("finished_at") or row.get("started_at") or ""),
        str(row.get("collected_at") or ""),
        row.get("domain") == "collection",
        str(row.get("event_id") or row.get("run_id") or ""),
    )


def audit_sources(
    backend: StorageBackend | None = None, *, prepared: PreparedSnapshot | None = None
) -> list[dict[str, object]]:
    """Return one row per source without retaining historical runs in Python."""
    sql = (
        "SELECT * FROM audit_sources "
        "ORDER BY last_activity DESC NULLS LAST, source_id DESC"
    )
    if prepared is not None:
        with (
            TemporaryDirectory(prefix="usagebassoon-source-audit-") as temporary,
            duckdb.connect(
                config={
                    "memory_limit": "128MB",
                    "threads": "1",
                    "temp_directory": temporary,
                }
            ) as connection,
        ):
            for table in SNAPSHOT_TABLES:
                relation = f"current_{table}"
                if table in prepared.files:
                    connection.read_parquet(str(prepared.files[table])).create_view(
                        relation
                    )
                else:
                    connection.register(
                        relation,
                        pa.Table.from_batches(
                            [], schema=CANONICAL_TABLE_SCHEMAS[table]
                        ),
                    )
            connection.execute(view_sql("duckdb", "audit_sources"))
            return connection.execute(sql).to_arrow_table().to_pylist()
    if backend is None:
        raise ValueError("a backend or prepared snapshot is required")
    with backend.consistent_read() as read:
        return read.query(sql).to_pylist()


def source_identity_warning(source_id: str, prepared: PreparedSnapshot) -> str | None:
    """Explain matching host evidence without treating hardware as source identity."""
    sources = audit_sources(prepared=prepared)
    if any(row["source_id"] == source_id for row in sources):
        return None
    metadata: dict[str, object] = {
        "host": gethostname(),
        **asdict(capture_system_metadata()),
    }
    required = ("host", "os_name", "architecture", "cpu_count", "memory_bytes")
    if any(metadata.get(field) is None for field in required):
        return None
    cutoff = datetime.now(UTC) - timedelta(days=90)
    matches = [
        row
        for row in sources
        if isinstance(row["last_activity"], datetime)
        and row["last_activity"] >= cutoff
        and all(row.get(field) == metadata[field] for field in required)
        and all(
            row.get(field) == value
            for field, value in metadata.items()
            if value is not None
        )
    ]
    if not matches:
        return None
    evidence = ", ".join(
        f"{row['source_id']} (last active {row['last_activity']})" for row in matches
    )
    return (
        "Your configured source ID differs from a recently active matching "
        f"source: {evidence}. Resuming collection with a new ID may duplicate "
        "history. Inspect bassoon audit sources --from-snapshot "
        f"{prepared.candidate.uri}. Restore preserves every source ID "
        "and will not change your configuration."
    )
