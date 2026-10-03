# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""audit.py — Importable source and run evidence from backends or snapshots."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from socket import gethostname

from usagebassoon.backends.base import StorageBackend
from usagebassoon.snapshot.reader import PreparedSnapshot
from usagebassoon.storage_model import SNAPSHOT_TABLES
from usagebassoon.system_metadata import capture_system_metadata

_HOST_FIELDS = (
    "host",
    "os_name",
    "os_version",
    "architecture",
    "cpu_model",
    "cpu_count",
    "memory_bytes",
    "shell",
)


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
    """Summarize every observed source, including sources without ledger evidence."""
    source_ids: set[str] = set()
    if prepared is not None:
        for table in SNAPSHOT_TABLES:
            for batch in prepared.batches(table):
                source_ids.update(
                    str(v)
                    for v in batch.column(
                        batch.schema.get_field_index("source_id")
                    ).to_pylist()
                )
        runs = (
            row
            for batch in prepared.batches("collection_ledger")
            for row in batch.to_pylist()
        )
    elif backend is not None:
        union = " UNION DISTINCT ".join(
            f"SELECT source_id FROM current_{table}" for table in SNAPSHOT_TABLES
        )
        source_ids.update(
            str(row["source_id"]) for row in backend.query(union).to_pylist()
        )
        runs = iter(
            backend.query(
                "SELECT * FROM current_collection_ledger "
                "ORDER BY finished_at DESC, run_id DESC"
            ).to_pylist()
        )
    else:
        raise ValueError("a backend or prepared snapshot is required")
    result: dict[str, dict[str, object]] = {}
    latest: dict[str, dict[str, object]] = {}
    seen_runs: set[tuple[str, str]] = set()
    for row in runs:
        source = str(row["source_id"])
        source_ids.add(source)
        key = (source, str(row["run_id"]))
        at = row.get("finished_at") or row.get("started_at")
        item = result.setdefault(
            source,
            {
                "source_id": source,
                "first_activity": at,
                "last_activity": at,
                "run_count": 0,
            },
        )
        if key not in seen_runs:
            item["run_count"] = int(str(item["run_count"])) + 1
            seen_runs.add(key)
        if str(at) < str(item["first_activity"]):
            item["first_activity"] = at
        if source not in latest or _run_order(row) > _run_order(latest[source]):
            latest[source] = row
            item["last_activity"] = at
            item["latest_outcome"] = row.get("status")
            item.update({field: row.get(field) for field in _HOST_FIELDS})
    for source in source_ids - set(result):
        result[source] = {
            "source_id": source,
            "first_activity": None,
            "last_activity": None,
            "run_count": 0,
            "latest_outcome": None,
            **dict.fromkeys(_HOST_FIELDS),
        }
    return sorted(
        result.values(),
        key=_source_order,
        reverse=True,
    )


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


def _source_order(item: dict[str, object]) -> tuple[str, str]:
    """Order sources by latest activity with an identifier tie-break."""
    return str(item["last_activity"] or ""), str(item["source_id"])
