# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""snapshots.py — Rotating Parquet snapshots over any StorageBackend.

Replaces per-run verbatim raw exports (quadratic growth). A snapshot is a
dated directory of full-table Parquet files plus a manifest led by a run
id; rotation enforces max_snapshots; restore rehydrates any backend from
cold (fresh container, torn-down VM, local-to-cloud migration).
"""

from __future__ import annotations

import io
import json
import pickle
from datetime import UTC, datetime
from typing import Any, Protocol

import pyarrow.parquet as pq
from fsspec.core import url_to_fs

from usagebassoon.backends.base import StorageBackend

SNAPSHOT_TABLES: tuple[str, ...] = (
    "sessions",
    "session_model_stats",
    "daily_stats",
    "daily_activity",
    "pricing_snapshots",
    "run_metrics",
    "reconciliation_issues",
    "schema_drift",
    "ingest_runs",
    "tags",
    "notes",
)


class SnapshotFilesystem(Protocol):
    """Minimal fsspec interface used for listing and rotating snapshots."""

    def ls(self, path: str) -> list[str | dict[str, str]]:
        """List paths under a snapshot root."""
        ...

    def rm(self, path: str, *, recursive: bool) -> None:
        """Remove one snapshot path."""
        ...


class SnapshotStore:
    """A rotating Parquet archive rooted at an fsspec URI.

    Attributes:
        uri: Configured base URI directory.
        max_snapshots: Retention ceiling.
    """

    def __init__(self, uri: str, *, max_snapshots: int = 30) -> None:
        """Configure the store.

        Args:
            uri: gs://, file://, or any fsspec-supported URI prefix.
            max_snapshots: Number of dated snapshots to retain.
        """
        self.uri = uri.rstrip("/")
        self.max_snapshots = max_snapshots

    def _fs_root(self) -> tuple[Any, str]:
        """Resolve the filesystem and in-filesystem root path.

        Returns:
            A (fs, root) pair for the configured URI.
        """
        return url_to_fs(self.uri)

    def _stamps(self, fs: SnapshotFilesystem, root: str) -> list[str]:
        """List snapshot directory names, oldest first.

        Args:
            fs: Resolved filesystem.
            root: Archive root path.

        Returns:
            Sorted snapshot stamps.
        """
        try:
            entries = fs.ls(root)
        except FileNotFoundError:
            return []
        names: list[str] = [
            entry["name"] if isinstance(entry, dict) else entry for entry in entries
        ]
        return sorted(p.rsplit("/", 1)[-1].rstrip("/") for p in names)

    def write(self, backend: StorageBackend, *, run_id: str) -> str:
        """Materialize a full-table snapshot and rotate the archive.

        Tables that raise (curation tables may not exist pre-v0.2) are
        skipped silently; only tables with at least one row are
        materialized.

        Args:
            backend: The backend to read from.
            run_id: Owning collection run id.

        Returns:
            The URI of the snapshot created.
        """
        fs, root = self._fs_root()
        when = datetime.now(UTC)
        stamp = when.strftime("%Y-%m-%dT%H%M%SZ")
        dest = f"{root}/{stamp}"
        fs.makedirs(dest, exist_ok=True)
        counts: dict[str, int] = {}
        for table in SNAPSHOT_TABLES:
            try:
                data = backend.query(f"SELECT * FROM {table}")
            except Exception:
                continue
            rows = data.num_rows if hasattr(data, "num_rows") else len(data)
            if rows == 0:
                continue
            if hasattr(data, "to_parquet") and not hasattr(data, "schema"):
                # pandas-backed shim path (offline conformance)
                with fs.open(f"{dest}/{table}.parquet", "wb") as fh:
                    fh.write(pickle.dumps(data))
            else:
                with fs.open(f"{dest}/{table}.parquet", "wb") as fh:
                    pq.write_table(data, fh)
            counts[table] = int(rows)
        manifest = {"run_id": run_id, "captured_at": when.isoformat(), "tables": counts}
        with fs.open(f"{dest}/manifest.json", "w") as fh:
            fh.write(json.dumps(manifest, indent=2) + "\n")
        self._rotate(fs, root)
        return f"{self.uri}/{stamp}"

    def _rotate(self, fs: SnapshotFilesystem, root: str) -> None:
        """Delete oldest snapshots beyond retention.

        Args:
            fs: Resolved filesystem.
            root: Archive root path.
        """
        stamps = self._stamps(fs, root)
        extra = len(stamps) - self.max_snapshots
        for stamp in stamps[: max(extra, 0)]:
            fs.rm(f"{root}/{stamp}", recursive=True)

    def list_snapshots(self) -> list[str]:
        """List snapshot stamps, oldest first.

        Returns:
            Snapshot directory names.
        """
        fs, root = self._fs_root()
        return self._stamps(fs, root)

    def restore(
        self,
        backend: StorageBackend,
        snapshot: str = "latest",
    ) -> dict[str, int]:
        """Rehydrate a backend from one snapshot.

        Args:
            backend: Destination backend (any dialect).
            snapshot: Stamp to restore, or 'latest'.

        Returns:
            Per-table restored row counts.

        Raises:
            ValueError: No snapshots exist, or stamp unknown.
        """
        fs, root = self._fs_root()
        stamps = self._stamps(fs, root)
        if not stamps:
            raise ValueError(f"no snapshots under {self.uri}")
        stamp = stamps[-1] if snapshot == "latest" else snapshot
        if stamp not in stamps:
            raise ValueError(f"unknown snapshot {stamp!r}")
        restored: dict[str, int] = {}
        for table in SNAPSHOT_TABLES:
            try:
                with fs.open(f"{root}/{stamp}/{table}.parquet", "rb") as fh:
                    raw = fh.read()
                data = (
                    pickle.loads(raw)
                    if raw[:2] != b"PA"
                    else pq.read_table(io.BytesIO(raw))
                )
            except FileNotFoundError:
                continue
            rows = data.num_rows if hasattr(data, "num_rows") else len(data)
            if rows == 0:
                continue
            backend.append(table, data)
            restored[table] = int(rows)
        return restored
