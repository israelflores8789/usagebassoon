# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_snapshots.py — Catalog, cadence, local rotation, and GCS archive tests."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import pyarrow as pa
import pytest

from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.backends.gcs import GcsArchive as RealGcsArchive
from usagebassoon.backends.gcs import GcsObject, GcsPreconditionError
from usagebassoon.snapshots import SNAPSHOT_TABLES, SnapshotStore


class MemoryGcsArchive:
    """A generation-aware in-memory GCS archive double."""

    def __init__(self) -> None:
        """Create an empty bucket-relative object store."""
        self.objects: dict[str, tuple[int, bytes]] = {}
        self.next_generation = 1

    def relative(self, object_name: str) -> str:
        """Return the archive-relative object name."""
        return object_name.removeprefix("archive/")

    def read_bytes(self, relative_name: str, *, generation: int) -> bytes:
        """Read an exact object generation."""
        current_generation, data = self.objects[f"archive/{relative_name}"]
        if current_generation != generation:
            raise GcsPreconditionError("generation changed")
        return data

    def write_bytes(
        self,
        relative_name: str,
        payload: bytes,
        *,
        if_generation_match: int | None = None,
        content_type: str = "application/octet-stream",
    ) -> GcsObject:
        """Create or update one object under an optional generation guard."""
        del content_type
        name = f"archive/{relative_name}"
        current = self.objects.get(name)
        current_generation = current[0] if current else None
        if if_generation_match == 0 and current is not None:
            raise GcsPreconditionError("already exists")
        if (
            if_generation_match not in (None, 0)
            and current_generation != if_generation_match
        ):
            raise GcsPreconditionError("generation changed")
        generation = self.next_generation
        self.next_generation += 1
        self.objects[name] = (generation, payload)
        return GcsObject(name, generation, len(payload), None)

    def read_json(
        self, relative_name: str
    ) -> tuple[dict[str, object] | None, int | None]:
        """Read a JSON object and its generation."""
        current = self.objects.get(f"archive/{relative_name}")
        if current is None:
            return None, None
        generation, data = current
        decoded = json.loads(data)
        assert isinstance(decoded, dict)
        return decoded, generation

    def write_json_cas(
        self,
        relative_name: str,
        payload: dict[str, object],
        *,
        generation: int | None,
    ) -> GcsObject:
        """Write a JSON document with a generation compare-and-swap guard."""
        return self.write_bytes(
            relative_name,
            json.dumps(payload).encode(),
            if_generation_match=0 if generation is None else generation,
            content_type="application/json",
        )

    def delete(self, relative_name: str, *, generation: int) -> None:
        """Delete an exact object generation."""
        name = f"archive/{relative_name}"
        current = self.objects.get(name)
        if current is None or current[0] != generation:
            raise GcsPreconditionError("generation changed")
        del self.objects[name]

    def list(self, relative_prefix: str) -> tuple[GcsObject, ...]:
        """List immutable object metadata below a relative prefix."""
        prefix = f"archive/{relative_prefix}"
        return tuple(
            GcsObject(name, generation, len(data), None)
            for name, (generation, data) in self.objects.items()
            if name.startswith(prefix)
        )

    def lifecycle_delete_warnings(self) -> tuple[str, ...]:
        """Return an injectable lifecycle warning."""
        return ("a GCS Delete lifecycle rule could match the snapshot archive",)


class TableBackend:
    """Minimal Arrow reader used to force snapshot capture outcomes."""

    dialect = "test"

    def __init__(self, *, failed_table: str | None = None) -> None:
        """Create fixed one-row Arrow tables, optionally failing one query."""
        self.failed_table = failed_table
        self.table = pa.table({"value": [1]})

    def query(self, sql: str) -> pa.Table:
        """Return an Arrow table or simulate a failed expected-table capture."""
        table = sql.removeprefix("SELECT * FROM ").removesuffix(" LIMIT 0")
        if table == self.failed_table:
            raise RuntimeError("capture failed")
        return self.table


def _backend() -> DuckDBBackend:
    """Create an initialized temporary DuckDB archive source."""
    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()
    return backend


def test_default_local_rotation_retains_three_complete_snapshots(
    tmp_path: Path,
) -> None:
    """Use the unified default limit and never delete the newest publication."""
    backend = _backend()
    store = SnapshotStore(f"file://{tmp_path}/archive")
    try:
        for index in range(4):
            assert store.write(backend, run_id=f"run-{index}") is not None
        snapshots = store.list_snapshots()
        assert len(snapshots) == 3
        assert all(
            (tmp_path / "archive" / snapshot / "manifest.json").exists()
            for snapshot in snapshots
        )
    finally:
        backend.close()


def test_custom_local_rotation_and_stale_prefix_are_catalog_safe(
    tmp_path: Path,
) -> None:
    """Ignore incomplete staging prefixes and apply a custom retention ceiling."""
    archive = tmp_path / "archive"
    (archive / "staging-orphan").mkdir(parents=True)
    (archive / "staging-orphan" / "manifest.json").write_text("{}")
    backend = _backend()
    store = SnapshotStore(f"file://{archive}", max_snapshots=1)
    try:
        assert store.list_snapshots() == []
        first = store.write(backend, run_id="first")
        second = store.write(backend, run_id="second")
        assert first is not None and second is not None
        assert store.list_snapshots() == [second.rsplit("/", 1)[-1]]
        assert (archive / "staging-orphan").exists()
    finally:
        backend.close()


def test_failed_table_never_publishes_a_manifest(tmp_path: Path) -> None:
    """Reject a partial capture instead of producing a misleading snapshot."""
    store = SnapshotStore(f"file://{tmp_path}/archive")
    backend = TableBackend(failed_table="notes")
    with pytest.raises(RuntimeError, match="capture failed"):
        store.write(cast(StorageBackend, backend), run_id="failed")
    assert store.list_snapshots() == []
    assert not list((tmp_path / "archive").glob("*/manifest.json"))


def test_zero_row_tables_are_complete_manifest_entries(tmp_path: Path) -> None:
    """Record every empty table as captured rather than silently omitting it."""
    backend = _backend()
    store = SnapshotStore(f"file://{tmp_path}/archive")
    try:
        uri = store.write(backend, run_id="empty")
        assert uri is not None
        manifest = json.loads(
            (
                tmp_path / "archive" / uri.rsplit("/", 1)[-1] / "manifest.json"
            ).read_text()
        )
        assert set(manifest["tables"]) == set(SNAPSHOT_TABLES)
        assert all(
            spec["rows"] == 0 and spec["objects"] == []
            for spec in manifest["tables"].values()
        )
    finally:
        backend.close()


def test_interval_reservation_takeover_and_fencing(tmp_path: Path) -> None:
    """Skip within cadence, take over expired leases, and reject stale owners."""
    backend = _backend()
    store = SnapshotStore(f"file://{tmp_path}/archive", interval="1h")
    try:
        assert store.write(backend, run_id="first") is not None
        assert store.write(backend, run_id="second") is None
        catalog, generation = store._read_catalog()
        assert generation is None
        now = datetime.now(UTC)
        catalog["reservation"] = {
            "owner": "missing",
            "fence": 99,
            "expires_at": (now - timedelta(seconds=1)).isoformat(),
        }
        store._write_catalog(catalog, generation)
        claim = store._claim(now + timedelta(hours=1))
        assert claim is not None
        owner, fence = claim
        catalog, generation = store._read_catalog()
        catalog["reservation"] = {
            "owner": "new-owner",
            "fence": fence + 1,
            "expires_at": (now + timedelta(hours=2)).isoformat(),
        }
        store._write_catalog(catalog, generation)
        assert (
            store._publish({}, owner=owner, fence=fence, now=now + timedelta(hours=1))
            is None
        )
    finally:
        backend.close()


def test_gcs_catalog_rotation_uses_generation_safe_publication() -> None:
    """Keep only catalog-published snapshots while conditionally cleaning GCS data."""
    archive = MemoryGcsArchive()
    backend = TableBackend()
    store = SnapshotStore("gs://bucket/archive", max_snapshots=1, gcs_archive=archive)
    assert store.lifecycle_warnings()
    first = store.write(cast(StorageBackend, backend), run_id="one")
    second = store.write(cast(StorageBackend, backend), run_id="two")
    assert first is not None and second is not None
    assert store.list_snapshots() == [second.rsplit("/", 1)[-1]]
    assert all(first.rsplit("/", 1)[-1] not in name for name in archive.objects)


@pytest.mark.gcs_live
def test_live_gcs_generation_operations_use_only_the_dedicated_bucket() -> None:
    """Verify the official client against an isolated, deleted-after-test prefix."""
    if os.environ.get("USAGEBASSOON_GCS_LIVE") != "1":
        pytest.skip("set USAGEBASSOON_GCS_LIVE=1 to run the GCS integration test")
    archive = RealGcsArchive(
        "gs://usagebassoon-test-snapshots-gen-lang-client-0670612427/"
        f"usagebassoon-tests/{uuid4().hex}"
    )
    try:
        first = archive.write_bytes("probe", b"one", if_generation_match=0)
        assert archive.read_bytes("probe", generation=first.generation) == b"one"
        with pytest.raises(GcsPreconditionError):
            archive.write_bytes("probe", b"two", if_generation_match=0)
        catalog = archive.write_json_cas(
            "catalog.json", {"entries": []}, generation=None
        )
        assert archive.read_json("catalog.json") == (
            {"entries": []},
            catalog.generation,
        )
        archive.lifecycle_delete_warnings()
    finally:
        for object_ref in archive.list(""):
            archive.delete(
                archive.relative(object_ref.name), generation=object_ref.generation
            )
