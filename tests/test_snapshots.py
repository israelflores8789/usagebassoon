# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_snapshots.py — Local snapshot catalog, rotation, and restore tests."""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from threading import Barrier
from typing import cast

import pyarrow as pa
import pytest

from tests._snapshot_fakes import TableBackend
from usagebassoon.archiver import SNAPSHOT_TABLES
from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.buckets.base import SnapshotPreconditionError
from usagebassoon.buckets.local import LocalSnapshotBucket


def _backend() -> DuckDBBackend:
    """Create an initialized temporary DuckDB archive source."""
    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()
    return backend


def _append_note(backend: DuckDBBackend) -> None:
    """Insert one restorable row so a snapshot includes a Parquet object."""
    captured_at = datetime(2026, 9, 19, tzinfo=UTC)
    backend.append(
        "notes",
        pa.table(
            {
                "source_id": ["11111111-1111-4111-8111-111111111111"],
                "client": ["codex"],
                "session_id": ["private-session"],
                "note": ["private note"],
                "created_at": [captured_at],
                "updated_at": [captured_at],
            }
        ),
    )


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


def test_snapshot_catalog_rejects_traversal_ids_and_local_cleanup(
    tmp_path: Path,
) -> None:
    """Reject catalog traversal before it can influence local deletion."""
    archive = tmp_path / "archive"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel.txt").write_text("keep")
    backend = _backend()
    store = SnapshotStore(f"file://{archive}")
    try:
        snapshot = store.write(backend, run_id="safe")
        assert snapshot is not None
        catalog, generation = store._read_catalog()
        entries = cast(list[dict[str, object]], catalog["entries"])
        entries[0]["snapshot_id"] = "../../outside"
        store._write_catalog(catalog, generation)

        with pytest.raises(ValueError, match="invalid snapshot identifier"):
            store.list_snapshots()
        with pytest.raises(ValueError, match="invalid snapshot identifier"):
            store._cleanup_entry({"snapshot_id": "../../outside"})
        assert (outside / "sentinel.txt").read_text() == "keep"
    finally:
        backend.close()


def test_restore_rejects_traversal_manifest_and_object_names(tmp_path: Path) -> None:
    """Constrain catalog and manifest references to their snapshot directory."""
    archive = tmp_path / "archive"
    source = _backend()
    target = _backend()
    _append_note(source)
    store = SnapshotStore(f"file://{archive}")
    try:
        uri = store.write(source, run_id="safe")
        assert uri is not None
        snapshot_id = uri.rsplit("/", 1)[-1]
        catalog_path = archive / "catalog.json"
        catalog = json.loads(catalog_path.read_text())
        entry = catalog["entries"][0]
        entry["manifest"]["name"] = "../../private-file"
        catalog_path.write_text(json.dumps(catalog))
        with pytest.raises(ValueError, match="unsafe component"):
            store.restore(target, snapshot_id)

        entry["manifest"]["name"] = f"{snapshot_id}/manifest.json"
        catalog_path.write_text(json.dumps(catalog))

        uri = store.write(source, run_id="second")
        assert uri is not None
        snapshot_id = uri.rsplit("/", 1)[-1]
        catalog = json.loads(catalog_path.read_text())
        entry = catalog["entries"][-1]
        manifest_path = archive / snapshot_id / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["tables"]["notes"]["objects"][0]["name"] = "../../private-file"
        raw = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
        manifest_path.write_bytes(raw)
        entry["manifest"]["size"] = len(raw)
        entry["manifest"]["sha256"] = sha256(raw).hexdigest()
        catalog_path.write_text(json.dumps(catalog))
        with pytest.raises(ValueError, match="unsafe component"):
            store.restore(target, snapshot_id)
    finally:
        source.close()
        target.close()


def test_restore_verifies_manifest_and_object_sha256(tmp_path: Path) -> None:
    """Reject corrupt bytes before JSON or Parquet parsing can consume them."""
    archive = tmp_path / "archive"
    source = _backend()
    target = _backend()
    _append_note(source)
    store = SnapshotStore(f"file://{archive}")
    try:
        uri = store.write(source, run_id="safe")
        assert uri is not None
        snapshot_id = uri.rsplit("/", 1)[-1]
        object_path = archive / snapshot_id / "notes.parquet"
        object_path.write_bytes(b"x" * len(object_path.read_bytes()))
        with pytest.raises(ValueError, match="SHA-256"):
            store.restore(target, snapshot_id)

        uri = store.write(source, run_id="second")
        assert uri is not None
        snapshot_id = uri.rsplit("/", 1)[-1]
        manifest_path = archive / snapshot_id / "manifest.json"
        manifest_path.write_bytes(b"x" * len(manifest_path.read_bytes()))
        with pytest.raises(ValueError, match="SHA-256"):
            store.restore(target, snapshot_id)
    finally:
        source.close()
        target.close()


def test_snapshot_store_restore_requires_an_empty_backend(tmp_path: Path) -> None:
    """Enforce empty destinations for direct library callers as well as the CLI."""
    backend = _backend()
    _append_note(backend)
    store = SnapshotStore(f"file://{tmp_path}/archive")
    try:
        uri = store.write(backend, run_id="safe")
        assert uri is not None
        with pytest.raises(ValueError, match="restore requires an empty warehouse"):
            store.restore(backend)
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
        assert isinstance(generation, str)
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


def test_local_catalog_claim_has_one_winner_under_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Allow only one writer to claim a previously absent local catalog."""
    archive = LocalSnapshotBucket(f"file://{tmp_path}/archive")
    barrier = Barrier(2)
    original_replace = Path.replace

    def replace_slowly(path: Path, target: Path) -> Path:
        """Keep the first catalog replacement in flight during contention."""
        if path.name.startswith(".catalog.json."):
            time.sleep(0.05)
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", replace_slowly)

    def claim(owner: str) -> bool:
        """Return whether a writer claimed the same absent catalog."""
        barrier.wait(timeout=5)
        try:
            archive.write_json_cas(
                "catalog.json", {"owner": owner}, expected_version=None
            )
        except SnapshotPreconditionError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        winners = tuple(executor.map(claim, ("first", "second")))

    assert sum(winners) == 1
