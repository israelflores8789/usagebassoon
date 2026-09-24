# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_bucket_gcs.py — Offline GCS bucket and snapshot publication tests."""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier, Lock
from typing import cast, override

import pyarrow as pa
import pytest

from tests._snapshot_fakes import TableBackend
from usagebassoon import archiver as snapshot_archiver
from usagebassoon.archiver import SNAPSHOT_TABLES
from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.base import StorageBackend
from usagebassoon.buckets.base import SnapshotObject as GcsObject
from usagebassoon.buckets.base import SnapshotPreconditionError as GcsPreconditionError
from usagebassoon.buckets.gcs import GcsClient
from usagebassoon.buckets.gcs import GcsSnapshotBucket as GcsArchive


class _RecordingBucket:
    """Minimal bucket double that records whether an object operation was attempted."""

    lifecycle_rules: tuple[object, ...] = ()

    def __init__(self) -> None:
        """Create an empty operation record."""
        self.blob_calls = 0

    def blob(self, name: str, generation: int | None = None) -> object:
        """Record an attempted object lookup."""
        del name, generation
        self.blob_calls += 1
        raise AssertionError("unsafe archive names must not reach GCS")

    def reload(self, *, timeout: float) -> None:
        """Satisfy the GCS bucket protocol."""
        del timeout


class _RecordingClient:
    """Minimal client double that records whether listing was attempted."""

    def __init__(self) -> None:
        """Create a recording client and its bucket."""
        self.recording_bucket = _RecordingBucket()
        self.list_calls = 0
        self.list_timeout: float | None = None

    def bucket(self, bucket_name: str) -> _RecordingBucket:
        """Return the recording bucket."""
        del bucket_name
        return self.recording_bucket

    def list_blobs(
        self, bucket: _RecordingBucket, *, prefix: str, timeout: float
    ) -> tuple[object, ...]:
        """Record an attempted listing."""
        del bucket, prefix
        self.list_calls += 1
        self.list_timeout = timeout
        return ()


class MemoryGcsArchive:
    """A generation-aware in-memory GCS archive double."""

    def __init__(self) -> None:
        """Create an empty bucket-relative object store."""
        self.uri = "gs://bucket/archive"
        self.objects: dict[str, tuple[int, bytes]] = {}
        self.next_generation = 1
        self.catalog_writes = 0
        self.fail_catalog_write_at: int | None = None

    def relative(self, object_name: str) -> str:
        """Return the archive-relative object name."""
        return object_name.removeprefix("archive/")

    def read_bytes(self, relative_name: str, *, version: int | str) -> bytes:
        """Read an exact object generation."""
        current_generation, data = self.objects[f"archive/{relative_name}"]
        if current_generation != version:
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
        return GcsObject(relative_name, generation, len(payload), None)

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
        expected_version: int | str | None,
    ) -> GcsObject:
        """Write a JSON document with a generation compare-and-swap guard."""
        if relative_name == "catalog.json":
            self.catalog_writes += 1
            if self.catalog_writes == self.fail_catalog_write_at:
                raise RuntimeError("catalog publication failed")
        return self.write_bytes(
            relative_name,
            json.dumps(payload).encode(),
            if_generation_match=(
                0 if expected_version is None else cast(int, expected_version)
            ),
            content_type="application/json",
        )

    def delete(self, relative_name: str, *, version: int | str) -> None:
        """Delete an exact object generation."""
        name = f"archive/{relative_name}"
        current = self.objects.get(name)
        if current is None or current[0] != version:
            raise GcsPreconditionError("generation changed")
        del self.objects[name]

    def list(self, relative_prefix: str) -> tuple[GcsObject, ...]:
        """List immutable object metadata below a relative prefix."""
        prefix = f"archive/{relative_prefix}"
        return tuple(
            GcsObject(name.removeprefix("archive/"), generation, len(data), None)
            for name, (generation, data) in self.objects.items()
            if name.startswith(prefix)
        )

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Return an injectable lifecycle warning."""
        return ("a GCS Delete lifecycle rule could match the snapshot archive",)


@pytest.mark.parametrize(
    "unsafe_name",
    ["../private", "/absolute", "C:\\Users\\Alice", "\\\\server\\share", "a/../b"],
)
def test_gcs_archive_rejects_unsafe_names_before_provider_calls(
    unsafe_name: str,
) -> None:
    """Reject traversal and Windows names without contacting the GCS client."""
    client = _RecordingClient()
    archive = GcsArchive("gs://bucket/archive", client=cast(GcsClient, client))

    with pytest.raises(ValueError):
        archive.key(unsafe_name)
    with pytest.raises(ValueError):
        archive.relative(unsafe_name)
    with pytest.raises(ValueError):
        archive.read_bytes(unsafe_name, version=1)
    with pytest.raises(ValueError):
        archive.write_bytes(unsafe_name, b"payload")
    with pytest.raises(ValueError):
        archive.read_json(unsafe_name)
    with pytest.raises(ValueError):
        archive.write_json_cas(unsafe_name, {}, expected_version=None)
    with pytest.raises(ValueError):
        archive.list(unsafe_name)
    with pytest.raises(ValueError):
        archive.delete(unsafe_name, version=1)

    assert client.recording_bucket.blob_calls == 0
    assert client.list_calls == 0


def test_gcs_archive_uses_configured_request_timeout() -> None:
    """Apply the GCS timeout to a provider listing request."""
    client = _RecordingClient()
    archive = GcsArchive(
        "gs://bucket/archive", timeout_seconds=25.0, client=cast(GcsClient, client)
    )

    assert archive.list("") == ()
    assert client.list_timeout == 25.0


def test_gcs_reservation_expires_during_slow_snapshot() -> None:
    """Reject a stale publisher after another writer claims the expired lease."""
    archive = MemoryGcsArchive()
    store = SnapshotStore("gs://bucket/archive", gcs_bucket=archive)
    started = datetime(2026, 9, 23, tzinfo=UTC)
    first_fence = store._claim_for(archive, started, "slow-writer")

    assert first_fence is not None
    assert (
        store._claim_for(archive, started + timedelta(minutes=4), "contender") is None
    )
    second_fence = store._claim_for(
        archive, started + timedelta(minutes=5, seconds=1), "new-writer"
    )
    assert second_fence is not None and second_fence > first_fence
    assert (
        store._publish_for(
            archive,
            {},
            owner="slow-writer",
            fence=first_fence,
            now=started + timedelta(minutes=5, seconds=1),
        )
        is None
    )
    catalog, _ = archive.read_json("catalog.json")
    assert catalog is not None and catalog["entries"] == []


def test_gcs_reservation_can_be_renewed_before_expiry() -> None:
    """Keep the current writer's fence while extending its reservation."""
    archive = MemoryGcsArchive()
    store = SnapshotStore("gs://bucket/archive", gcs_bucket=archive)
    started = datetime(2026, 9, 23, tzinfo=UTC)
    fence = store._claim_for(archive, started, "slow-writer")

    assert fence is not None
    assert store._renew_for(
        archive, "slow-writer", fence, started + timedelta(minutes=4)
    )
    assert (
        store._claim_for(
            archive, started + timedelta(minutes=5, seconds=1), "contender"
        )
        is None
    )
    assert store._publish_for(
        archive,
        {"snapshot_id": "long-capture"},
        owner="slow-writer",
        fence=fence,
        now=started + timedelta(minutes=6),
    )


def test_snapshot_renews_reservation_during_long_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Publish a capture that lasts longer than one reservation period."""

    class SlowBackend(TableBackend):
        """Make each canonical table capture take measurable time."""

        @override
        def query(self, sql: str) -> pa.Table:
            """Pause before returning one table."""
            time.sleep(0.2)
            return super().query(sql)

    monkeypatch.setattr(snapshot_archiver, "_LEASE_SECONDS", 1)
    archive = MemoryGcsArchive()
    store = SnapshotStore("gs://bucket/archive", gcs_bucket=archive)
    backend = SlowBackend()

    published = store.write(cast(StorageBackend, backend), run_id="slow")

    assert published is not None
    assert backend.queries == len(SNAPSHOT_TABLES)
    assert store.list_snapshots() == [published.rsplit("/", 1)[-1]]


def test_simultaneous_gcs_catalog_claim_has_one_winner() -> None:
    """Give one writer the reservation when both read an absent catalog."""

    class RacingArchive(MemoryGcsArchive):
        """Coordinate reads while preserving an atomic provider-side write."""

        def __init__(self) -> None:
            """Create a shared archive with a two-writer claim barrier."""
            super().__init__()
            self.read_barrier = Barrier(2)
            self.write_lock = Lock()

        @override
        def read_json(
            self, relative_name: str
        ) -> tuple[dict[str, object] | None, int | None]:
            """Make both claimants observe the same absent catalog version."""
            result = super().read_json(relative_name)
            if relative_name == "catalog.json" and result[0] is None:
                self.read_barrier.wait(timeout=5)
            return result

        @override
        def write_json_cas(
            self,
            relative_name: str,
            payload: dict[str, object],
            *,
            expected_version: int | str | None,
        ) -> GcsObject:
            """Apply one generation check and write at a time."""
            with self.write_lock:
                return super().write_json_cas(
                    relative_name, payload, expected_version=expected_version
                )

    archive = RacingArchive()
    store = SnapshotStore("gs://bucket/archive", gcs_bucket=archive)
    started = datetime(2026, 9, 23, tzinfo=UTC)

    def claim(owner: str) -> int | None:
        """Try to reserve the shared archive for one writer."""
        return store._claim_for(archive, started, owner)

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = tuple(executor.map(claim, ("first", "second")))

    assert sum(fence is not None for fence in claims) == 1
    catalog, _ = archive.read_json("catalog.json")
    assert catalog is not None
    reservation = catalog["reservation"]
    assert isinstance(reservation, dict)
    assert reservation["owner"] in {"first", "second"}


def test_gcs_catalog_rotation_uses_generation_safe_publication() -> None:
    """Keep only catalog-published snapshots while conditionally cleaning GCS data."""
    archive = MemoryGcsArchive()
    backend = TableBackend()
    store = SnapshotStore("gs://bucket/archive", max_snapshots=1, gcs_bucket=archive)
    assert store.lifecycle_warnings()
    first = store.write(cast(StorageBackend, backend), run_id="one")
    second = store.write(cast(StorageBackend, backend), run_id="two")
    assert first is not None and second is not None
    assert store.list_snapshots() == [second.rsplit("/", 1)[-1]]
    assert all(first.rsplit("/", 1)[-1] not in name for name in archive.objects)


def test_dual_destinations_capture_once_and_publish_the_same_snapshot(
    tmp_path: Path,
) -> None:
    """Write matching complete archives locally and to GCS from one capture."""
    archive = MemoryGcsArchive()
    backend = TableBackend()
    store = SnapshotStore(
        file_uri=f"file://{tmp_path}/local",
        gcs_archive_uri="gs://bucket/archive",
        gcs_bucket=archive,
    )

    local_uri = store.write(cast(StorageBackend, backend), run_id="dual")

    assert local_uri is not None
    assert backend.queries == len(SNAPSHOT_TABLES)
    snapshot_id = local_uri.rsplit("/", 1)[-1]
    local_catalog = json.loads((tmp_path / "local" / "catalog.json").read_text())
    gcs_catalog, _ = archive.read_json("catalog.json")
    assert gcs_catalog is not None
    gcs_entries = cast(list[dict[str, object]], gcs_catalog["entries"])
    assert [entry["snapshot_id"] for entry in local_catalog["entries"]] == [snapshot_id]
    assert [entry["snapshot_id"] for entry in gcs_entries] == [snapshot_id]
    assert local_catalog["entries"][0]["published_at"] == gcs_entries[0]["published_at"]

    local_manifest = json.loads(
        (tmp_path / "local" / snapshot_id / "manifest.json").read_text()
    )
    gcs_manifest_ref = cast(dict[str, object], gcs_entries[0]["manifest"])
    gcs_manifest_name = cast(str, gcs_manifest_ref["name"])
    gcs_manifest_generation = cast(int, gcs_manifest_ref["version"])
    gcs_manifest = json.loads(
        archive.read_bytes(gcs_manifest_name, version=gcs_manifest_generation)
    )
    assert local_manifest["snapshot_id"] == gcs_manifest["snapshot_id"]
    assert local_manifest["schema_fingerprint"] == gcs_manifest["schema_fingerprint"]
    for table in SNAPSHOT_TABLES:
        local_spec = local_manifest["tables"][table]
        gcs_spec = gcs_manifest["tables"][table]
        for field in ("status", "completed_at", "rows", "schema", "schema_ipc"):
            assert local_spec[field] == gcs_spec[field]
        assert len(local_spec["objects"]) == len(gcs_spec["objects"])
        for local_object, gcs_object in zip(
            local_spec["objects"], gcs_spec["objects"], strict=True
        ):
            assert local_object["name"] == gcs_object["name"]
            assert (tmp_path / "local" / local_object["name"]).read_bytes() == (
                archive.read_bytes(gcs_object["name"], version=gcs_object["version"])
            )


def test_dual_publication_failure_does_not_prune_previous_snapshots(
    tmp_path: Path,
) -> None:
    """Retain the previous catalog entry when the second publication fails."""
    archive = MemoryGcsArchive()
    backend = TableBackend()
    store = SnapshotStore(
        file_uri=f"file://{tmp_path}/local",
        gcs_archive_uri="gs://bucket/archive",
        gcs_bucket=archive,
        max_snapshots=1,
    )

    first = store.write(cast(StorageBackend, backend), run_id="first")
    assert first is not None
    first_id = first.rsplit("/", 1)[-1]
    archive.fail_catalog_write_at = archive.catalog_writes + 2

    with pytest.raises(RuntimeError, match="catalog publication failed"):
        store.write(cast(StorageBackend, backend), run_id="second")

    assert store.list_snapshots() == [first_id]
    gcs_catalog, _ = archive.read_json("catalog.json")
    assert gcs_catalog is not None
    gcs_entries = cast(list[dict[str, object]], gcs_catalog["entries"])
    assert [entry["snapshot_id"] for entry in gcs_entries] == [first_id]
