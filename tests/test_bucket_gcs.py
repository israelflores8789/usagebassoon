# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_bucket_gcs.py — Offline GCS bucket and snapshot publication tests."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event
from typing import cast, override

import pyarrow as pa
import pytest

from tests._snapshot_fakes import TableBackend
from usagebassoon.archiver import SNAPSHOT_TABLES
from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.base import StorageBackend
from usagebassoon.buckets.base import SnapshotObject as GcsObject
from usagebassoon.buckets.base import SnapshotPreconditionError as GcsPreconditionError
from usagebassoon.buckets.gcs import GcsClient
from usagebassoon.buckets.gcs import GcsSnapshotBucket as GcsArchive
from usagebassoon.snapshot.catalog import ArchiveBusy, Catalog


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

    def upload_file(self, relative_name: str, path: Path) -> GcsObject:
        """Simulate a create-only file upload."""
        return self.write_bytes(relative_name, path.read_bytes(), if_generation_match=0)

    def download_file(self, relative_name: str, path: Path) -> GcsObject:
        """Resolve this copy's current provider generation before downloading."""
        generation, payload = self.objects[f"archive/{relative_name}"]
        path.write_bytes(payload)
        return GcsObject(relative_name, generation, len(payload), None)


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
    """Reject a stale publisher after another writer replaces an expired claim."""
    archive = MemoryGcsArchive()
    first, second = Catalog(archive), Catalog(archive)
    first._update_reservation(claim=True)
    document, version = first.control()
    reservation = document["reservation"]
    assert isinstance(reservation, dict)
    reservation["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    archive.write_json_cas("control.json", document, expected_version=version)
    second._update_reservation(claim=True)
    assert (
        first.fence is not None
        and second.fence is not None
        and second.fence > first.fence
    )
    with pytest.raises(ArchiveBusy, match="lost"):
        first.publish("stale", datetime.now(UTC).isoformat())
    assert first.entries() == []


def test_gcs_reservation_can_be_renewed_before_expiry() -> None:
    """Retain the same fence during renewal and block competing mutations."""
    archive = MemoryGcsArchive()
    with Catalog(archive).hold() as catalog:
        fence = catalog.fence
        catalog.check()
        assert catalog.fence == fence
        with pytest.raises(ArchiveBusy), Catalog(archive).hold():
            pytest.fail("second archive owner entered")


def test_snapshot_renews_reservation_during_long_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Renew a claimed archive while canonical capture waits for the heartbeat."""
    import usagebassoon.snapshot.catalog as catalog_module

    renewed = Event()
    capturing = Event()
    original = Catalog._update_reservation
    monkeypatch.setattr(catalog_module, "LEASE_SECONDS", 1)

    def renew(self: Catalog, *, claim: bool = False) -> None:
        original(self, claim=claim)
        if capturing.is_set() and not claim:
            renewed.set()

    class WaitingBackend(TableBackend):
        @override
        def query(self, sql: str) -> pa.Table:
            capturing.set()
            assert renewed.wait(timeout=5)
            return super().query(sql)

    monkeypatch.setattr(Catalog, "_update_reservation", renew)
    archive = MemoryGcsArchive()
    backend = WaitingBackend()
    store = SnapshotStore(archive.uri, gcs_bucket=archive)
    assert store.write(cast(StorageBackend, backend), run_id="waiting") is not None
    assert renewed.is_set()
    assert backend.queries == len(SNAPSHOT_TABLES)


def test_simultaneous_gcs_catalog_claim_has_one_winner() -> None:
    """Compare-and-swap lets only one writer claim the same missing catalog."""
    archive = MemoryGcsArchive()
    barrier = Barrier(2)
    claimants = [Catalog(archive), Catalog(archive)]

    def claim(catalog: Catalog) -> bool:
        barrier.wait(timeout=5)
        try:
            catalog._update_reservation(claim=True)
        except ArchiveBusy:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        winners = list(executor.map(claim, claimants))
    assert sum(winners) == 1


def test_gcs_catalog_rotation_uses_generation_safe_publication() -> None:
    """Retire objects at exact generations while preserving a lifecycle tombstone."""
    archive = MemoryGcsArchive()
    store = SnapshotStore(archive.uri, max_snapshots=1, gcs_bucket=archive)
    backend = TableBackend()
    first = store.write(cast(StorageBackend, backend), run_id="one")
    second = store.write(cast(StorageBackend, backend), run_id="two")
    assert first is not None and second is not None
    assert store.list_snapshots() == [second.rsplit("/", 1)[-1]]
    retired = first.rsplit("/", 1)[-1]
    assert [obj.name for obj in archive.list(retired)] == [f"{retired}/state.json"]
    state, _ = archive.read_json(f"{retired}/state.json")
    assert state is not None and state["retired"] is True


def test_dual_destinations_capture_once_and_publish_the_same_snapshot(
    tmp_path: Path,
) -> None:
    """Portable immutable manifests have identical bytes at every destination."""
    archive = MemoryGcsArchive()
    backend = TableBackend()
    local = tmp_path / "local"
    store = SnapshotStore(
        file_uri=str(local), gcs_archive_uri=archive.uri, gcs_bucket=archive
    )
    uri = store.write(cast(StorageBackend, backend), run_id="dual")
    assert uri is not None
    identifier = uri.rsplit("/", 1)[-1]
    assert backend.queries == len(SNAPSHOT_TABLES)
    raw = (local / identifier / "manifest.json").read_bytes()
    manifest, _ = archive.read_json(f"{identifier}/manifest.json")
    assert json.loads(raw) == manifest
    for name in ("manifest.json", "COMPLETE", "notes.parquet"):
        obj = next(
            o for o in archive.list(identifier) if o.name == f"{identifier}/{name}"
        )
        assert (
            archive.read_bytes(obj.name, version=obj.version)
            == (local / identifier / name).read_bytes()
        )


def test_pending_destination_renews_during_slow_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain every destination reservation until all copies finish publication."""
    archive = MemoryGcsArchive()
    store = SnapshotStore(
        file_uri=str(tmp_path / "local"),
        gcs_archive_uri=archive.uri,
        gcs_bucket=archive,
    )
    original = Catalog.publish

    def publish(self: Catalog, identifier: str, captured_at: str) -> None:
        original(self, identifier, captured_at)
        for bucket in store.reader.buckets:
            document, _ = bucket.read_json("control.json")
            assert document is not None and isinstance(document["reservation"], dict)

    monkeypatch.setattr(Catalog, "publish", publish)
    assert store.write(cast(StorageBackend, TableBackend()), run_id="dual") is not None


def test_dual_publication_failure_does_not_prune_previous_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not rotate either destination after an incomplete dual publication."""
    archive = MemoryGcsArchive()
    store = SnapshotStore(
        file_uri=str(tmp_path / "local"),
        gcs_archive_uri=archive.uri,
        gcs_bucket=archive,
        max_snapshots=1,
    )
    first = store.write(cast(StorageBackend, TableBackend()), run_id="first")
    assert first is not None
    original = Catalog.publish

    def fail(self: Catalog, identifier: str, captured_at: str) -> None:
        if self.bucket is archive:
            raise RuntimeError("catalog publication failed")
        original(self, identifier, captured_at)

    monkeypatch.setattr(Catalog, "publish", fail)
    with pytest.raises(RuntimeError, match="catalog publication failed"):
        store.write(cast(StorageBackend, TableBackend()), run_id="second")
    candidates, _ = store.reader.candidates()
    # A retired rollback entry must never be eligible for recovery.
    with store.reader.prepare() as prepared:
        assert prepared.candidate.identifier == first.rsplit("/", 1)[-1]
    assert any(c.identifier == first.rsplit("/", 1)[-1] for c in candidates)
