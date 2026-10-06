# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_bucket_gcs.py — Offline GCS bucket and snapshot publication tests."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event, Lock
from typing import cast, override
from unittest.mock import ANY, MagicMock

import pyarrow as pa
import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed, ServiceUnavailable

from tests._snapshot_fakes import TableBackend
from usagebassoon.archiver import SNAPSHOT_TABLES
from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.base import StorageBackend
from usagebassoon.buckets.base import SnapshotObject as GcsObject
from usagebassoon.buckets.base import SnapshotPreconditionError as GcsPreconditionError
from usagebassoon.buckets.gcs import GcsBlob, GcsBucket, GcsClient
from usagebassoon.buckets.gcs import GcsSnapshotBucket as GcsArchive
from usagebassoon.snapshot.catalog import ArchiveBusy, Catalog


def test_gcs_transfer_and_metadata_consume_the_same_operation_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transfer's metadata request cannot receive a fresh snapshot timeout."""
    import usagebassoon.deadlines as deadlines

    clock = [0.0]
    monkeypatch.setattr(deadlines, "monotonic", lambda: clock[0])
    client = MagicMock(spec=GcsClient)
    blob = client.bucket.return_value.blob.return_value
    blob.name = "archive/table.parquet"
    blob.generation, blob.size, blob.crc32c = 7, 3, "checksum"
    archive = GcsArchive("gs://bucket/archive", client=cast(GcsClient, client))
    path = tmp_path / "table.parquet"
    path.write_bytes(b"abc")

    def upload(
        _path: str, *, if_generation_match: int, timeout: float, retry: object
    ) -> None:
        assert if_generation_match == 0 and retry is None and timeout == 10
        clock[0] = 8

    blob.upload_from_filename.side_effect = upload
    with deadlines.operation(10):
        archive.upload_file("table.parquet", path)
    blob.reload.assert_called_once_with(timeout=2, retry=None)


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
        self, bucket: _RecordingBucket, *, prefix: str, timeout: float, retry: object
    ) -> tuple[object, ...]:
        """Record an attempted listing."""
        del bucket, prefix
        assert retry is None
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
    assert client.list_timeout is not None and 0 < client.list_timeout <= 25.0


@pytest.mark.parametrize(
    "operation", ["read", "json", "create", "replace", "upload", "download", "delete"]
)
def test_gcs_adapter_passes_generation_guards_and_timeouts(
    tmp_path: Path, operation: str
) -> None:
    """Exercise the actual adapter's SDK boundary rather than an archive double."""
    client = MagicMock(spec=GcsClient)
    bucket = MagicMock(spec=GcsBucket)
    blob = MagicMock(spec=GcsBlob)
    client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    blob.name = "archive/copy/data.parquet"
    blob.generation, blob.size, blob.crc32c = 7, 3, "checksum"
    archive = GcsArchive(
        "gs://bucket/archive", timeout_seconds=17, client=cast(GcsClient, client)
    )
    path = tmp_path / "data.parquet"
    path.write_bytes(b"abc")
    if operation in {"read", "json"}:
        if operation == "json":
            blob.download_as_bytes.return_value = b'{"value": 42}'
            assert archive.read_json("copy/data.parquet") == ({"value": 42}, 7)
            bucket.blob.assert_any_call("archive/copy/data.parquet", generation=7)
            blob.reload.assert_called_once_with(timeout=ANY, retry=None)
        else:
            blob.download_as_bytes.return_value = b"abc"
            assert archive.read_bytes("copy/data.parquet", version=7) == b"abc"
            bucket.blob.assert_called_once_with(
                "archive/copy/data.parquet", generation=7
            )
        blob.download_as_bytes.assert_called_once_with(
            if_generation_match=7, timeout=ANY, retry=None
        )
    elif operation in {"create", "replace"}:
        reference = archive.write_json_cas(
            "copy/data.parquet",
            {"value": 42},
            expected_version=None if operation == "create" else 6,
        )
        args, kwargs = blob.upload_from_string.call_args
        assert json.loads(args[0]) == {"value": 42}
        assert kwargs == {
            "content_type": "application/json",
            "if_generation_match": 0 if operation == "create" else 6,
            "timeout": ANY,
            "retry": None,
        }
        assert reference.version == 7
    elif operation == "upload":
        reference = archive.upload_file("copy/data.parquet", path)
        blob.upload_from_filename.assert_called_once_with(
            str(path), if_generation_match=0, timeout=ANY, retry=None
        )
        assert reference == GcsObject("copy/data.parquet", 7, 3, "checksum")
    elif operation == "download":

        def reload(*, timeout: float, retry: object) -> None:
            assert 0 < timeout <= 17
            assert retry is None
            blob.generation = 8

        blob.reload.side_effect = reload
        reference = archive.download_file("copy/data.parquet", path)
        blob.download_to_filename.assert_called_once_with(
            str(path), if_generation_match=8, timeout=ANY, retry=None
        )
        assert reference.version == 8
    else:
        archive.delete("copy/data.parquet", version=7)
        blob.delete.assert_called_once_with(
            if_generation_match=7, timeout=ANY, retry=None
        )


@pytest.mark.parametrize("operation", ["read", "delete", "replace"])
@pytest.mark.parametrize(
    "error_type", [NotFound, PreconditionFailed, ServiceUnavailable]
)
def test_gcs_adapter_distinguishes_generation_conflicts_from_service_failures(
    operation: str, error_type: type[Exception]
) -> None:
    """Translate stale generations while preserving unrelated provider failures."""
    client = MagicMock(spec=GcsClient)
    blob = client.bucket.return_value.blob.return_value
    failure = error_type("controlled provider failure")
    method = {
        "read": "download_as_bytes",
        "delete": "delete",
        "replace": "upload_from_string",
    }[operation]
    getattr(blob, method).side_effect = failure
    archive = GcsArchive("gs://bucket/archive", client=cast(GcsClient, client))
    conflict = error_type is PreconditionFailed or (
        error_type is NotFound and operation != "replace"
    )
    expected = GcsPreconditionError if conflict else error_type
    with pytest.raises(expected) as caught:
        if operation == "read":
            archive.read_bytes("copy/data.parquet", version=7)
        elif operation == "delete":
            archive.delete("copy/data.parquet", version=7)
        else:
            archive.write_json_cas("control.json", {}, expected_version=7)
    if conflict:
        assert caught.value.__cause__ is failure
    else:
        assert caught.value is failure


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
    store = SnapshotStore(archive.uri, buckets=(archive,))
    assert store.write(cast(StorageBackend, backend), run_id="waiting") is not None
    assert renewed.is_set()
    assert backend.queries == len(SNAPSHOT_TABLES)


def test_simultaneous_gcs_catalog_claim_has_one_winner() -> None:
    """Force both claimants to observe one generation before atomic provider CAS."""
    barrier = Barrier(2)

    class RacingArchive(MemoryGcsArchive):
        def __init__(self) -> None:
            super().__init__()
            self.lock = Lock()

        @override
        def read_json(
            self, relative_name: str
        ) -> tuple[dict[str, object] | None, int | None]:
            result = super().read_json(relative_name)
            if relative_name == "control.json" and result[0] is None:
                barrier.wait(timeout=5)
            return result

        @override
        def write_json_cas(
            self,
            relative_name: str,
            payload: dict[str, object],
            *,
            expected_version: str | int | None,
        ) -> GcsObject:
            with self.lock:
                return super().write_json_cas(
                    relative_name, payload, expected_version=expected_version
                )

    archive = RacingArchive()
    claimants = [Catalog(archive), Catalog(archive)]

    def claim(catalog: Catalog) -> bool:
        try:
            catalog._update_reservation(claim=True)
        except ArchiveBusy:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        winners = list(executor.map(claim, claimants))
    assert sum(winners) == 1


def test_gcs_catalog_rotation_uses_generation_safe_publication() -> None:
    """Retire exact generations and forget completed cleanup."""
    archive = MemoryGcsArchive()
    store = SnapshotStore(archive.uri, max_snapshots=1, buckets=(archive,))
    backend = TableBackend()
    first = store.write(cast(StorageBackend, backend), run_id="one")
    second = store.write(cast(StorageBackend, backend), run_id="two")
    assert first is not None and second is not None
    assert store.list_snapshots() == [second.rsplit("/", 1)[-1]]
    retired = first.rsplit("/", 1)[-1]
    assert archive.list(retired) == ()
    assert Catalog(archive).state(retired) is None


def test_dual_destinations_capture_once_and_publish_the_same_snapshot(
    tmp_path: Path,
) -> None:
    """Portable immutable manifests have identical bytes at every destination."""
    archive = MemoryGcsArchive()
    backend = TableBackend()
    local = tmp_path / "local"
    store = SnapshotStore(
        destination_uris=(str(local), archive.uri), buckets=(archive,)
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
        destination_uris=(str(tmp_path / "local"), archive.uri),
        buckets=(archive,),
    )
    import usagebassoon.snapshot.catalog as module

    monkeypatch.setattr(module, "LEASE_SECONDS", 1)
    original = Catalog.publish
    original_renew = Catalog._update_reservation
    published = Event()
    renewed = Event()

    def renew(self: Catalog, *, claim: bool = False) -> None:
        original_renew(self, claim=claim)
        if self.bucket is archive and published.is_set() and not claim:
            renewed.set()

    def publish(self: Catalog, identifier: str, captured_at: str) -> None:
        original(self, identifier, captured_at)
        if self.bucket is not archive:
            published.set()
            assert renewed.wait(5), "pending remote destination did not renew"
        for bucket in store.reader.buckets:
            document, _ = bucket.read_json("control.json")
            assert document is not None and isinstance(document["reservation"], dict)

    monkeypatch.setattr(Catalog, "_update_reservation", renew)
    monkeypatch.setattr(Catalog, "publish", publish)
    assert store.write(cast(StorageBackend, TableBackend()), run_id="dual") is not None
    assert renewed.is_set()


def test_dual_publication_failure_does_not_prune_previous_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not rotate either destination after an incomplete dual publication."""
    archive = MemoryGcsArchive()
    store = SnapshotStore(
        destination_uris=(str(tmp_path / "local"), archive.uri),
        buckets=(archive,),
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
    # Both destinations preserve their prior recovery point after rollback.
    with store.reader.prepare() as prepared:
        assert prepared.candidate.identifier == first.rsplit("/", 1)[-1]
    assert len(candidates) == 2
    assert all(c.identifier == first.rsplit("/", 1)[-1] for c in candidates)
    for bucket in store.reader.buckets:
        assert Catalog(bucket).entries() == [
            {
                "snapshot_id": first.rsplit("/", 1)[-1],
                "captured_at": candidates[0].captured_at,
            }
        ]
        assert any(
            obj.name == first.rsplit("/", 1)[-1] + "/COMPLETE"
            for obj in bucket.list("")
        )


@pytest.mark.parametrize("project", ["ci-test-project", None])
def test_gcs_live_project_uses_ci_variable_without_adc_project_discovery(
    monkeypatch: pytest.MonkeyPatch,
    project: str | None,
) -> None:
    """WIF CI selects its bucket project explicitly, without a local ADC project."""
    import google.auth

    from tests.test_bucket_gcs_live import _test_project

    def no_adc_project(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("CI must not discover a project from local ADC")

    monkeypatch.setenv("CI", "true")
    monkeypatch.setattr(google.auth, "default", no_adc_project)
    if project is None:
        monkeypatch.delenv("USAGEBASSOON_GCS_PROJECT", raising=False)
        with pytest.raises(
            pytest.fail.Exception, match="USAGEBASSOON_GCS_PROJECT is required"
        ):
            _test_project()
    else:
        monkeypatch.setenv("USAGEBASSOON_GCS_PROJECT", project)
        assert _test_project() == project
