# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_snapshots.py — Catalog, cadence, local rotation, and GCS archive tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from usagebassoon.archiver import SNAPSHOT_TABLES
from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.buckets.base import (
    SnapshotObject as GcsObject,
)
from usagebassoon.buckets.base import (
    SnapshotPreconditionError as GcsPreconditionError,
)
from usagebassoon.buckets.gcs import GcsClient
from usagebassoon.buckets.gcs import GcsSnapshotBucket as GcsArchive


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
