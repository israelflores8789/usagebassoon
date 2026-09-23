# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_gcs_live.py — Opt-in live GCS snapshot integration tests.

Run with ``USAGEBASSOON_GCS_LIVE=1 uv run pytest -m gcs_live``.

The ``gcs_live`` marker selects these tests, and ``USAGEBASSOON_GCS_LIVE=1``
enables access to a preconfigured GCS test bucket.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

import pyarrow as pa
import pytest

from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.buckets.base import SnapshotPreconditionError as GcsPreconditionError
from usagebassoon.buckets.gcs import GcsSnapshotBucket as GcsArchive

pytestmark = pytest.mark.gcs_live

_TEST_BUCKET = "usagebassoon-test-snapshots-gen-lang-client-0670612427"


@pytest.fixture
def live_archive() -> Generator[GcsArchive]:
    """Create and remove an isolated prefix in the dedicated GCS test bucket."""
    if os.environ.get("USAGEBASSOON_GCS_LIVE") != "1":
        pytest.skip("set USAGEBASSOON_GCS_LIVE=1 to run GCS integration tests")
    archive = GcsArchive(f"gs://{_TEST_BUCKET}/usagebassoon-tests/{uuid4().hex}")
    try:
        yield archive
    finally:
        for object_ref in archive.list(""):
            archive.delete(object_ref.name, version=object_ref.version)


def _backend() -> DuckDBBackend:
    """Create an initialized ephemeral DuckDB backend."""
    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()
    return backend


def _append_note(backend: DuckDBBackend) -> None:
    """Insert one row so the snapshot contains a Parquet object."""
    captured_at = datetime(2026, 9, 19, tzinfo=UTC)
    backend.append(
        "notes",
        pa.table(
            {
                "source_id": ["11111111-1111-4111-8111-111111111111"],
                "client": ["codex"],
                "session_id": ["live-gcs-session"],
                "note": ["live GCS snapshot"],
                "created_at": [captured_at],
                "updated_at": [captured_at],
            }
        ),
    )


def _store(archive: GcsArchive) -> SnapshotStore:
    """Create a snapshot store backed by one live archive."""
    return SnapshotStore(archive.uri, gcs_bucket=archive)


def test_live_gcs_generation_operations(live_archive: GcsArchive) -> None:
    """Verify official-client conditional object and catalog operations."""
    first = live_archive.write_bytes("probe", b"one", if_generation_match=0)
    assert live_archive.read_bytes("probe", version=first.version) == b"one"
    with pytest.raises(GcsPreconditionError):
        live_archive.write_bytes("probe", b"two", if_generation_match=0)
    catalog = live_archive.write_json_cas(
        "catalog.json", {"entries": []}, expected_version=None
    )
    assert live_archive.read_json("catalog.json") == (
        {"entries": []},
        catalog.version,
    )
    live_archive.lifecycle_warnings()


def test_live_gcs_snapshot_round_trip_verifies_downloaded_references(
    live_archive: GcsArchive,
) -> None:
    """Restore a real GCS snapshot after verifying manifest and Parquet SHA-256."""
    source = _backend()
    destination = _backend()
    _append_note(source)
    try:
        store = _store(live_archive)
        snapshot = store.write(source, run_id="live-round-trip")
        assert snapshot is not None

        restored = store.restore(destination)

        assert restored["notes"] == 1
        assert destination.query("SELECT * FROM notes").to_pylist() == [
            {
                "source_id": "11111111-1111-4111-8111-111111111111",
                "client": "codex",
                "session_id": "live-gcs-session",
                "note": "live GCS snapshot",
                "created_at": datetime(2026, 9, 19, tzinfo=UTC),
                "updated_at": datetime(2026, 9, 19, tzinfo=UTC),
            }
        ]
    finally:
        source.close()
        destination.close()


def test_live_gcs_restore_rejects_replaced_object_generation(
    live_archive: GcsArchive,
) -> None:
    """Fail before appending when a cataloged GCS object has been replaced."""
    source = _backend()
    destination = _backend()
    _append_note(source)
    try:
        store = _store(live_archive)
        snapshot = store.write(source, run_id="live-replacement")
        assert snapshot is not None
        catalog, _ = store._read_catalog()
        entry = cast(dict[str, object], cast(list[object], catalog["entries"])[0])
        manifest = store._load_manifest(entry)
        tables = cast(dict[str, object], manifest["tables"])
        notes = cast(dict[str, object], tables["notes"])
        reference = cast(dict[str, object], cast(list[object], notes["objects"])[0])
        object_name = cast(str, reference["name"])

        live_archive.write_bytes(object_name, b"replacement")

        with pytest.raises(GcsPreconditionError):
            store.restore(destination, snapshot.rsplit("/", 1)[-1])
        assert destination.query("SELECT * FROM notes").num_rows == 0
    finally:
        source.close()
        destination.close()
