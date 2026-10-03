# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_bucket_gcs_live.py — Opt-in live GCS snapshot integration tests.

Run with ``just test-gcs-live`` against a disposable bucket.

Raw developer invocation::

    export USAGEBASSOON_GCS_LIVE=1
    uv run pytest -m gcs_live tests/test_bucket_gcs_live.py

The ``gcs_live`` marker selects these tests, and ``USAGEBASSOON_GCS_LIVE=1``
enables access to a preconfigured GCS test bucket.

The test bucket is fixed by ``_TEST_BUCKET`` and has no environment override.
Each test creates and removes its own prefix, without a bucket reset control.

Credentials use application default authentication, including
``GOOGLE_APPLICATION_CREDENTIALS`` when configured.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

import pyarrow as pa
import pytest

from tests._observations import observations
from tests._snapshot_fakes import seed_recovery_data
from tests._sql_parity import normalized_records
from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.buckets.base import SnapshotPreconditionError as GcsPreconditionError
from usagebassoon.buckets.gcs import GcsSnapshotBucket as GcsArchive
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import normalize
from usagebassoon.persistence import persist_run
from usagebassoon.storage_model import (
    DEBUG_TABLES,
)

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
        observations(
            pa.table(
                {
                    "source_id": ["11111111-1111-4111-8111-111111111111"],
                    "client": ["codex"],
                    "session_id": ["live-gcs-session"],
                    "note": ["live GCS snapshot"],
                    "created_at": [captured_at],
                    "collected_at": [captured_at],
                }
            )
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
    tmp_path: Path,
    collection_bundle: CollectionBundle,
) -> None:
    """Restore a real GCS snapshot after verifying manifest and Parquet SHA-256."""
    source = _backend()
    destination = _backend()
    sources = (str(uuid4()), str(uuid4()))
    expected = seed_recovery_data(source, collection_bundle, sources)
    try:
        store = _store(live_archive)
        snapshot = store.write(source, run_id="live-round-trip", manual=True, pin=True)
        assert snapshot is not None
        local_uri = store.copy(snapshot, str(tmp_path / "local"))
        relocated_root = live_archive.uri + "/relocated"
        relocated_uri = SnapshotStore(str(tmp_path / "local")).copy(
            local_uri, relocated_root
        )
        relocated = GcsArchive(relocated_root)
        _, generation = relocated.read_json("catalog.json")
        assert generation is not None
        relocated.delete("catalog.json", version=generation)
        recovery = SnapshotStore(relocated_root, gcs_bucket=relocated)
        assert recovery.reader.listing()[0]["pinned"] is True
        restored = recovery.restore(destination, relocated_uri)
        assert restored == {table: len(rows) for table, rows in expected.items()}
        for table, records in expected.items():
            relation = ("replay_" if table in DEBUG_TABLES else "current_") + table
            assert (
                normalized_records(destination.query(f"SELECT * FROM {relation}"))
                == records
            )
        persist_run(
            destination,
            normalize(
                replace(collection_bundle, source_id=sources[0], run_id=str(uuid4()))
            ),
        )
        assert destination.query("SELECT * FROM current_daily_stats").num_rows == len(
            expected["daily_stats"]
        )
        assert set(
            destination.query("SELECT DISTINCT source_id FROM current_daily_stats")
            .column("source_id")
            .to_pylist()
        ) == set(sources)
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
        candidates, _ = store.reader.candidates()
        candidate = candidates[0]
        manifest = store.reader.manifest(candidate)
        tables = cast(dict[str, object], manifest["tables"])
        notes = cast(dict[str, object], tables["notes"])
        reference = cast(dict[str, object], cast(list[object], notes["objects"])[0])
        object_name = f"{candidate.identifier}/{reference['name']}"

        live_archive.write_bytes(object_name, b"replacement")

        with pytest.raises(ValueError, match="SHA-256"):
            store.restore(destination, snapshot.rsplit("/", 1)[-1])
        assert destination.query("SELECT * FROM notes").num_rows == 0
    finally:
        source.close()
        destination.close()
