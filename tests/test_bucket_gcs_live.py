# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_bucket_gcs_live.py — Opt-in live GCS snapshot integration tests.

Run with ``just test-gcs-live`` against a disposable bucket.

Raw developer invocation::

    export USAGEBASSOON_GCS_LIVE=1
    uv run pytest -m gcs_live tests/test_bucket_gcs_live.py

The ``gcs_live`` marker selects these tests, and ``USAGEBASSOON_GCS_LIVE=1``
enables access to a preconfigured GCS test bucket.

The bucket is always ``usagebassoon-test-snapshots-<gcp-project-id>``.
``USAGEBASSOON_GCS_PROJECT`` selects the project explicitly in CI. Local runs without
that variable may use the project resolved by application default authentication.
Arbitrary bucket names are never accepted.

Each test creates and removes its own prefix, without a bucket reset control.

Credentials use application default authentication, including
``GOOGLE_APPLICATION_CREDENTIALS`` when configured.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from typing import cast
from uuid import uuid4

import google.auth
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
from usagebassoon.snapshot.catalog import ArchiveBusy, Catalog
from usagebassoon.storage_model import (
    DEBUG_TABLES,
)

pytestmark = pytest.mark.gcs_live

_TEST_BUCKET_PREFIX = "usagebassoon-test-snapshots-"


def _test_project() -> str:
    """Select the CI project explicitly, with discovery for local runs only."""
    project = os.environ.get("USAGEBASSOON_GCS_PROJECT")
    if not project and os.environ.get("CI") == "true":
        pytest.fail("USAGEBASSOON_GCS_PROJECT is required for CI GCS tests")
    if not project:
        _, project = google.auth.default()
    if not project:
        pytest.fail("GCS project is unavailable; set USAGEBASSOON_GCS_PROJECT")
    return project


@pytest.fixture
def live_archive() -> Generator[GcsArchive]:
    """Create and remove an isolated prefix in the dedicated GCS test bucket."""
    if os.environ.get("USAGEBASSOON_GCS_LIVE") != "1":
        pytest.skip("set USAGEBASSOON_GCS_LIVE=1 to run GCS integration tests")
    project = _test_project()
    archive = GcsArchive(
        f"gs://{_TEST_BUCKET_PREFIX}{project}/usagebassoon-tests/{uuid4().hex}",
        project=project,
    )
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
    return SnapshotStore(archive.uri, buckets=(archive,))


def test_live_gcs_generation_operations(live_archive: GcsArchive) -> None:
    """Verify official-client conditional object and catalog operations."""
    first = live_archive.write_bytes("probe", b"one", if_generation_match=0)
    assert live_archive.read_bytes("probe", version=first.version) == b"one"
    with pytest.raises(GcsPreconditionError):
        live_archive.write_bytes("probe", b"two", if_generation_match=0)
    replacement = live_archive.write_bytes(
        "probe", b"two", if_generation_match=cast(int, first.version)
    )
    assert replacement.version != first.version
    try:
        historical = live_archive.read_bytes("probe", version=first.version)
    except GcsPreconditionError:
        pass
    else:
        # Versioned buckets may retain the old generation; never return new bytes.
        assert historical == b"one"
    with pytest.raises(GcsPreconditionError):
        live_archive.delete("probe", version=first.version)
    assert live_archive.read_bytes("probe", version=replacement.version) == b"two"
    catalog = live_archive.write_json_cas(
        "catalog.json", {"entries": []}, expected_version=None
    )
    assert live_archive.read_json("catalog.json") == (
        {"entries": []},
        catalog.version,
    )
    updated = live_archive.write_json_cas(
        "catalog.json", {"entries": ["updated"]}, expected_version=catalog.version
    )
    with pytest.raises(GcsPreconditionError):
        live_archive.write_json_cas(
            "catalog.json", {"entries": []}, expected_version=catalog.version
        )
    assert live_archive.read_json("catalog.json") == (
        {"entries": ["updated"]},
        updated.version,
    )
    live_archive.lifecycle_warnings()


def test_live_gcs_catalog_claim_race_and_takeover_reject_stale_owner(
    live_archive: GcsArchive,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Native CAS admits one claimant and fences an expired owner's mutations."""
    claimants = [
        Catalog(GcsArchive(live_archive.uri, project=live_archive.project))
        for _ in range(2)
    ]
    barrier = Barrier(2)

    def synchronize_read(
        bucket: GcsArchive,
    ) -> Callable[[str], tuple[dict[str, object] | None, str | int | None]]:
        original = bucket.read_json

        def read(name: str) -> tuple[dict[str, object] | None, str | int | None]:
            result = original(name)
            if name == "control.json" and result[0] is None:
                # Both native reads must observe absence before either native CAS.
                barrier.wait(timeout=30)
            return result

        return read

    def claim(catalog: Catalog) -> bool:
        try:
            catalog._update_reservation(claim=True)
        except ArchiveBusy:
            return False
        return True

    with monkeypatch.context() as synchronized:
        for catalog in claimants:
            assert isinstance(catalog.bucket, GcsArchive)
            synchronized.setattr(
                catalog.bucket, "read_json", synchronize_read(catalog.bucket)
            )
        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(claim, claimants))
    assert sum(outcomes) == 1
    winner, successor = (
        claimants[outcomes.index(True)],
        claimants[outcomes.index(False)],
    )
    winner.stage("owned", {"pinned": False, "retired": False, "published": False})
    old_fence = winner.fence
    document, version = winner.control()
    reservation = document["reservation"]
    assert isinstance(reservation, dict)
    reservation["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    live_archive.write_json_cas("control.json", document, expected_version=version)
    successor._update_reservation(claim=True)
    assert old_fence is not None and successor.fence is not None
    assert successor.fence > old_fence
    successor.pin("owned")
    with pytest.raises(ArchiveBusy):
        winner.publish("owned", datetime.now(UTC).isoformat())
    state = successor.state("owned")
    assert state is not None and state["pinned"] is True
    assert state["published"] is False and state["retired"] is False
    control, _ = successor.control()
    reservation = control["reservation"]
    assert isinstance(reservation, dict) and reservation["owner"] == successor.owner


def test_live_gcs_retirement_forgets_only_completed_cleanup(
    live_archive: GcsArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resume interrupted native deletion and preserve the archive fence."""
    catalog = Catalog(live_archive)
    with catalog.hold():
        catalog.stage(
            "pending", {"pinned": False, "retired": False, "published": False}
        )
        live_archive.write_bytes("pending/data.parquet", b"disposable integration data")
        original = live_archive.delete

        def interrupt(name: str, *, version: str | int) -> None:
            """Leave the final portable state deletion pending."""
            if name == "pending/state.json":
                raise OSError("interrupted final deletion")
            original(name, version=version)

        with monkeypatch.context() as patch:
            patch.setattr(live_archive, "delete", interrupt)
            with pytest.raises(OSError, match="interrupted final deletion"):
                catalog.retire("pending")
        state = catalog.state("pending")
        assert state is not None and state["retired"] is True
        assert [obj.name for obj in live_archive.list("pending")] == [
            "pending/state.json"
        ]
        fence = catalog.fence
        archive_id = catalog.control()[0]["archive_id"]
    with Catalog(live_archive).hold(enforce_policy=True) as resumed:
        assert resumed.fence is not None and fence is not None
        assert resumed.fence > fence
        document, _ = resumed.control()
        assert document["archive_id"] == archive_id
        assert document["records"] == {}
        assert live_archive.list("pending") == ()


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
        recovery = SnapshotStore(relocated_root, buckets=(relocated,))
        assert recovery.reader.listing()[0]["pinned"] is True
        # Immutable emergency recovery does not need working lifecycle controls.
        relocated.write_bytes("control.json", b'{"version": 999}')
        relocated.write_bytes(
            f"{relocated_uri.rsplit('/', 1)[-1]}/state.json", b"damaged"
        )
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
