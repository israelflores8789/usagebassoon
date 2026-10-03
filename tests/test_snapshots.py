# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_snapshots.py — Local snapshot catalog, rotation, and restore tests."""

from __future__ import annotations

import errno
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

from tests._observations import observations
from tests._snapshot_fakes import TableBackend
from usagebassoon.archiver import SNAPSHOT_TABLES
from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.buckets.base import SnapshotPreconditionError
from usagebassoon.buckets.local import LocalSnapshotBucket
from usagebassoon.config import SnapshotConfig, UsageBassoonConfig
from usagebassoon.snapshot.catalog import ArchiveBusy, Catalog
from usagebassoon.snapshot.reader import SnapshotReader


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
        observations(
            pa.table(
                {
                    "source_id": ["11111111-1111-4111-8111-111111111111"],
                    "client": ["codex"],
                    "session_id": ["private-session"],
                    "note": ["private note"],
                    "created_at": [captured_at],
                    "collected_at": [captured_at],
                }
            )
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
    """Reject unsafe identities before retirement can access an outside file."""
    bucket = LocalSnapshotBucket(str(tmp_path / "archive"))
    outside = tmp_path / "outside"
    outside.write_text("keep")
    bucket.write_json_cas(
        "catalog.json",
        {
            "version": 1,
            "entries": [
                {
                    "snapshot_id": "../../outside",
                    "captured_at": datetime.now(UTC).isoformat(),
                }
            ],
        },
        expected_version=None,
    )
    with pytest.raises(ValueError, match="invalid snapshot identifier"):
        Catalog(bucket).entries()
    with pytest.raises(ValueError):
        bucket.delete("../../outside", version="unknown")
    assert outside.read_text() == "keep"


def test_restore_rejects_traversal_manifest_and_object_names(tmp_path: Path) -> None:
    """Constrain portable object references even with a matching manifest digest."""
    source, target = _backend(), _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        uri = store.write(source, run_id="safe")
        assert uri is not None
        directory = Path(uri)
        manifest = json.loads((directory / "manifest.json").read_text())
        manifest["tables"]["notes"]["objects"][0]["name"] = "../../private-file"
        raw = json.dumps(manifest).encode()
        (directory / "manifest.json").write_bytes(raw)
        (directory / "COMPLETE").write_text(
            json.dumps(
                {
                    "snapshot_id": directory.name,
                    "manifest_sha256": sha256(raw).hexdigest(),
                }
            )
        )
        with pytest.raises(ValueError, match="unsafe component"):
            store.restore(target, uri)
        assert target.query("SELECT * FROM notes").num_rows == 0
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
    """Keep cadence independent of manual capture and reject a stolen fence."""
    backend = _backend()
    bucket = LocalSnapshotBucket(str(tmp_path / "archive"))
    store = SnapshotStore(bucket.uri, interval="1h")
    try:
        assert store.write(backend, run_id="first") is not None
        assert store.write(backend, run_id="second") is None
        assert store.write(backend, run_id="manual", manual=True, pin=True) is not None
        assert store.write(backend, run_id="third") is None
        catalog = Catalog(bucket)
        with pytest.raises(ArchiveBusy, match="lost"), catalog.hold():
            document, version = catalog.control()
            document["reservation"] = {
                "owner": "other",
                "fence": 999,
                "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            }
            bucket.write_json_cas("control.json", document, expected_version=version)
            catalog.check()
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


def test_portable_copy_recovers_without_catalog_and_preserves_pin(
    tmp_path: Path,
) -> None:
    """Recover a copied, independently complete archive with its lifecycle state."""
    source, target = _backend(), _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "source"))
    try:
        uri = store.write(source, run_id="portable", manual=True, pin=True)
        assert uri is not None
        copied = store.copy(uri, str(tmp_path / "copy"))
        catalog = tmp_path / "copy" / "catalog.json"
        catalog.unlink()
        relocated = SnapshotStore(str(tmp_path / "copy"))
        assert relocated.reader.listing()[0]["pinned"] is True
        restored = relocated.restore(target, copied + "/manifest.json")
        assert restored["notes"] == 1
        assert target.query("SELECT note FROM notes").to_pylist() == [
            {"note": "private note"}
        ]
        with Catalog(LocalSnapshotBucket(str(tmp_path / "copy"))).hold() as index:
            index.repair()
        assert (
            json.loads(catalog.read_text())["entries"][0]["snapshot_id"]
            == Path(uri).name
        )
    finally:
        source.close()
        target.close()


def test_pins_preserve_manifest_digest_and_survive_rotation(tmp_path: Path) -> None:
    """Pins change lifecycle state while immutable content stays recoverable."""
    backend = _backend()
    store = SnapshotStore(str(tmp_path / "archive"), max_snapshots=1)
    try:
        uri = store.write(backend, run_id="protect")
        assert uri is not None
        expected = json.loads((Path(uri) / "COMPLETE").read_text())["manifest_sha256"]
        store.pin(uri)
        assert (
            sha256((Path(uri) / "manifest.json").read_bytes()).hexdigest() == expected
        )
        for index in range(3):
            assert store.write(backend, run_id=str(index)) is not None
        assert len(store.list_snapshots()) == 2
        assert Path(uri).name in store.list_snapshots()
        store.delete(uri)
        assert len(store.list_snapshots()) == 1
        # Repeating an interrupted deletion addresses its surviving tombstone.
        store.delete(uri)
        assert json.loads((Path(uri) / "state.json").read_text())["retired"] is True
    finally:
        backend.close()


def test_weekly_retention_is_independent_and_never_fabricates_slots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep four successful UTC week slots plus a manually pinned recovery point."""
    import usagebassoon.archiver as module

    backend = _backend()
    configuration = UsageBassoonConfig(
        tmp_path / "config.toml",
        "11111111-1111-4111-8111-111111111111",
        "duckdb",
        snapshots=SnapshotConfig(file_uri=str(tmp_path / "archive"), max_snapshots=1),
    )
    store = SnapshotStore.from_config(configuration)
    clock = [datetime(2026, 1, 5, tzinfo=UTC)]
    monkeypatch.setattr(module, "_now", lambda: clock[0])
    try:
        pinned = store.write(backend, run_id="manual", manual=True, pin=True)
        assert pinned is not None
        slots: list[str] = []
        for index in (0, 1, 3, 4, 5, 6):
            clock[0] = datetime(2026, 1, 5, tzinfo=UTC) + timedelta(weeks=index)
            assert store.write(backend, run_id="weekly") is not None
            assert store.write(backend, run_id="same-slot") is None
            slots.append(clock[0].strftime("%G-W%V"))
        records = store.reader.listing()
        assert len(records) == 5
        assert {r["weekly_slot"] for r in records if not r["pinned"]} == set(slots[-4:])
        assert any(r["id"] == Path(pinned).name and r["pinned"] for r in records)
    finally:
        backend.close()


def test_global_latest_warns_and_falls_back_before_restore(tmp_path: Path) -> None:
    """Sort by capture point across roots and skip a corrupted automatic copy."""
    source, target = _backend(), _backend()
    _append_note(source)
    oldest = SnapshotStore(str(tmp_path / "old"))
    newest = SnapshotStore(str(tmp_path / "new"))
    try:
        old = oldest.write(source, run_id="old")
        new = newest.write(source, run_id="new")
        assert old is not None and new is not None
        reader = SnapshotReader(
            (
                LocalSnapshotBucket(str(tmp_path / "old")),
                LocalSnapshotBucket(str(tmp_path / "new")),
            ),
            LocalSnapshotBucket,
        )
        with reader.prepare() as prepared:
            assert prepared.candidate.identifier == Path(new).name
        (Path(new) / "notes.parquet").write_bytes(b"corrupt")
        warnings: list[str] = []
        with reader.prepare(warning=warnings.append) as prepared:
            assert prepared.candidate.identifier == Path(old).name
            assert warnings and Path(new).name in warnings[0]
        with pytest.raises(ValueError, match="SHA-256"):
            newest.restore(target, new)
        assert target.query("SELECT * FROM notes").num_rows == 0
    finally:
        source.close()
        target.close()


def test_restore_rejects_unexpected_data_and_rolls_back_late_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Native emptiness and transactional writes protect the destination."""
    source, target = _backend(), _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        uri = store.write(source, run_id="atomic")
        assert uri is not None
        target.connection.execute("CREATE TABLE unexpected (value INTEGER)")
        target.connection.execute("INSERT INTO unexpected VALUES (1)")
        with pytest.raises(ValueError, match=r"populated main\.unexpected"):
            store.restore(target)
        target.connection.execute("DELETE FROM unexpected")
        original = target.append
        calls: list[str] = []

        def append_then_fail(table: str, data: pa.Table) -> None:
            original(table, data)
            calls.append(table)
            raise RuntimeError("late destination failure")

        with monkeypatch.context() as failed:
            failed.setattr(target, "append", append_then_fail)
            with pytest.raises(RuntimeError, match="late destination failure"):
                store.restore(target)
        assert calls == ["notes"]
        assert target.query("SELECT * FROM notes").num_rows == 0
        assert target.query("SELECT * FROM restore_receipts").num_rows == 0
        assert store.restore(target)["notes"] == 1
        notices: list[str] = []
        assert store.restore(target, notice=notices.append)["notes"] == 1
        assert "already committed" in notices[0]
        assert target.query("SELECT * FROM notes").num_rows == 1
    finally:
        source.close()
        target.close()


def test_corrupt_index_repair_preserves_evidence_and_control(tmp_path: Path) -> None:
    """Rebuild an index without overwriting independent reservation metadata."""
    backend = _backend()
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        uri = store.write(backend, run_id="repair", manual=True, pin=True)
        assert uri is not None
        bucket = LocalSnapshotBucket(str(tmp_path / "archive"))
        (tmp_path / "archive" / "catalog.json").write_bytes(b"broken catalog")
        with Catalog(bucket).hold() as catalog:
            owner = catalog.owner
            catalog.repair()
            control, _ = catalog.control()
            assert (
                isinstance(control["reservation"], dict)
                and control["reservation"]["owner"] == owner
            )
        assert store.reader.listing()[0]["pinned"] is True
        evidence = [
            obj for obj in bucket.list("") if obj.name.startswith("catalog.corrupt-")
        ]
        assert len(evidence) == 1
        assert (
            bucket.read_bytes(evidence[0].name, version=evidence[0].version)
            == b"broken catalog"
        )
    finally:
        backend.close()


def test_temporary_disk_exhaustion_stops_selection_without_destination_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A host resource failure must not select an older recovery point."""
    source, target = _backend(), _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    calls: list[str] = []
    try:
        assert store.write(source, run_id="older") is not None
        latest = store.write(source, run_id="newer")
        assert latest is not None

        def no_space(
            _self: LocalSnapshotBucket, relative_name: str, _path: Path
        ) -> object:
            calls.append(relative_name)
            raise OSError(errno.ENOSPC, "no space")

        monkeypatch.setattr(LocalSnapshotBucket, "download_file", no_space)
        with pytest.raises(RuntimeError, match="Insufficient temporary disk space"):
            store.restore(target)
        assert len(calls) == 1 and calls[0].startswith(Path(latest).name + "/")
        assert target.query("SELECT * FROM notes").num_rows == 0
        assert target.query("SELECT * FROM restore_receipts").num_rows == 0
    finally:
        source.close()
        target.close()
