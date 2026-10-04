# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_snapshots.py — Local snapshot catalog, rotation, and restore tests."""

from __future__ import annotations

import errno
import json
import time
from collections.abc import Mapping
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
from usagebassoon.config import (
    LocalSnapshotConfig,
    SnapshotsConfig,
    UsageBassoonConfig,
)
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
        snapshots=SnapshotsConfig(
            max_snapshots=1,
            local=LocalSnapshotConfig(path=tmp_path / "archive"),
        ),
    )
    store = SnapshotStore.from_config(configuration)
    store.interval = None  # Exercise weekly retention independently of cadence.
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


@pytest.mark.parametrize("mutable", ["catalog.json", "control.json", "state.json"])
def test_emergency_restore_ignores_corrupt_mutable_metadata(
    tmp_path: Path, mutable: str
) -> None:
    """Intact immutable recovery units survive damaged coordination documents."""
    source, target = _backend(), _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        uri = store.write(source, run_id="recover", manual=True, pin=True)
        assert uri is not None
        path = (
            Path(uri) / mutable
            if mutable == "state.json"
            else Path(uri).parent / mutable
        )
        path.write_text('{"version": 999}' if mutable == "control.json" else "broken")
        notices: list[str] = []
        assert store.restore(target, notice=notices.append)["notes"] == 1
        assert target.query("SELECT note FROM notes").to_pylist() == [
            {"note": "private note"}
        ]
    finally:
        source.close()
        target.close()


def test_retired_immutable_copy_remains_readable_before_cleanup(tmp_path: Path) -> None:
    """Retirement affects management eligibility, not immutable recovery validity."""
    source, target = _backend(), _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        uri = store.write(source, run_id="retired", manual=True)
        assert uri is not None
        bucket = LocalSnapshotBucket(str(Path(uri).parent))
        with Catalog(bucket).hold() as catalog:
            state = catalog.state(Path(uri).name)
            assert state is not None
            catalog.transition(Path(uri).name, {**state, "retired": True})
        assert store.list_snapshots() == []
        assert store.restore(target, uri)["notes"] == 1
    finally:
        source.close()
        target.close()


@pytest.mark.parametrize("operation", ["publish", "retire", "pin"])
def test_takeover_between_authority_read_and_mutation_blocks_stale_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    """A changed control generation rejects the transition itself, not a later check."""
    bucket = LocalSnapshotBucket(str(tmp_path / "archive"))
    first, second = Catalog(bucket), Catalog(bucket)
    first._update_reservation(claim=True)
    first.stage("owned", {"pinned": False, "retired": False, "published": False})
    original = bucket.write_json_cas
    intercepted = False

    def takeover(
        name: str, payload: dict[str, object], *, expected_version: str | int | None
    ) -> object:
        nonlocal intercepted
        if name == "control.json" and not intercepted:
            intercepted = True
            document, version = first.control()
            reservation = document["reservation"]
            assert isinstance(reservation, dict)
            reservation["expires_at"] = (
                datetime.now(UTC) - timedelta(seconds=1)
            ).isoformat()
            original(name, document, expected_version=version)
            second._update_reservation(claim=True)
        return original(name, payload, expected_version=expected_version)

    monkeypatch.setattr(bucket, "write_json_cas", takeover)
    with pytest.raises(ArchiveBusy, match="lost"):
        if operation == "publish":
            first.publish("owned", datetime.now(UTC).isoformat())
        elif operation == "retire":
            first.retire("owned")
        else:
            first.pin("owned")
    state = second.state("owned")
    assert state is not None and state["retired"] is False
    assert state["published"] is False and state["pinned"] is False


def test_repair_corrupt_index_with_abandoned_stage(tmp_path: Path) -> None:
    """Index reconstruction does not retire through the corrupt index first."""
    backend = _backend()
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        uri = store.write(backend, run_id="complete", manual=True, pin=True)
        assert uri is not None
        bucket = LocalSnapshotBucket(str(Path(uri).parent))
        with Catalog(bucket).hold() as catalog:
            catalog.stage(
                "abandoned", {"pinned": False, "retired": False, "published": False}
            )
        (Path(uri).parent / "catalog.json").write_text("broken")
        with Catalog(bucket).hold() as catalog:
            catalog.repair()
            catalog.cleanup_abandoned()
        assert store.list_snapshots() == [Path(uri).name]
        state = Catalog(bucket).state("abandoned")
        assert state is not None and state["retired"] is True
    finally:
        backend.close()


def test_transformation_rejects_bad_intermediate_contract_before_next_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Synthetic future registrations test infrastructure without legacy fixtures."""
    import usagebassoon.snapshot.format as format_module
    import usagebassoon.snapshot.reader as reader_module

    backend = _backend()
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        uri = store.write(backend, run_id="contract", manual=True)
        assert uri is not None
        monkeypatch.setattr(format_module, "DATA_SCHEMA_VERSION", 3)
        monkeypatch.setattr(reader_module, "DATA_SCHEMA_VERSION", 3)
        contracts = format_module.DATA_CONTRACTS
        monkeypatch.setitem(contracts, 2, contracts[1])
        monkeypatch.setitem(contracts, 3, contracts[1])
        called: list[str] = []

        def invalid_step(_files: object, _output: Path) -> dict[str, Path]:
            called.append("invalid")
            return {}

        def forbidden_step(_files: object, _output: Path) -> dict[str, Path]:
            called.append("next")
            raise AssertionError("next step must not consume invalid intermediate data")

        def validate(_before: object, _after: object) -> None:
            called.append("semantic")

        monkeypatch.setattr(
            format_module,
            "TRANSFORMATIONS",
            (
                format_module.SnapshotTransformation(1, 2, invalid_step, validate),
                format_module.SnapshotTransformation(2, 3, forbidden_step, validate),
            ),
        )
        with (
            pytest.raises(ValueError, match="every contract table"),
            store.reader.prepare(uri),
        ):
            pass
        assert called == ["invalid"]
    finally:
        backend.close()


def test_transformation_fallback_isolates_candidates_and_preserves_identities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed upgrade cannot contaminate the next candidate's temporary files."""
    import shutil

    import pyarrow.parquet as pq

    import usagebassoon.snapshot.format as format_module
    import usagebassoon.snapshot.reader as reader_module

    source = _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        older = store.write(source, run_id="older", manual=True)
        newer = store.write(source, run_id="newer", manual=True)
        assert older is not None and newer is not None
        original_manifest = (Path(older) / "manifest.json").read_bytes()
        original_notes = (Path(older) / "notes.parquet").read_bytes()
        contracts = format_module.DATA_CONTRACTS
        monkeypatch.setattr(format_module, "DATA_SCHEMA_VERSION", 3)
        monkeypatch.setattr(reader_module, "DATA_SCHEMA_VERSION", 3)
        monkeypatch.setitem(contracts, 2, contracts[1])
        monkeypatch.setitem(contracts, 3, contracts[1])
        failed = [False]
        validations: list[int] = []

        def transform(files: Mapping[str, Path], output: Path) -> Mapping[str, Path]:
            if not failed[0]:
                failed[0] = True
                (output / "partial").write_text("interrupted")
                raise ValueError("interrupted transformation")
            result: dict[str, Path] = {}
            for table, schema in contracts[1].items():
                path = output / f"{table}.parquet"
                if table in files:
                    shutil.copyfile(files[table], path)
                else:
                    pq.write_table(pa.Table.from_batches([], schema=schema), path)
                result[table] = path
            return result

        def semantic(before: Mapping[str, Path], after: Mapping[str, Path]) -> None:
            assert pq.read_table(before["notes"]).equals(pq.read_table(after["notes"]))
            validations.append(1)

        monkeypatch.setattr(
            format_module,
            "TRANSFORMATIONS",
            (
                format_module.SnapshotTransformation(1, 2, transform, semantic),
                format_module.SnapshotTransformation(2, 3, transform, semantic),
            ),
        )
        notices: list[str] = []
        with store.reader.prepare(warning=notices.append) as prepared:
            assert prepared.candidate.identifier == Path(older).name
            assert prepared.rows["notes"] == 1
            assert len(validations) == 2
            assert next(iter(prepared.batches("notes"))).num_rows == 1
        assert any("interrupted transformation" in notice for notice in notices)
        assert (Path(older) / "manifest.json").read_bytes() == original_manifest
        assert (Path(older) / "notes.parquet").read_bytes() == original_notes
    finally:
        source.close()


def test_lost_restore_acknowledgement_resolves_receipt_without_replaying(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A post-commit exception is distinguished from a failed transaction."""
    source, target = _backend(), _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    original = target.restore_snapshot
    attempts: list[str] = []

    def lost_reply(
        files: Mapping[str, Path], *, operation_id: str, snapshot_id: str
    ) -> None:
        attempts.append(operation_id)
        original(files, operation_id=operation_id, snapshot_id=snapshot_id)
        raise RuntimeError("commit reply lost")

    try:
        uri = store.write(source, run_id="receipt", manual=True)
        assert uri is not None
        monkeypatch.setattr(target, "restore_snapshot", lost_reply)
        notices: list[str] = []
        assert store.restore(target, uri, notice=notices.append)["notes"] == 1
        assert any("acknowledgement was interrupted" in notice for notice in notices)
        assert store.restore(target, uri)["notes"] == 1
        assert len(attempts) == 1
        assert target.query("SELECT * FROM notes").num_rows == 1
        assert target.query("SELECT * FROM restore_receipts").num_rows == 1
    finally:
        source.close()
        target.close()


def test_download_reservation_blocks_rotation_until_files_are_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A normal reader protects its recovery point throughout bounded download."""
    from threading import Event

    source = _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    started, release = Event(), Event()
    original = LocalSnapshotBucket.download_file

    def download(bucket: LocalSnapshotBucket, relative_name: str, path: Path) -> object:
        started.set()
        assert release.wait(5)
        return original(bucket, relative_name, path)

    def read(uri: str) -> int:
        with store.reader.prepare(uri) as prepared:
            return prepared.rows["notes"]

    try:
        uri = store.write(source, run_id="reader", manual=True)
        assert uri is not None
        monkeypatch.setattr(LocalSnapshotBucket, "download_file", download)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(read, uri)
            try:
                assert started.wait(5)
                bucket = LocalSnapshotBucket(str(Path(uri).parent))
                with pytest.raises(ArchiveBusy), Catalog(bucket).hold():
                    pytest.fail("rotation acquired an active download reservation")
            finally:
                release.set()
            assert future.result(timeout=5) == 1
        assert (Path(uri) / "COMPLETE").exists()
    finally:
        source.close()


def test_rescue_copy_preserves_immutables_when_source_lifecycle_is_damaged(
    tmp_path: Path,
) -> None:
    """A verified rescue copy conservatively pins unknown lifecycle state."""
    backend = _backend()
    _append_note(backend)
    store = SnapshotStore(str(tmp_path / "source"))
    try:
        uri = store.write(backend, run_id="rescue", manual=True)
        assert uri is not None
        manifest = (Path(uri) / "manifest.json").read_bytes()
        (Path(uri).parent / "control.json").write_text("damaged")
        (Path(uri) / "state.json").write_text("damaged")
        copied = store.copy(uri, str(tmp_path / "rescue"))
        assert (Path(copied) / "manifest.json").read_bytes() == manifest
        assert (
            SnapshotStore(str(tmp_path / "rescue")).reader.listing()[0]["pinned"]
            is True
        )
    finally:
        backend.close()


def test_verified_download_survives_corrupted_reservation_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mutables changing during download cannot invalidate already verified data."""
    source, target = _backend(), _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    original = LocalSnapshotBucket.download_file

    def corrupt_after_read(
        bucket: LocalSnapshotBucket, relative_name: str, path: Path
    ) -> object:
        result = original(bucket, relative_name, path)
        (tmp_path / "archive" / "control.json").write_text("corrupted during reading")
        return result

    try:
        uri = store.write(source, run_id="read-release", manual=True)
        assert uri is not None
        monkeypatch.setattr(LocalSnapshotBucket, "download_file", corrupt_after_read)
        notices: list[str] = []
        assert store.restore(target, uri, notice=notices.append)["notes"] == 1
        assert any("reservation release failed" in notice for notice in notices)
        assert target.query("SELECT * FROM notes").num_rows == 1
    finally:
        source.close()
        target.close()


def test_repair_indexes_only_verified_copies_without_destroying_recovery_data(
    tmp_path: Path,
) -> None:
    """Failed repair verification excludes management without preventing rescue."""
    source, target = _backend(), _backend()
    _append_note(source)
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        older = store.write(source, run_id="older", manual=True)
        newer = store.write(source, run_id="newer", manual=True)
        assert older is not None and newer is not None
        path = Path(newer) / "notes.parquet"
        original = path.read_bytes()
        path.write_bytes(b"damaged")
        bucket = LocalSnapshotBucket(str(Path(newer).parent))
        with Catalog(bucket).hold() as catalog:
            catalog.repair()
        assert store.list_snapshots() == [Path(older).name]
        assert (Path(newer) / "COMPLETE").exists()
        path.write_bytes(original)
        assert store.restore(target, newer)["notes"] == 1
    finally:
        source.close()
        target.close()


@pytest.mark.parametrize("local_enabled", [False, True])
@pytest.mark.parametrize("gcs_enabled", [False, True])
def test_archiver_uses_only_enabled_destinations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    local_enabled: bool,
    gcs_enabled: bool,
) -> None:
    """Archive locally and to one remote bucket according to explicit flags."""
    from tests.test_bucket_gcs import MemoryGcsArchive
    from usagebassoon.config import GcsConfig

    archive = MemoryGcsArchive()
    calls: list[str] = []

    def cloud(
        uri: str, *, project: str, credentials_file: Path | None, timeout_seconds: float
    ) -> MemoryGcsArchive:
        del project, credentials_file, timeout_seconds
        calls.append(uri)
        return archive

    monkeypatch.setattr("usagebassoon.buckets.gcs.GcsSnapshotBucket", cloud)
    config = UsageBassoonConfig(
        tmp_path / "config.toml",
        "11111111-1111-4111-8111-111111111111",
        "duckdb",
        snapshots=SnapshotsConfig(
            local=LocalSnapshotConfig(path=tmp_path / "archive", enable=local_enabled),
            gcs=GcsConfig(uri=archive.uri, project="test", enable=gcs_enabled),
            max_snapshots=5,
        ),
    )
    if not local_enabled and not gcs_enabled:
        with pytest.raises(ValueError, match="at least one snapshot destination"):
            SnapshotStore.from_config(config)
        assert not calls
        return
    store = SnapshotStore.from_config(config)
    expected = ([str(tmp_path / "archive")] if local_enabled else []) + (
        [archive.uri] if gcs_enabled else []
    )
    assert store.destination_uris == tuple(expected)
    assert calls == ([archive.uri] if gcs_enabled else [])
    assert store.interval == timedelta(hours=12)
    assert store._weekly == set(expected)
    assert (
        store.write(cast(StorageBackend, TableBackend()), run_id="shared") is not None
    )
    for bucket in store.reader.buckets:
        control, _ = Catalog(bucket).control()
        assert control["policy"] == {"max_snapshots": 5, "weekly_slots": 4}


@pytest.mark.parametrize("disable_weekly", [False, True])
def test_automatic_snapshots_observe_independent_cadence_and_weekly_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, disable_weekly: bool
) -> None:
    """Automatic captures serve due roles and skip a call before twelve hours."""
    import usagebassoon.archiver as module
    from usagebassoon.config import SnapshotScheduleConfig

    clock = [datetime.now(UTC)]
    monkeypatch.setattr(module, "_now", lambda: clock[0])
    config = UsageBassoonConfig(
        tmp_path / "config.toml",
        "11111111-1111-4111-8111-111111111111",
        "duckdb",
        snapshots=SnapshotsConfig(
            local=LocalSnapshotConfig(path=tmp_path / "archive"),
            schedule=SnapshotScheduleConfig(disable_weekly=disable_weekly),
        ),
    )
    store = SnapshotStore.from_config(config)
    backend = _backend()
    try:
        uri = store.write(backend, run_id="scheduled")
        assert uri is not None
        manifest = json.loads((Path(uri) / "manifest.json").read_text())
        assert manifest["cadence"]["roles"] == (
            ["scheduled"] if disable_weekly else ["scheduled", "weekly"]
        )
        assert manifest["cadence"]["interval_seconds"] == 43200.0
        clock[0] += timedelta(minutes=1)
        assert store.write(backend, run_id="early") is None
        clock[0] += timedelta(hours=13)
        assert store.write(backend, run_id="due") is not None
    finally:
        backend.close()


def test_archive_read_configuration_does_not_require_a_backend(tmp_path: Path) -> None:
    """Discover archives without requiring valid backend settings."""
    path = tmp_path / "config.toml"
    path.write_text(
        'backend.provider = "unavailable"\n'
        f'[snapshots.local]\npath = "{tmp_path / "archive"}"\n'
        '[snapshots.schedule]\ninterval = "2d"\ndisable_weekly = true\n'
        "[snapshots.gcs]\nenable = false\n"
    )
    store = SnapshotStore.for_read(path)
    assert store.destination_uris == (str(tmp_path / "archive"),)
    assert store.interval == timedelta(days=2)


def test_archive_discovery_honors_logging_opt_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read-only archive commands honor disabled logs without backend validation."""
    import logging

    from usagebassoon.logger import LOG_DIRECTORY_ENV_VAR, LOGGER_NAME

    directory = tmp_path / "disabled-logs"
    monkeypatch.setenv(LOG_DIRECTORY_ENV_VAR, str(directory))
    path = tmp_path / "config.toml"
    path.write_text(
        f'[snapshots.local]\npath = "{tmp_path / "archive"}"\n'
        "[logging]\ndisable = true\n"
    )
    store = SnapshotStore.for_read(path)
    assert store.destination_uris == (str(tmp_path / "archive"),)
    assert logging.getLogger(LOGGER_NAME).disabled
    assert not directory.exists()


@pytest.mark.parametrize("body_fails", [False, True])
def test_reservation_release_failure_preserves_operation_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    body_fails: bool,
) -> None:
    """Failed release is logged and expires without hiding successful or failed work."""
    import logging

    from usagebassoon.buckets.base import SnapshotObject, SnapshotVersion

    bucket = LocalSnapshotBucket(str(tmp_path / "archive"))
    original = bucket.write_json_cas
    failure = ValueError("primary snapshot failure")

    def release_fails(
        name: str,
        payload: dict[str, object],
        *,
        expected_version: SnapshotVersion | None,
    ) -> SnapshotObject:
        if name == "control.json" and payload.get("reservation") is None:
            raise OSError("release unavailable")
        return original(name, payload, expected_version=expected_version)

    monkeypatch.setattr(bucket, "write_json_cas", release_fails)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "disabled", False)
    catalog = Catalog(bucket)
    if body_fails:
        with pytest.raises(ValueError) as caught, catalog.hold():
            raise failure
        assert caught.value is failure
    else:
        with catalog.hold():
            catalog.check()
    assert catalog.fence is None
    assert "Snapshot reservation release failed" in caplog.text
    assert "release unavailable" in caplog.text
    assert Catalog(bucket).control()[0]["reservation"] is not None


def test_read_only_recovery_logs_reservation_failure_without_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Library recovery remains observable even when notice callbacks are absent."""
    import logging

    source = _backend()
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        uri = store.write(source, run_id="read-only", manual=True)
        assert uri is not None

        def denied(_catalog: Catalog, *, enforce_policy: bool = False) -> None:
            assert not enforce_policy
            raise PermissionError("archive is read-only")

        monkeypatch.setattr(Catalog, "hold", denied)
        monkeypatch.setattr(logging.getLogger("usagebassoon"), "propagate", True)
        monkeypatch.setattr(logging.getLogger("usagebassoon"), "disabled", False)
        with store.reader.prepare(uri) as prepared:
            assert prepared.candidate.uri == uri
        assert "Snapshot read reservation unavailable" in caplog.text
        assert "archive is read-only" in caplog.text
    finally:
        source.close()


def test_maintenance_notice_failure_does_not_hide_restore_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Diagnostic transport failures cannot replace the primary recovery error."""
    import logging

    from usagebassoon.snapshot.restore import report_maintenance

    backend = _backend()
    failure = RuntimeError("primary restore failure")

    def unavailable() -> tuple[bool, str] | None:
        raise OSError("maintenance inspection unavailable")

    def broken_notice(_message: str) -> None:
        raise OSError("notice sink unavailable")

    monkeypatch.setattr(backend, "maintenance_status", unavailable)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "disabled", False)
    try:
        with pytest.raises(RuntimeError) as caught:
            try:
                raise failure
            finally:
                report_maintenance(backend, broken_notice)
        assert caught.value is failure
        assert "maintenance inspection unavailable" in caplog.text
        assert "notice sink unavailable" in caplog.text
    finally:
        backend.close()


def test_local_publication_failure_survives_failed_temporary_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Local bucket cleanup retains the primary write failure and logs debris."""
    import logging

    from usagebassoon.buckets.local import durable_replace

    failure = OSError("primary snapshot sync failure")

    def failed_sync(_descriptor: int) -> None:
        raise failure

    def failed_unlink(_path: Path, *, missing_ok: bool = False) -> None:
        assert missing_ok
        raise PermissionError("secondary snapshot cleanup failure")

    monkeypatch.setattr("usagebassoon.buckets.local.os.fsync", failed_sync)
    monkeypatch.setattr(Path, "unlink", failed_unlink)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "disabled", False)
    try:
        with pytest.raises(OSError) as caught:
            durable_replace(tmp_path / "manifest.json", b"{}")
        assert caught.value is failure
        assert not (tmp_path / "manifest.json").exists()
        assert "secondary snapshot cleanup failure" in caplog.text
    finally:
        monkeypatch.undo()
