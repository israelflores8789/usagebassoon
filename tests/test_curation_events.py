# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_curation_events.py — Curation lifetimes across native writer strategies."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pytest

from tests._bigquery_replay import BigQueryReplayBackend
from usagebassoon.archiver import SnapshotArchiver
from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.curation import (
    NoteAssignment,
    TagAssignment,
    add_tag,
    remove_note,
    remove_tag,
    rename_tag,
    set_note,
)
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS
from usagebassoon.storage_model import STATE_KEYS


@pytest.fixture(params=["duckdb", "bigquery"])
def warehouse(request: pytest.FixtureRequest) -> Iterator[StorageBackend]:
    """Provide each native curation writer against its canonical read strategy."""
    backend = (
        BigQueryReplayBackend()
        if request.param == "bigquery"
        else DuckDBBackend(":memory:")
    )
    if isinstance(backend, DuckDBBackend):
        backend.apply_ddl()
    try:
        yield backend
    finally:
        backend.close()


def test_assignment_lifetimes_and_no_op_timestamps(warehouse: StorageBackend) -> None:
    """Edits and rename preserve creation; explicit deletion starts a new lifetime."""
    created = datetime(2026, 9, 1, tzinfo=UTC)
    changed = created + timedelta(days=1)
    recreated = datetime.now(UTC) + timedelta(days=1)
    tag = TagAssignment("client", "source", "old", client="codex")
    note = NoteAssignment("source", "codex", "session", "first")
    add_tag(warehouse, tag, at=created)
    set_note(warehouse, note, at=created)
    original_tag = warehouse.query("SELECT * FROM current_tags").to_pylist()[0]
    original_note = warehouse.query("SELECT * FROM current_notes").to_pylist()[0]
    assert add_tag(warehouse, tag, at=changed).affected == 0
    assert set_note(warehouse, note, at=changed).affected == 0
    assert warehouse.query("SELECT * FROM current_tags").to_pylist() == [original_tag]
    assert warehouse.query("SELECT * FROM current_notes").to_pylist() == [original_note]
    assert rename_tag(warehouse, tag, "new", at=changed).renamed
    revised = NoteAssignment("source", "codex", "session", "edited")
    set_note(warehouse, revised, at=changed)
    for table in ("tags", "notes"):
        row = warehouse.query(f"SELECT * FROM current_{table}").to_pylist()[0]
        assert row["created_at"] == created
        assert row["updated_at"] == row["collected_at"] == changed
        assert row["op"] == "upsert"
        UUID(row["op_id"])
    if isinstance(warehouse, BigQueryReplayBackend):
        events = warehouse.query(
            "SELECT * FROM raw_tags ORDER BY collected_at, op"
        ).to_pylist()
        assert events[1]["op"] == "delete"
        assert events[2]["op"] == "upsert"
        assert events[1]["op_id"] == events[2]["op_id"]
        assert events[1]["event_id"] != events[2]["event_id"]
        before = {
            table: warehouse.query(f"SELECT * FROM current_{table}").to_pylist()
            for table in ("tags", "notes")
        }
        warehouse.compact()
        for table, rows in before.items():
            assert warehouse.query(f"SELECT * FROM current_{table}").to_pylist() == rows
    renamed = TagAssignment("client", "source", "new", client="codex")
    assert remove_tag(warehouse, renamed) == 1
    assert remove_note(warehouse, revised) == 1
    assert warehouse.query("SELECT * FROM current_tags").num_rows == 0
    assert warehouse.query("SELECT * FROM current_notes").num_rows == 0
    add_tag(warehouse, renamed, at=recreated)
    set_note(warehouse, revised, at=recreated)
    for table in ("tags", "notes"):
        row = warehouse.query(f"SELECT * FROM current_{table}").to_pylist()[0]
        assert row["created_at"] == row["updated_at"] == recreated
        assert (
            row["op_id"]
            != (original_tag if table == "tags" else original_note)["op_id"]
        )
    if isinstance(warehouse, BigQueryReplayBackend):
        warehouse.compact()
        for table in ("tags", "notes"):
            warehouse.engine.connection.execute(f"DELETE FROM raw_{table}")
            row = warehouse.query(f"SELECT * FROM current_{table}").to_pylist()[0]
            assert row["created_at"] == recreated
            assert row["op"] == "upsert"


def test_snapshot_restore_preserves_winning_curation_fields(
    warehouse: StorageBackend, tmp_path: Path
) -> None:
    """Carry the same lifetimes and operation metadata through portable Parquet."""
    created = datetime(2026, 9, 1, tzinfo=UTC)
    edited = created + timedelta(days=1)
    tag = TagAssignment("client", "source", "old", client="codex")
    add_tag(warehouse, tag, at=created)
    rename_tag(warehouse, tag, "new", at=edited)
    set_note(
        warehouse, NoteAssignment("source", "codex", "session", "first"), at=created
    )
    set_note(
        warehouse, NoteAssignment("source", "codex", "session", "edited"), at=edited
    )
    store = SnapshotArchiver(str(tmp_path / "archive"))
    destination = DuckDBBackend(":memory:")
    destination.apply_ddl()
    try:
        for phase in range(2):
            if phase and isinstance(warehouse, BigQueryReplayBackend):
                warehouse.compact()
            assert store.write(warehouse, run_id=f"snapshot-{phase}") is not None
            store.restore(destination)
            for table in ("tags", "notes"):
                original = warehouse.query(f"SELECT * FROM current_{table}").to_pylist()
                assert (
                    destination.query(f"SELECT * FROM current_{table}").to_pylist()
                    == original
                )
                destination.connection.execute(f"DELETE FROM {table}")
    finally:
        destination.close()


def test_global_tags_and_source_scoped_notes_survive_compaction(
    warehouse: StorageBackend,
) -> None:
    """Global tags reconcile provenance while same-ID notes remain independent."""
    created = datetime(2026, 9, 1, tzinfo=UTC)
    changed = created + timedelta(days=1)
    tag = TagAssignment("client", "source-a", "old", client="codex")
    add_tag(warehouse, tag, at=created)
    set_note(
        warehouse, NoteAssignment("source-a", "codex", "session", "first"), at=created
    )
    if isinstance(warehouse, BigQueryReplayBackend):
        warehouse.compact()
        for table in ("tags", "notes"):
            warehouse.engine.connection.execute(f"DELETE FROM raw_{table}")
    other_tag = TagAssignment("client", "source-b", "old", client="codex")
    assert add_tag(warehouse, other_tag, at=changed).affected == 0
    assert rename_tag(warehouse, other_tag, "new", at=changed).renamed
    set_note(
        warehouse, NoteAssignment("source-b", "codex", "session", "second"), at=changed
    )
    tag_rows = warehouse.query("SELECT * FROM current_tags").to_pylist()
    assert len(tag_rows) == 1
    assert tag_rows[0]["source_id"] == "source-b"
    assert tag_rows[0]["created_at"] == created
    assert tag_rows[0]["updated_at"] == changed
    expected_notes = [
        {"source_id": "source-a", "note": "first", "created_at": created},
        {"source_id": "source-b", "note": "second", "created_at": changed},
    ]
    assert (
        warehouse.query(
            "SELECT source_id, note, created_at FROM current_notes ORDER BY source_id"
        ).to_pylist()
        == expected_notes
    )
    if isinstance(warehouse, BigQueryReplayBackend):
        warehouse.compact()
        for table in ("tags", "notes"):
            warehouse.engine.connection.execute(f"DELETE FROM raw_{table}")
        assert (
            warehouse.query(
                "SELECT source_id, note, created_at FROM current_notes "
                "ORDER BY source_id"
            ).to_pylist()
            == expected_notes
        )
        assert warehouse.query("SELECT source_id FROM current_tags").to_pylist() == [
            {"source_id": "source-b"}
        ]
        assert (
            warehouse.query("SELECT * FROM current_tags WHERE tag = 'old'").num_rows
            == 0
        )
    assert (
        remove_note(
            warehouse, NoteAssignment("source-c", "codex", "session", "ignored")
        )
        == 0
    )
    assert (
        remove_note(
            warehouse, NoteAssignment("source-b", "codex", "session", "ignored")
        )
        == 1
    )
    assert (
        remove_tag(
            warehouse, TagAssignment("client", "source-c", "new", client="codex")
        )
        == 1
    )
    if isinstance(warehouse, BigQueryReplayBackend):
        warehouse.compact()
        for table in ("tags", "notes"):
            warehouse.engine.connection.execute(f"DELETE FROM raw_{table}")
            target = (
                " WHERE tag = 'new'"
                if table == "tags"
                else " WHERE source_id = 'source-b'"
            )
            assert (
                warehouse.query(f"SELECT * FROM current_{table}{target}").num_rows == 0
            )
            rows = warehouse.query(
                f"SELECT source_id, op FROM {table}{target}"
            ).to_pylist()
            assert rows
            assert all(
                row["source_id"] == ("source-c" if table == "tags" else "source-b")
                and row["op"] == "delete"
                for row in rows
            )
    assert warehouse.query("SELECT source_id, note FROM current_notes").to_pylist() == [
        {"source_id": "source-a", "note": "first"}
    ]


@pytest.mark.parametrize("table", ["tags", "notes"])
def test_curation_upsert_ties_use_event_uuid_in_both_writer_strategies(
    warehouse: StorageBackend, table: str
) -> None:
    """Equal-time upserts retain the greatest event ID regardless of row order."""
    stamp = datetime(2026, 9, 1, tzinfo=UTC)
    if table == "tags":
        add_tag(
            warehouse,
            TagAssignment("client", "source", "tag", client="codex"),
            at=stamp,
        )
    else:
        set_note(
            warehouse, NoteAssignment("source", "codex", "session", "note"), at=stamp
        )
    seed = warehouse.query(f"SELECT * FROM current_{table}").to_pylist()[0]
    winner = dict(
        seed,
        event_id="ffffffff-ffff-4fff-bfff-ffffffffffff",
        source_id="winner-source" if table == "tags" else seed["source_id"],
        collected_at=stamp + timedelta(days=1),
        created_at=stamp + timedelta(hours=1),
    )
    loser = dict(
        winner,
        event_id="00000000-0000-4000-8000-000000000001",
        source_id="loser-source" if table == "tags" else seed["source_id"],
    )
    if table == "notes":
        winner["note"], loser["note"] = "winning note", "losing note"
    for row in (winner, loser, winner):
        warehouse.upsert(
            table,
            pa.Table.from_pylist([row], schema=CANONICAL_TABLE_SCHEMAS[table]),
            STATE_KEYS[table],
            (),
        )
    assert warehouse.query(f"SELECT * FROM current_{table}").to_pylist() == [winner]
    if isinstance(warehouse, BigQueryReplayBackend):
        warehouse.compact()
        warehouse.engine.connection.execute(f"DELETE FROM raw_{table}")
        assert warehouse.query(f"SELECT * FROM current_{table}").to_pylist() == [winner]
