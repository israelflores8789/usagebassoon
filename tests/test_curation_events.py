# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_curation_events.py — Curation lifetimes across native writer strategies."""

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from threading import Lock
from typing import cast, override
from unittest.mock import MagicMock
from uuid import UUID

import pyarrow as pa
import pytest
import sqlglot
from google.api_core.exceptions import ServiceUnavailable
from google.cloud import bigquery
from sqlglot import exp

from tests._sql_parity import statements
from usagebassoon.archiver import SnapshotArchiver
from usagebassoon.backends.base import SnapshotRead, StorageBackend
from usagebassoon.backends.bigquery import BigQueryBackend
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
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS, normalize
from usagebassoon.persistence import persist_run
from usagebassoon.storage_model import DEBUG_TABLES, STATE_KEYS


class _AppendWarehouse(BigQueryBackend):
    """Execute BigQuery writers and replay its SQL using a local test engine."""

    def __init__(self) -> None:
        """Install the BigQuery physical schema and transpiled canonical views."""
        super().__init__(
            "usagebassoon-test",
            "usagebassoon_emulated",
            client=cast(bigquery.Client, MagicMock(spec=bigquery.Client)),
        )
        self.engine = DuckDBBackend(":memory:")
        for statement in statements("bigquery", "ddl.sql"):
            if isinstance(statement, exp.Create):
                statement.set("properties", None)
                self.engine.connection.execute(statement.sql(dialect="duckdb"))
        for statement in statements("bigquery", "views.sql"):
            if statement.this.name != "compaction_backlog":
                self.engine.connection.execute(statement.sql(dialect="duckdb"))

    @override
    def query(self, sql: str, parameters: Mapping[str, str] | None = None) -> pa.Table:
        """Read the actual canonical views while avoiding cloud transport."""
        return self.engine.query(sql.replace("`", '"'), parameters)

    @override
    def _load(
        self,
        data: pa.Table,
        destination: str,
        *,
        disposition: str,
        schema: Sequence[bigquery.SchemaField] | None = None,
    ) -> None:
        """Record the canonical batch produced by the real BigQuery adapter."""
        assert disposition == "WRITE_APPEND"
        self.engine.append(destination.rsplit(".", 1)[-1], data)

    def compact(self) -> None:
        """Replay shipped curation winner SQL and replacements, retaining tombstones."""
        sql = (
            resources.files("usagebassoon.sql.bigquery")
            .joinpath("compaction.sql")
            .read_text()
        )
        parsed = [
            statement for statement in sqlglot.parse(sql, read="bigquery") if statement
        ]
        with self.engine.transaction():
            for table in ("tags", "notes"):
                self.engine.connection.execute(
                    f"DROP TABLE IF EXISTS candidates_{table}"
                )
                self.engine.connection.execute(f"DROP TABLE IF EXISTS winners_{table}")
                self.engine.connection.execute(
                    f"CREATE TEMP TABLE candidates_{table} AS "
                    f"SELECT DISTINCT source_id FROM raw_{table}"
                )
                self.engine.connection.execute(f"DROP TABLE IF EXISTS keys_{table}")
                for statement in parsed:
                    if isinstance(statement, exp.Create) and statement.this.name in {
                        f"keys_{table}",
                        f"winners_{table}",
                    }:
                        for relation in statement.find_all(exp.Table):
                            relation.set("version", None)
                        self.engine.connection.execute(statement.sql(dialect="duckdb"))
                    elif (
                        isinstance(statement, (exp.Delete, exp.Insert))
                        and (
                            statement.this.this.name
                            if isinstance(statement.this, exp.Schema)
                            else statement.this.name
                        )
                        == table
                    ):
                        self.engine.connection.execute(statement.sql(dialect="duckdb"))

    @override
    def read_snapshot_tables(self, tables: Sequence[str]) -> SnapshotRead:
        """Capture canonical BigQuery views with the production portable schemas."""
        with self.engine.transaction():
            captured = datetime.now(UTC)
            materialized = {
                table: self.query(
                    f"SELECT * FROM "
                    f"{'replay_' if table in DEBUG_TABLES else 'current_'}{table}"
                )
                .select(CANONICAL_TABLE_SCHEMAS[table].names)
                .cast(CANONICAL_TABLE_SCHEMAS[table])
                for table in tables
            }
        return SnapshotRead(captured, materialized)

    @override
    def close(self) -> None:
        """Close the local replay engine."""
        self.engine.close()


@pytest.fixture(params=["duckdb", "bigquery"])
def warehouse(request: pytest.FixtureRequest) -> Iterator[StorageBackend]:
    """Provide each native curation writer against its canonical read strategy."""
    backend = (
        _AppendWarehouse() if request.param == "bigquery" else DuckDBBackend(":memory:")
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
    if isinstance(warehouse, _AppendWarehouse):
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
    if isinstance(warehouse, _AppendWarehouse):
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
            if phase and isinstance(warehouse, _AppendWarehouse):
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
    if isinstance(warehouse, _AppendWarehouse):
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
    if isinstance(warehouse, _AppendWarehouse):
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
    if isinstance(warehouse, _AppendWarehouse):
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


def test_partial_publication_never_marks_missing_facts_complete(
    collection_bundle: CollectionBundle, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fact and ledger failures leave useful data visible and safe to recollect."""
    backend = _AppendWarehouse()
    bundle = normalize(collection_bundle)
    original_append = backend.append
    lock = Lock()
    failed_table = "daily_stats"

    def append(table: str, data: pa.Table) -> None:
        """Serialize the local replay engine while preserving independent loads."""
        with lock:
            if table == failed_table:
                raise ServiceUnavailable("controlled publication failure")
            original_append(table, data)

    monkeypatch.setattr(backend, "append", append)
    try:
        with pytest.raises(ServiceUnavailable):
            persist_run(backend, bundle)
        assert backend.query("SELECT * FROM current_sessions").num_rows > 0
        assert backend.query("SELECT * FROM current_daily_stats").num_rows == 0
        assert backend.query("SELECT * FROM collection_status").num_rows == 0
        failed_table = "collection_ledger"
        with pytest.raises(ServiceUnavailable):
            persist_run(backend, bundle)
        assert backend.query("SELECT * FROM current_daily_stats").num_rows > 0
        assert backend.query("SELECT * FROM collection_status").num_rows == 0
        failed_table = ""
        persist_run(backend, bundle)
        assert backend.query("SELECT * FROM collection_runs").num_rows == 1
        assert backend.query("SELECT * FROM collection_status").num_rows > 0
        for table in ("sessions", "daily_stats", "price_versions"):
            assert backend.query(f"SELECT * FROM current_{table}").num_rows == (
                bundle.tables[table].num_rows
            )
    finally:
        backend.close()


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
    if isinstance(warehouse, _AppendWarehouse):
        warehouse.compact()
        warehouse.engine.connection.execute(f"DELETE FROM raw_{table}")
        assert warehouse.query(f"SELECT * FROM current_{table}").to_pylist() == [winner]
