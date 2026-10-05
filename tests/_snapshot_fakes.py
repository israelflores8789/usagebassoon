# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_snapshot_fakes.py — Shared storage doubles for snapshot tests."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pyarrow as pa

from tests._sql_parity import normalized_records
from usagebassoon.backends.base import SnapshotRead, SnapshotStream, StorageBackend
from usagebassoon.curation import NoteAssignment, TagAssignment, add_tag, set_note
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import normalize
from usagebassoon.persistence import persist_run
from usagebassoon.storage_model import (
    CANONICAL_TABLE_SCHEMAS,
    DEBUG_TABLES,
    SNAPSHOT_TABLES,
    note_id_for_session,
)


class TableBackend:
    """Minimal Arrow reader used to force snapshot capture outcomes."""

    dialect = "duckdb"

    def snapshot_provenance(self) -> dict[str, object]:
        """Use the canonical DuckDB baseline for portable test snapshots."""
        from usagebassoon.schema_assets import SCHEMA_VERSION, schema_hash

        return {
            "source_backend": "duckdb",
            "backend_schema_version": SCHEMA_VERSION,
            "backend_schema_hash": schema_hash("duckdb"),
        }

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

    def read_snapshot_tables(self, tables: tuple[str, ...]) -> SnapshotRead:
        """Return fixed test tables as one materialized capture."""
        return SnapshotRead(
            datetime.now(UTC),
            {table: self.query(f"SELECT * FROM {table}") for table in tables},
        )

    @contextmanager
    def stream_snapshot(self, tables: Sequence[str]) -> Generator[SnapshotStream]:
        """Return canonical streams with an injectable capture failure."""
        values: dict[str, list[pa.RecordBatch]] = {}
        for table in tables:
            self.query(f"SELECT * FROM {table}")
            schema = CANONICAL_TABLE_SCHEMAS[table]
            data = pa.Table.from_batches([], schema=schema)
            if table == "notes":
                now = datetime.now(UTC)
                data = pa.Table.from_pylist(
                    [
                        {
                            "event_id": "event",
                            "note_id": note_id_for_session(
                                "source", "codex", "session"
                            ),
                            "source_id": "source",
                            "client": "codex",
                            "session_id": "session",
                            "note": "note",
                            "created_at": now,
                            "updated_at": now,
                            "collected_at": now,
                            "op": "upsert",
                            "op_id": None,
                        }
                    ],
                    schema=schema,
                )
            values[table] = data.to_batches()
        yield SnapshotStream(datetime.now(UTC), values)


def seed_recovery_data(
    backend: StorageBackend, collection: CollectionBundle, sources: tuple[str, ...]
) -> dict[str, list[dict[str, object]]]:
    """Seed all domains and return independently queried recovery expectations."""
    for source_id in sources:
        persist_run(
            backend,
            normalize(replace(collection, source_id=source_id, run_id=str(uuid4()))),
        )
    session = collection.report_rows[0]
    add_tag(
        backend, TagAssignment("client", sources[0], "recovery", client=session.client)
    )
    for source_id in sources:
        set_note(
            backend,
            NoteAssignment(
                source_id, session.client, session.session_id, f"note for {source_id}"
            ),
        )
    stamp = datetime.now(UTC)
    common: dict[str, object] = {
        "source_id": sources[0],
        "run_id": str(uuid4()),
        "event_id": str(uuid4()),
        "created_at": stamp,
        "collected_at": stamp,
        "resolved": False,
        "observation_count": 1,
    }
    backend.append(
        "reconciliation_issues",
        pa.Table.from_pylist(
            [
                {
                    **common,
                    "check_name": "tokens",
                    "issue_key": "recovery",
                    "message": "recovery evidence",
                }
            ],
            schema=CANONICAL_TABLE_SCHEMAS["reconciliation_issues"],
        ),
    )
    backend.append(
        "schema_drift_events",
        pa.Table.from_pylist(
            [
                {
                    **common,
                    "event_id": str(uuid4()),
                    "domain": "models",
                    "tokscale_ver": collection.graph.meta.version,
                    "contract_tokscale_ver": collection.graph.meta.version,
                    "drift_key": "recovery",
                    "drift_kind": "unknown_field",
                    "path": "entries[].future",
                    "detail": "recovery evidence",
                }
            ],
            schema=CANONICAL_TABLE_SCHEMAS["schema_drift_events"],
        ),
    )
    expected: dict[str, list[dict[str, object]]] = {}
    for table in SNAPSHOT_TABLES:
        relation = ("replay_" if table in DEBUG_TABLES else "current_") + table
        expected[table] = normalized_records(backend.query(f"SELECT * FROM {relation}"))
    return expected
