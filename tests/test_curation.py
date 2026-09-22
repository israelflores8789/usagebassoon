# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_curation.py — Tests for user-owned tag and note persistence."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

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
from usagebassoon.normalizer import normalize
from usagebassoon.persistence import persist_run


def test_effective_tags_inherit_from_client_and_workspace(
    collection_bundle: CollectionBundle,
) -> None:
    """Assert session tags include the two supported inherited scopes."""
    backend = DuckDBBackend(":memory:")
    target = next(row for row in collection_bundle.report_rows if row.workspace)
    timestamp = datetime(2026, 9, 15, tzinfo=UTC)
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(collection_bundle))
        assert (
            add_tag(
                backend,
                TagAssignment(
                    scope="client",
                    source_id=collection_bundle.source_id,
                    client=target.client,
                    tag="project-alpha",
                ),
                at=timestamp,
            ).inserted
            == 1
        )
        assert (
            add_tag(
                backend,
                TagAssignment(
                    scope="workspace",
                    source_id=collection_bundle.source_id,
                    workspace=target.workspace,
                    tag="shared-workspace",
                ),
                at=timestamp,
            ).inserted
            == 1
        )
        assert (
            add_tag(
                backend,
                TagAssignment(
                    scope="session",
                    source_id=collection_bundle.source_id,
                    client=target.client,
                    session_id=target.session_id,
                    tag="important-session",
                ),
                at=timestamp,
            ).inserted
            == 1
        )

        tags = backend.query(
            "SELECT tag, tag_scope FROM session_tags "
            f"WHERE source_id = '{collection_bundle.source_id}' "
            f"AND client = '{target.client}' "
            f"AND session_id = '{target.session_id}' ORDER BY tag"
        ).to_pylist()
        assert tags == [
            {"tag": "important-session", "tag_scope": "session"},
            {"tag": "project-alpha", "tag_scope": "client"},
            {"tag": "shared-workspace", "tag_scope": "workspace"},
        ]
    finally:
        backend.close()


def test_session_note_is_updatable_without_replacing_its_creation_time(
    collection_bundle: CollectionBundle,
) -> None:
    """Assert notes are session-only and retain their original creation time."""
    backend = DuckDBBackend(":memory:")
    target = collection_bundle.report_rows[0]
    created = datetime(2026, 9, 14, tzinfo=UTC)
    updated = created + timedelta(days=1)
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(collection_bundle))
        assert (
            set_note(
                backend,
                NoteAssignment(
                    source_id=collection_bundle.source_id,
                    client=target.client,
                    session_id=target.session_id,
                    note="First annotation",
                ),
                at=created,
            ).inserted
            == 1
        )
        assert (
            set_note(
                backend,
                NoteAssignment(
                    source_id=collection_bundle.source_id,
                    client=target.client,
                    session_id=target.session_id,
                    note="Revised annotation",
                ),
                at=updated,
            ).updated
            == 1
        )
        assert backend.query(
            "SELECT note, note_created_at, note_updated_at FROM noted_sessions "
            f"WHERE client = '{target.client}' "
            f"AND session_id = '{target.session_id}'"
        ).to_pylist() == [
            {
                "note": "Revised annotation",
                "note_created_at": created,
                "note_updated_at": updated,
            }
        ]
    finally:
        backend.close()


def test_tags_do_not_cross_source_namespaces(
    collection_bundle: CollectionBundle,
) -> None:
    """Assert a client tag resolves only onto sessions from its own source."""
    backend = DuckDBBackend(":memory:")
    alternate_source = "22222222-2222-4222-8222-222222222222"
    target = collection_bundle.report_rows[0]
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(collection_bundle))
        persist_run(
            backend,
            normalize(
                replace(
                    collection_bundle,
                    run_id=str(uuid4()),
                    source_id=alternate_source,
                )
            ),
        )
        add_tag(
            backend,
            TagAssignment(
                scope="client",
                source_id=collection_bundle.source_id,
                client=target.client,
                tag="source-one-only",
            ),
        )
        assert backend.query(
            "SELECT DISTINCT source_id FROM session_tags WHERE tag = 'source-one-only'"
        ).to_pylist() == [{"source_id": collection_bundle.source_id}]
    finally:
        backend.close()


def test_tag_assignment_rejects_invalid_scope_targets() -> None:
    """Assert invalid scope combinations fail before reaching a backend."""
    with pytest.raises(ValueError, match="workspace tags require"):
        TagAssignment(
            scope="workspace",
            source_id="source",
            client="codex",
            tag="project-alpha",
        )
    with pytest.raises(ValueError, match="session tags require"):
        TagAssignment(scope="session", source_id="source", tag="project-alpha")


def test_tag_rename_preserves_creation_time_and_updates_timestamp() -> None:
    """Assert a rename changes only a complete assignment's label and freshness."""
    backend = DuckDBBackend(":memory:")
    created = datetime(2026, 9, 14, tzinfo=UTC)
    renamed = created + timedelta(days=1)
    assignment = TagAssignment(
        scope="client",
        source_id="11111111-1111-4111-8111-111111111111",
        client="codex",
        tag="old",
    )
    try:
        backend.apply_ddl()
        add_tag(backend, assignment, at=created)
        result = rename_tag(backend, assignment, "new", at=renamed)
        assert result.renamed
        assert backend.query(
            "SELECT tag, created_at, updated_at FROM tags"
        ).to_pylist() == [{"tag": "new", "created_at": created, "updated_at": renamed}]
    finally:
        backend.close()


def test_tag_rename_leaves_source_when_destination_exists() -> None:
    """Assert an existing destination prevents any rename mutation."""
    backend = DuckDBBackend(":memory:")
    source = TagAssignment(
        scope="client", source_id="source", client="codex", tag="old"
    )
    destination = TagAssignment(
        scope="client", source_id="source", client="codex", tag="new"
    )
    try:
        backend.apply_ddl()
        add_tag(backend, source)
        add_tag(backend, destination)
        result = rename_tag(backend, source, "new")
        assert not result.renamed
        assert result.destination_exists
        assert backend.query("SELECT tag FROM tags ORDER BY tag").to_pylist() == [
            {"tag": "new"},
            {"tag": "old"},
        ]
    finally:
        backend.close()


def test_curated_remove_requires_the_complete_identity() -> None:
    """Assert curation deletions leave unrelated tags and notes intact."""
    backend = DuckDBBackend(":memory:")
    first_note = NoteAssignment("source", "codex", "first", "first note")
    second_note = NoteAssignment("source", "codex", "second", "second note")
    first_tag = TagAssignment(
        scope="client", source_id="source", client="codex", tag="first"
    )
    second_tag = TagAssignment(
        scope="client", source_id="source", client="codex", tag="second"
    )
    try:
        backend.apply_ddl()
        set_note(backend, first_note)
        set_note(backend, second_note)
        add_tag(backend, first_tag)
        add_tag(backend, second_tag)
        assert remove_note(backend, first_note) == 1
        assert remove_tag(backend, first_tag) == 1
        assert backend.query("SELECT session_id FROM notes").to_pylist() == [
            {"session_id": "second"}
        ]
        assert backend.query("SELECT tag FROM tags").to_pylist() == [{"tag": "second"}]
    finally:
        backend.close()
