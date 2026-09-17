# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""curation.py — User-owned tags and notes outside collected usage facts."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import pyarrow as pa

from usagebassoon.backends.base import (
    CuratedIdentity,
    CuratedRenameError,
    CuratedRenameResult,
    StorageBackend,
    UpsertResult,
)

type TagScope = Literal["client", "workspace", "session"]

_LOG = logging.getLogger("usagebassoon")


@dataclass(frozen=True, slots=True)
class NoteAssignment:
    """One user-owned note at a complete session identity.

    Attributes:
        source_id: Stable namespace of the session.
        client: Client that owns the session.
        session_id: Session receiving the note.
        note: Free-text user annotation.
    """

    source_id: str
    client: str
    session_id: str
    note: str

    def __post_init__(self) -> None:
        """Validate the complete identity and meaningful note content."""
        if not self.source_id.strip():
            raise ValueError("note source_id must not be empty")
        if not self.client.strip():
            raise ValueError("note client must not be empty")
        if not self.session_id.strip():
            raise ValueError("note session_id must not be empty")
        if not self.note.strip():
            raise ValueError("note must not be empty")

    @property
    def identity(self) -> CuratedIdentity:
        """Return the complete backend identity for this note."""
        return CuratedIdentity(
            "notes",
            (
                ("source_id", self.source_id),
                ("client", self.client),
                ("session_id", self.session_id),
            ),
        )


@dataclass(frozen=True, slots=True)
class TagAssignment:
    """A user-owned tag at one supported scope.

    A tag may be assigned to a client, workspace, or individual session. The
    ``session_tags`` view resolves the first two scopes onto matching sessions.

    Attributes:
        scope: The curation scope of the assignment.
        source_id: Stable namespace of the tag target.
        tag: User-defined tag label.
        client: Client identifier required for client and session scope.
        workspace: Workspace identifier required only for workspace scope.
        session_id: Session identifier required only for session scope.
    """

    scope: TagScope
    source_id: str
    tag: str
    client: str | None = None
    workspace: str | None = None
    session_id: str | None = None

    def __post_init__(self) -> None:
        """Validate that scope and target identify exactly one supported layer."""
        if not self.tag.strip():
            raise ValueError("tag must not be empty")
        if not self.source_id.strip():
            raise ValueError("tag source_id must not be empty")
        if self.scope == "client" and (
            not self.client or self.workspace is not None or self.session_id is not None
        ):
            raise ValueError("client tags require a client and no other target")
        if self.scope == "workspace" and (
            not self.workspace or self.client is not None or self.session_id is not None
        ):
            raise ValueError("workspace tags require only a workspace")
        if self.scope == "session" and (
            not self.client or self.workspace is not None or not self.session_id
        ):
            raise ValueError("session tags require a client and session")

    @property
    def identity(self) -> CuratedIdentity:
        """Return the complete backend identity for this tag assignment."""
        return CuratedIdentity(
            "tags",
            (
                ("source_id", self.source_id),
                ("scope", self.scope),
                ("client", self.client or ""),
                ("workspace", self.workspace or ""),
                ("session_id", self.session_id or ""),
                ("tag", self.tag),
            ),
        )


def add_tag(
    backend: StorageBackend,
    assignment: TagAssignment,
    *,
    at: datetime | None = None,
) -> UpsertResult:
    """Persist a tag assignment without duplicating an existing assignment."""
    timestamp = at or datetime.now(UTC)
    data = pa.table(
        {
            "source_id": [assignment.source_id],
            "scope": [assignment.scope],
            "client": [assignment.client or ""],
            "workspace": [assignment.workspace or ""],
            "session_id": [assignment.session_id or ""],
            "tag": [assignment.tag],
            "created_at": [timestamp],
            "updated_at": [timestamp],
        }
    )
    return backend.upsert(
        "tags",
        data,
        ("source_id", "scope", "client", "workspace", "session_id", "tag"),
        (),
    )


def rename_tag(
    backend: StorageBackend,
    source: TagAssignment,
    new_tag: str,
    *,
    at: datetime | None = None,
) -> CuratedRenameResult:
    """Rename one tag assignment without changing its target or creation time."""
    destination = TagAssignment(
        scope=source.scope,
        source_id=source.source_id,
        client=source.client,
        workspace=source.workspace,
        session_id=source.session_id,
        tag=new_tag,
    )
    try:
        return backend.rename_curated(
            source.identity,
            destination.identity,
            updated_at=at or datetime.now(UTC),
        )
    except Exception as error:
        _LOG.warning(
            "tag rename did not complete; the original assignment was retained",
            exc_info=True,
        )
        if isinstance(error, CuratedRenameError):
            raise
        raise CuratedRenameError("atomic tag rename did not complete") from error


def remove_tag(backend: StorageBackend, assignment: TagAssignment) -> int:
    """Remove one complete tag assignment without touching usage facts."""
    return backend.delete_curated(assignment.identity)


def get_note(
    backend: StorageBackend, *, source_id: str, client: str, session_id: str
) -> NoteAssignment | None:
    """Read one session note from the dialect-paired curation view."""
    identity = CuratedIdentity(
        "notes",
        (("source_id", source_id), ("client", client), ("session_id", session_id)),
    )
    row = next(
        iter(
            backend.query(
                "SELECT note FROM session_notes WHERE source_id = :source_id "
                "AND client = :client AND session_id = :session_id",
                identity.parameters(),
            ).to_pylist()
        ),
        None,
    )
    return (
        None
        if row is None
        else NoteAssignment(note=str(row["note"]), **dict(identity.values))
    )


def set_note(
    backend: StorageBackend,
    assignment: NoteAssignment,
    *,
    at: datetime | None = None,
) -> UpsertResult:
    """Create or replace the single user-owned note for a session."""
    timestamp = at or datetime.now(UTC)
    data = pa.table(
        {
            "source_id": [assignment.source_id],
            "client": [assignment.client],
            "session_id": [assignment.session_id],
            "note": [assignment.note],
            "created_at": [timestamp],
            "updated_at": [timestamp],
        }
    )
    return backend.upsert(
        "notes",
        data,
        ("source_id", "client", "session_id"),
        ("note",),
    )


def edit_note(
    backend: StorageBackend,
    assignment: NoteAssignment,
    *,
    at: datetime | None = None,
) -> UpsertResult:
    """Replace an existing note and reject an unknown session note identity."""
    if (
        get_note(
            backend,
            source_id=assignment.source_id,
            client=assignment.client,
            session_id=assignment.session_id,
        )
        is None
    ):
        raise LookupError("no note exists for this session; use note set to create one")
    return set_note(backend, assignment, at=at)


def remove_note(backend: StorageBackend, assignment: NoteAssignment) -> int:
    """Remove one complete session note without touching usage facts."""
    return backend.delete_curated(assignment.identity)
