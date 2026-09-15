# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""curation.py — User-owned tags and notes outside collected usage facts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import pyarrow as pa

from usagebassoon.backends.base import StorageBackend, UpsertResult

type TagScope = Literal["client", "workspace", "session"]


@dataclass(frozen=True, slots=True)
class TagAssignment:
    """A user-owned tag at one supported scope.

    A tag may be assigned to a client, workspace, or individual session. The
    ``session_tags`` view resolves the first two scopes onto matching sessions.

    Attributes:
        scope: The curation scope of the assignment.
        source_id: Stable namespace of the tag target.
        client: Client identifier required for client and session scope.
        tag: User-defined tag label.
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


def add_tag(
    backend: StorageBackend,
    assignment: TagAssignment,
    *,
    at: datetime | None = None,
) -> UpsertResult:
    """Persist a tag assignment without duplicating an existing assignment.

    Args:
        backend: Destination storage backend.
        assignment: Validated user-owned tag target.
        at: Creation timestamp; defaults to the current UTC time.

    Returns:
        Whether a new tag assignment was inserted.
    """
    created_at = at or datetime.now(UTC)
    data = pa.table(
        {
            "source_id": [assignment.source_id],
            "scope": [assignment.scope],
            "client": [assignment.client or ""],
            "workspace": [assignment.workspace or ""],
            "session_id": [assignment.session_id or ""],
            "tag": [assignment.tag],
            "created_at": [created_at],
        }
    )
    return backend.upsert(
        "tags",
        data,
        ("source_id", "scope", "client", "workspace", "session_id", "tag"),
        (),
    )


def set_note(
    backend: StorageBackend,
    *,
    source_id: str,
    client: str,
    session_id: str,
    note: str,
    at: datetime | None = None,
) -> UpsertResult:
    """Create or replace the single user-owned note for a session.

    Args:
        backend: Destination storage backend.
        source_id: Stable namespace of the session.
        client: Client that owns the session.
        session_id: Session receiving the note.
        note: Free-text user annotation.
        at: Write timestamp; defaults to the current UTC time.

    Returns:
        Whether a session note was inserted or materially updated.

    Raises:
        ValueError: If the target or note is empty.
    """
    if not source_id.strip():
        raise ValueError("note source_id must not be empty")
    if not client.strip():
        raise ValueError("note client must not be empty")
    if not session_id.strip():
        raise ValueError("note session_id must not be empty")
    if not note.strip():
        raise ValueError("note must not be empty")
    timestamp = at or datetime.now(UTC)
    data = pa.table(
        {
            "source_id": [source_id],
            "client": [client],
            "session_id": [session_id],
            "note": [note],
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
