# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""curation.py — User-owned tags and notes outside collected usage facts."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import uuid4

import pyarrow as pa
from sqlglot import exp

from usagebassoon.backends.base import (
    CuratedIdentity,
    CuratedRenameError,
    CuratedRenameResult,
    StorageBackend,
    UpsertResult,
)
from usagebassoon.storage_model import normalize_note_id, note_id_for_session

type TagScope = Literal["client", "workspace", "session"]

_LOG = logging.getLogger("usagebassoon")
NOTE_PAGE_SIZE = 16
MAX_NOTE_DESCRIBE_IDS = 256
_NOTE_COLUMNS = "note_id, source_id, client, session_id, note, created_at, updated_at"


@dataclass(frozen=True, slots=True)
class NoteAssignment:
    """One user-owned note at a source/client/session identity.

    Attributes:
        source_id: Source namespace of the target session.
        client: Client that owns the session.
        session_id: Session receiving the note.
        note: Free-text user annotation.
    """

    source_id: str
    client: str
    session_id: str
    note: str

    @property
    def note_id(self) -> str:
        """Return the stable compact UUID for this session's note."""
        return note_id_for_session(self.source_id, self.client, self.session_id)

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
class NoteRecord:
    """A current note assignment with its creation and last-modified dates.

    Attributes:
        assignment: Existing session target and free-text annotation.
        created_at: Start of the current note lifetime.
        updated_at: Last modification of the winning observation.
    """

    assignment: NoteAssignment
    created_at: datetime
    updated_at: datetime

    @property
    def note_id(self) -> str:
        """Return the assignment's compact UUID handle."""
        return self.assignment.note_id

    def describe(self) -> dict[str, object]:
        """Return metadata for structured descriptions without the note text."""
        return {
            "note_id": self.note_id,
            "source_id": self.assignment.source_id,
            "client": self.assignment.client,
            "session_id": self.assignment.session_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


def _note_record(row: Mapping[str, object]) -> NoteRecord:
    """Read a canonical note and reject inconsistent identity or date metadata."""
    try:
        assignment = NoteAssignment(
            source_id=str(row["source_id"]),
            client=str(row["client"]),
            session_id=str(row["session_id"]),
            note=str(row["note"]),
        )
        if row["note_id"] != assignment.note_id:
            raise ValueError("stored note ID does not match its session identity")
        dates: list[datetime] = []
        for column in ("created_at", "updated_at"):
            value = row[column]
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise ValueError("stored note dates must include a timezone")
            dates.append(value.astimezone(UTC))
        return NoteRecord(assignment, dates[0], dates[1])
    except (KeyError, ValueError):
        _LOG.warning("could not read canonical note metadata", exc_info=True)
        raise


def get_notes_by_id(
    backend: StorageBackend, note_ids: Sequence[str]
) -> list[NoteRecord]:
    """Read active notes by compact or standard UUID in requested order.

    Duplicate IDs are returned once. Unknown or deleted IDs raise LookupError,
    so a multi-note description never silently omits requested notes.
    """
    if not 1 <= len(note_ids) <= MAX_NOTE_DESCRIBE_IDS:
        raise ValueError(f"provide between 1 and {MAX_NOTE_DESCRIBE_IDS} note IDs")
    identifiers = tuple(dict.fromkeys(normalize_note_id(value) for value in note_ids))
    bindings = {f"note_id_{index}": value for index, value in enumerate(identifiers)}
    parameters = ", ".join(f":{name}" for name in bindings)
    rows = backend.query(
        f"SELECT {_NOTE_COLUMNS} FROM session_notes "
        f"WHERE note_id IN ({parameters}) LIMIT {MAX_NOTE_DESCRIBE_IDS}",
        bindings,
    ).to_pylist()
    records = {}
    for row in rows:
        record = _note_record(row)
        records[record.note_id] = record
    missing = [value for value in identifiers if value not in records]
    if missing:
        raise LookupError("no active note exists for ID(s): " + ", ".join(missing))
    return [records[value] for value in identifiers]


def get_note_by_id(backend: StorageBackend, note_id: str) -> NoteRecord:
    """Read one active note without requiring the source/client/session key."""
    return get_notes_by_id(backend, [note_id])[0]


def list_notes(
    backend: StorageBackend,
    *,
    page: int = 1,
    source_id: str | None = None,
    after: NoteRecord | None = None,
) -> list[NoteRecord]:
    """Read at most 17 current notes, ordered by last modification and ID.

    The extra row signals whether another 16-note page exists. Interactive
    continuations use the last displayed date and ID rather than an offset.
    Explicit page selection uses an offset for noninteractive callers.
    """
    if page < 1 or (after is not None and page != 1):
        raise ValueError("use a positive page number or a continuation, not both")
    predicates: list[str] = []
    bindings: dict[str, str] = {}
    if source_id is not None:
        predicates.append("source_id = :source_id")
        bindings["source_id"] = source_id
    if after is not None:
        timestamp_type = exp.DataType.build("TIMESTAMPTZ").sql(dialect=backend.dialect)
        timestamp = f"CAST(:after_time AS {timestamp_type})"
        predicates.append(
            f"(updated_at < {timestamp} "
            f"OR (updated_at = {timestamp} AND note_id > :after_id))"
        )
        bindings["after_time"] = after.updated_at.isoformat()
        bindings["after_id"] = after.note_id
    where = " WHERE " + " AND ".join(predicates) if predicates else ""
    rows = backend.query(
        f"SELECT {_NOTE_COLUMNS} FROM session_notes{where} "
        f"ORDER BY updated_at DESC, note_id ASC LIMIT {NOTE_PAGE_SIZE + 1} "
        f"OFFSET {(page - 1) * NOTE_PAGE_SIZE}",
        bindings,
    ).to_pylist()
    return [_note_record(row) for row in rows]


@dataclass(frozen=True, slots=True)
class TagAssignment:
    """A user-owned tag at one supported scope.

    A tag may be assigned to a client, workspace, or individual session. The
    ``session_tags`` view resolves the first two scopes onto matching sessions.

    Attributes:
        scope: The curation scope of the assignment.
        source_id: Source emitting the mutation; provenance only.
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


def _current_assignment(
    backend: StorageBackend, identity: CuratedIdentity
) -> dict[str, object] | None:
    """Read current assignment fields from the dialect-paired curation view."""
    predicate = " AND ".join(f"{name} = :{name}" for name, _ in identity.target_values)
    rows = backend.query(
        f"SELECT * FROM current_{identity.table} WHERE {predicate}",
        identity.parameters(),
    ).to_pylist()
    return cast(dict[str, object], rows[0]) if rows else None


def add_tag(
    backend: StorageBackend,
    assignment: TagAssignment,
    *,
    at: datetime | None = None,
) -> UpsertResult:
    """Persist a tag assignment without duplicating an existing assignment."""
    timestamp = at or datetime.now(UTC)
    existing = _current_assignment(backend, assignment.identity)
    if existing is not None:
        return UpsertResult()
    data = pa.table(
        {
            "source_id": [assignment.source_id],
            "scope": [assignment.scope],
            "client": [assignment.client or ""],
            "workspace": [assignment.workspace or ""],
            "session_id": [assignment.session_id or ""],
            "tag": [assignment.tag],
            "created_at": [timestamp],
            "collected_at": [timestamp],
            "event_id": [str(uuid4())],
            "updated_at": [timestamp],
            "op": ["upsert"],
            "op_id": [str(uuid4())],
        }
    )
    return backend.upsert(
        "tags",
        data,
        ("scope", "client", "workspace", "session_id", "tag"),
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
                "SELECT note FROM session_notes "
                "WHERE source_id = :source_id "
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
    existing = _current_assignment(backend, assignment.identity)
    if existing is not None and existing["note"] == assignment.note:
        return UpsertResult()
    created_at = existing["created_at"] if existing is not None else timestamp
    data = pa.table(
        {
            "source_id": [assignment.source_id],
            "client": [assignment.client],
            "session_id": [assignment.session_id],
            "note_id": [assignment.note_id],
            "note": [assignment.note],
            "created_at": [created_at],
            "collected_at": [timestamp],
            "event_id": [str(uuid4())],
            "updated_at": [timestamp],
            "op": ["upsert"],
            "op_id": [str(uuid4())],
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


def edit_note_by_id(
    backend: StorageBackend,
    note_id: str,
    note: str,
    *,
    at: datetime | None = None,
) -> UpsertResult:
    """Edit a note by its UUID while retaining the existing session-key write path."""
    existing = get_note_by_id(backend, note_id)
    return edit_note(backend, replace(existing.assignment, note=note), at=at)


def remove_note_by_id(backend: StorageBackend, note_id: str) -> int:
    """Resolve a UUID and remove the note through its complete natural identity."""
    return remove_note(backend, get_note_by_id(backend, note_id).assignment)
