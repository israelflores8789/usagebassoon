# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""note.py — Typer commands for user-owned session notes."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.backends.base import StorageBackend, UpsertResult, close_backend
from usagebassoon.cli._output import render_records
from usagebassoon.cli._utils import configured_backend
from usagebassoon.cli.spinner import spinner
from usagebassoon.curation import (
    NOTE_PAGE_SIZE,
    NoteAssignment,
    edit_note,
    get_note,
    get_note_by_id,
    get_notes_by_id,
    list_notes,
    remove_note,
    set_note,
)
from usagebassoon.display import sanitize_display
from usagebassoon.storage_model import normalize_note_id

note_app = typer.Typer(help="Manage user-owned session notes.", no_args_is_help=True)


def _assignment(
    source_id: str, client: str, session_id: str, note: str
) -> NoteAssignment:
    """Build one validated session-note assignment for a CLI command."""
    try:
        return NoteAssignment(
            source_id=source_id, client=client, session_id=session_id, note=note
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error


def _note_action(result: UpsertResult) -> str:
    """Return the user-facing outcome word for one note upsert result."""
    return "Added" if result.inserted else "Updated" if result.updated else "Unchanged"


def _validate_target(
    note_id: str | None, client: str | None, session_id: str | None
) -> None:
    """Require either a UUID selector or the existing complete session selector."""
    if note_id is not None:
        if client is not None or session_id is not None:
            raise typer.BadParameter("use --id or --client with --session, not both")
        try:
            normalize_note_id(note_id)
        except ValueError as error:
            raise typer.BadParameter(str(error), param_hint="--id") from error
    elif client is None or session_id is None:
        raise typer.BadParameter("provide --id or both --client and --session")


def _selected_note(
    backend: StorageBackend,
    source_id: str,
    client: str | None,
    session_id: str | None,
    note_id: str | None,
) -> NoteAssignment | None:
    """Read a note with either selector, preserving the resolved source namespace."""
    if note_id is not None:
        try:
            return get_note_by_id(backend, note_id).assignment
        except (ValueError, LookupError) as error:
            raise typer.BadParameter(str(error), param_hint="--id") from error
    assert client is not None and session_id is not None
    return get_note(backend, source_id=source_id, client=client, session_id=session_id)


def _preview(note: str) -> str:
    """Return the first ten whitespace-separated words as one text line."""
    words = note.split()
    return " ".join(words[:10]) + (" …" if len(words) > 10 else "")


def _can_page() -> bool:
    """Detect terminal input and output before enabling interactive paging."""
    return sys.stdin.isatty() and sys.stdout.isatty()


def _next_page() -> bool:
    """Offer line-oriented continuation without requiring a full-screen UI."""
    while True:
        try:
            response = (
                typer.prompt(
                    "Enter for next 16 notes, q to quit", default="", show_default=False
                )
                .strip()
                .lower()
            )
        except typer.Abort:
            return False
        if response in {"", "n", "next"}:
            return True
        if response in {"q", "quit"}:
            return False


@note_app.command("list")
def list_command(
    page: Annotated[
        int, typer.Option("--page", min=1, help="Starting 16-note page.")
    ] = 1,
    source_id: Annotated[
        str | None,
        typer.Option("--source-id", help="Show only notes from this source."),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Output this page as JSON without prompting.")
    ] = False,
    no_pager: Annotated[
        bool, typer.Option("--no-pager", help="Print just the selected page.")
    ] = False,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """List recent notes, 16 at a time, with ten-word previews."""
    configuration, backend = configured_backend(config)
    try:
        interactive = not (json_output or no_pager) and _can_page()
        with spinner(configuration):
            records = list_notes(backend, page=page, source_id=source_id)
        while True:
            visible = records[:NOTE_PAGE_SIZE]
            rows = [
                {"note_id": record.note_id, "note": _preview(record.assignment.note)}
                for record in visible
            ]
            if json_output:
                render_records(
                    rows, columns=("note_id", "note"), format="json", save=None
                )
            elif rows:
                typer.echo(f"{'Note ID':<26}Note")
                for row in rows:
                    typer.echo(f"{row['note_id']:<26}{sanitize_display(row['note'])}")
            else:
                typer.echo("No notes found.")
            if len(records) <= NOTE_PAGE_SIZE:
                break
            if not interactive:
                typer.echo(f"More notes: use --page {page + 1}.", err=True)
                break
            if not _next_page():
                break
            with spinner(configuration):
                records = list_notes(backend, source_id=source_id, after=visible[-1])
            page += 1
    finally:
        close_backend(backend, context="listing notes")


@note_app.command("describe")
def describe(
    note_ids: Annotated[list[str], typer.Argument(help="One or more note UUIDs.")],
    json_output: Annotated[
        bool, typer.Option("--json", help="Output JSON instead of YAML.")
    ] = False,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """Describe note identities and creation/modification dates in YAML or JSON."""
    configuration, backend = configured_backend(config)
    try:
        try:
            with spinner(configuration):
                records = get_notes_by_id(backend, note_ids)
        except (ValueError, LookupError) as error:
            raise typer.BadParameter(str(error), param_hint="note_ids") from error
        rows = [record.describe() for record in records]
        render_records(
            rows,
            columns=(
                "note_id",
                "source_id",
                "client",
                "session_id",
                "created_at",
                "updated_at",
            ),
            format="json" if json_output else "yaml",
            save=None,
        )
    finally:
        close_backend(backend, context="describing notes")


@note_app.command("set")
def set(
    note_text: Annotated[
        str, typer.Argument(help="Free-text annotation for the session.")
    ],
    client: Annotated[
        str, typer.Option("--client", help="Client that owns the session.")
    ],
    session_id: Annotated[str, typer.Option("--session", help="Session to annotate.")],
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Create or replace a session note."""
    configuration, backend = configured_backend(config)
    try:
        with spinner(configuration):
            result = set_note(
                backend,
                _assignment(configuration.source_id, client, session_id, note_text),
            )
    finally:
        close_backend(backend, context="setting a note")
    typer.echo(f"{_note_action(result)} note for session {session_id!r}.")


def _editor_command() -> list[str]:
    """Return the selected editor command or explain how to configure one."""
    configured = os.environ.get("VISUAL") or os.environ.get("EDITOR")
    if not configured:
        raise typer.BadParameter(
            "set VISUAL or EDITOR to edit notes, or use 'bassoon note set'"
        )
    command = shlex.split(configured)
    if not command:
        raise typer.BadParameter("VISUAL or EDITOR must name an editor command")
    return command


@note_app.command("edit")
def edit(
    client: Annotated[
        str | None, typer.Option("--client", help="Client that owns the session.")
    ] = None,
    session_id: Annotated[
        str | None, typer.Option("--session", help="Session to edit.")
    ] = None,
    note_id: Annotated[
        str | None, typer.Option("--id", help="Note UUID to edit.")
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Edit an existing session note in VISUAL or EDITOR."""
    _validate_target(note_id, client, session_id)
    command = _editor_command()
    configuration, backend = configured_backend(config)
    temporary_path: Path | None = None
    try:
        with spinner(configuration):
            existing = _selected_note(
                backend, configuration.source_id, client, session_id, note_id
            )
        if existing is None:
            raise typer.BadParameter(
                "no note exists for this session; use 'bassoon note set'"
            )
        session_id = existing.session_id
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".txt", delete=False
        ) as temporary_file:
            temporary_file.write(existing.note)
            temporary_path = Path(temporary_file.name)
        try:
            completed = subprocess.run([*command, str(temporary_path)], check=False)
        except OSError as error:
            raise typer.BadParameter(
                f"could not run configured editor: {error}"
            ) from error
        if completed.returncode != 0:
            raise typer.BadParameter(
                "configured editor exited with status "
                f"{completed.returncode}; note was not changed"
            )
        edited = temporary_path.read_text(encoding="utf-8")
        if not edited.strip():
            raise typer.BadParameter(
                "blank notes are not saved; use 'bassoon note remove'"
            )
        if edited == existing.note:
            typer.echo(f"Unchanged note for session {session_id!r}.")
            return
        with spinner(configuration):
            result = edit_note(
                backend,
                _assignment(
                    existing.source_id, existing.client, existing.session_id, edited
                ),
            )
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                logging.getLogger("usagebassoon").exception(
                    "could not remove temporary note file %s", temporary_path
                )
        close_backend(backend, context="editing a note")
    typer.echo(f"{_note_action(result)} note for session {session_id!r}.")


@note_app.command("remove")
def remove(
    client: Annotated[
        str | None, typer.Option("--client", help="Client that owns the session.")
    ] = None,
    session_id: Annotated[
        str | None, typer.Option("--session", help="Session to clear.")
    ] = None,
    note_id: Annotated[
        str | None, typer.Option("--id", help="Note UUID to remove.")
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Remove a session note."""
    _validate_target(note_id, client, session_id)
    configuration, backend = configured_backend(config)
    try:
        with spinner(configuration):
            if note_id is not None:
                assignment = _selected_note(
                    backend, configuration.source_id, client, session_id, note_id
                )
                assert assignment is not None
                session_id = assignment.session_id
            else:
                assert client is not None and session_id is not None
                assignment = _assignment(
                    configuration.source_id, client, session_id, "present"
                )
            deleted = remove_note(backend, assignment)
    finally:
        close_backend(backend, context="removing a note")
    action = "Removed" if deleted else "No note exists for"
    typer.echo(f"{action} session {session_id!r}.")
