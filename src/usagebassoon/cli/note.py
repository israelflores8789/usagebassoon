# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""note.py — Typer commands for user-owned session notes."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import tempfile
from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.backends.base import UpsertResult, close_backend
from usagebassoon.cli._utils import configured_backend
from usagebassoon.curation import (
    NoteAssignment,
    edit_note,
    get_note,
    remove_note,
    set_note,
)

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
        str, typer.Option("--client", help="Client that owns the session.")
    ],
    session_id: Annotated[str, typer.Option("--session", help="Session to edit.")],
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Edit an existing session note in VISUAL or EDITOR."""
    command = _editor_command()
    configuration, backend = configured_backend(config)
    temporary_path: Path | None = None
    try:
        existing = get_note(
            backend,
            source_id=configuration.source_id,
            client=client,
            session_id=session_id,
        )
        if existing is None:
            raise typer.BadParameter(
                "no note exists for this session; use 'bassoon note set'"
            )
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
        result = edit_note(
            backend,
            _assignment(configuration.source_id, client, session_id, edited),
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
        str, typer.Option("--client", help="Client that owns the session.")
    ],
    session_id: Annotated[str, typer.Option("--session", help="Session to clear.")],
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Use this configuration file."),
    ] = None,
) -> None:
    """Remove a session note."""
    configuration, backend = configured_backend(config)
    try:
        assignment = _assignment(configuration.source_id, client, session_id, "present")
        deleted = remove_note(backend, assignment)
    finally:
        close_backend(backend, context="removing a note")
    action = "Removed" if deleted else "No note exists for"
    typer.echo(f"{action} session {session_id!r}.")
