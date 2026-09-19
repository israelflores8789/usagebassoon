# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""tag.py — Typer commands for user-owned tag assignments."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.backends.base import CuratedRenameError, close_backend
from usagebassoon.cli._utils import configured_backend
from usagebassoon.curation import TagAssignment, add_tag, remove_tag, rename_tag

tag_app = typer.Typer(help="Manage user-owned tag assignments.", no_args_is_help=True)


def _assignment(
    source_id: str,
    tag: str,
    client: str | None,
    workspace: str | None,
    session_id: str | None,
) -> TagAssignment:
    """Build one validated tag assignment from the target options."""
    if workspace is not None and (client is not None or session_id is not None):
        raise typer.BadParameter(
            "--workspace cannot be combined with --client or --session"
        )
    if session_id is not None and client is None:
        raise typer.BadParameter("--session requires --client")
    scope = (
        "workspace"
        if workspace is not None
        else "session"
        if session_id is not None
        else "client"
    )
    if client is None and workspace is None:
        raise typer.BadParameter("supply --workspace or --client")
    try:
        return TagAssignment(
            scope=scope,
            source_id=source_id,
            tag=tag,
            client=client,
            workspace=workspace,
            session_id=session_id,
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error


def _target(assignment: TagAssignment) -> str:
    """Return the display value for one tag target."""
    return assignment.workspace or assignment.session_id or assignment.client or ""


@tag_app.command("add")
def add(
    tag_name: Annotated[str, typer.Argument(help="Tag label to assign.")],
    client: Annotated[
        str | None, typer.Option("--client", help="Client target.")
    ] = None,
    workspace: Annotated[
        str | None, typer.Option("--workspace", help="Workspace target.")
    ] = None,
    session_id: Annotated[
        str | None, typer.Option("--session", help="Session target.")
    ] = None,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """Assign a tag at client, workspace, or session scope."""
    configuration, backend = configured_backend(config)
    try:
        assignment = _assignment(
            configuration.source_id, tag_name, client, workspace, session_id
        )
        result = add_tag(backend, assignment)
    finally:
        close_backend(backend, context="adding a tag")
    action = "Added" if result.inserted else "Already present"
    typer.echo(
        f"{action} tag {tag_name!r} for {assignment.scope} {_target(assignment)!r}."
    )


@tag_app.command("rename")
def rename(
    old: Annotated[str, typer.Argument(help="Existing tag label.")],
    new: Annotated[str, typer.Argument(help="Replacement tag label.")],
    client: Annotated[
        str | None, typer.Option("--client", help="Client target.")
    ] = None,
    workspace: Annotated[
        str | None, typer.Option("--workspace", help="Workspace target.")
    ] = None,
    session_id: Annotated[
        str | None, typer.Option("--session", help="Session target.")
    ] = None,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """Rename one tag assignment at its exact scope and target."""
    configuration, backend = configured_backend(config)
    try:
        assignment = _assignment(
            configuration.source_id, old, client, workspace, session_id
        )
        try:
            result = rename_tag(backend, assignment, new)
        except CuratedRenameError as error:
            typer.echo(f"Warning: {error}; no tag assignment was changed.", err=True)
            raise typer.Exit(1) from error
    finally:
        close_backend(backend, context="renaming a tag")
    if result.destination_exists:
        typer.echo(f"Tag {new!r} is already assigned; kept {old!r} unchanged.")
    elif result.renamed:
        typer.echo(f"Renamed tag {old!r} to {new!r} for {_target(assignment)!r}.")
    else:
        typer.echo(f"No tag {old!r} exists for {_target(assignment)!r}.")


@tag_app.command("remove")
def remove(
    tag_name: Annotated[str, typer.Argument(help="Tag label to remove.")],
    client: Annotated[
        str | None, typer.Option("--client", help="Client target.")
    ] = None,
    workspace: Annotated[
        str | None, typer.Option("--workspace", help="Workspace target.")
    ] = None,
    session_id: Annotated[
        str | None, typer.Option("--session", help="Session target.")
    ] = None,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """Remove one tag assignment at its exact scope and target."""
    configuration, backend = configured_backend(config)
    try:
        assignment = _assignment(
            configuration.source_id, tag_name, client, workspace, session_id
        )
        deleted = remove_tag(backend, assignment)
    finally:
        close_backend(backend, context="removing a tag")
    action = "Removed" if deleted else "No tag exists for"
    typer.echo(f"{action} {assignment.scope} {_target(assignment)!r}.")
