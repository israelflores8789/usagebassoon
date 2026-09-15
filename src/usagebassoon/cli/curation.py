# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""curation.py — Typer commands for user-owned tags and session notes."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.backends.base import StorageBackend
from usagebassoon.config import ConfigurationError, ConfigurationManager, open_backend
from usagebassoon.curation import TagAssignment, add_tag, set_note


def _open_configured_backend(
    config: Path | None,
) -> tuple[StorageBackend, str]:
    """Open the backend resolved solely from the configured settings.

    Args:
        config: Explicit path supplied by ``--config``, when present.

    Returns:
        Open configured backend and its source namespace.

    Raises:
        typer.BadParameter: If configuration loading or backend opening fails.
    """
    try:
        configuration = ConfigurationManager(config).load()
        backend = open_backend(configuration)
        backend.apply_ddl()
    except (ConfigurationError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    return backend, configuration.source_id


def tag(
    tag_name: Annotated[str, typer.Argument(help="Tag label to assign.")],
    client: Annotated[
        str | None,
        typer.Option("--client", help="Client that owns the tag target."),
    ] = None,
    workspace: Annotated[
        str | None,
        typer.Option("--workspace", help="Assign the tag to this workspace."),
    ] = None,
    session_id: Annotated[
        str | None,
        typer.Option("--session", help="Assign the tag to this session."),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Use this configuration file instead of the environment/default path.",
        ),
    ] = None,
) -> None:
    """Assign a tag at client, workspace, or session scope.

    Client and workspace tags are independent peer scopes. A session tag
    requires ``--client`` and ``--session`` together.
    """
    if workspace is not None and (client is not None or session_id is not None):
        raise typer.BadParameter(
            "--workspace cannot be combined with --client or --session"
        )
    if session_id is not None and client is None:
        raise typer.BadParameter("--session requires --client")
    if workspace is not None:
        scope = "workspace"
    elif session_id is not None:
        scope = "session"
    elif client is not None:
        scope = "client"
    else:
        raise typer.BadParameter("supply --workspace or --client")
    backend, source_id = _open_configured_backend(config)
    assignment = TagAssignment(
        scope=scope,
        source_id=source_id,
        client=client,
        tag=tag_name,
        workspace=workspace,
        session_id=session_id,
    )
    try:
        result = add_tag(backend, assignment)
    finally:
        backend.close()
    target = workspace or session_id or client or ""
    action = "Added" if result.inserted else "Already present"
    typer.echo(f"{action} tag {tag_name!r} for {scope} {target!r}.")


def note(
    note_text: Annotated[
        str,
        typer.Argument(help="Free-text annotation for the session."),
    ],
    client: Annotated[
        str,
        typer.Option("--client", help="Client that owns the session."),
    ],
    session_id: Annotated[
        str,
        typer.Option("--session", help="Session to annotate."),
    ],
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Use this configuration file instead of the environment/default path.",
        ),
    ] = None,
) -> None:
    """Create or update the single user-owned note for a session."""
    backend, source_id = _open_configured_backend(config)
    try:
        result = set_note(
            backend,
            source_id=source_id,
            client=client,
            session_id=session_id,
            note=note_text,
        )
    finally:
        backend.close()
    action = (
        "Added" if result.inserted else "Updated" if result.updated else "Unchanged"
    )
    typer.echo(f"{action} note for session {session_id!r}.")
