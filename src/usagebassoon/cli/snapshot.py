# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""snapshot.py — Snapshot creation, discovery, auditing, and protected lifecycle."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.archiver import SnapshotArchiver
from usagebassoon.backends.base import close_backend
from usagebassoon.cli._output import render_records
from usagebassoon.cli._utils import configured_backend, snapshot_archiver
from usagebassoon.cli.spinner import spinner
from usagebassoon.config import ConfigurationManager
from usagebassoon.snapshot.catalog import Catalog

snapshot_app = typer.Typer(
    invoke_without_command=True, help="Create and manage portable private snapshots."
)


def read_archiver(config: Path | None) -> SnapshotArchiver:
    """Open archive settings independently of the configured destination backend."""
    return SnapshotArchiver.for_read(ConfigurationManager(config).path)


def notice(message: str) -> None:
    """Keep archive warnings and progress separate from structured stdout."""
    typer.echo(message, err=True)


def confirm_location(archiver: SnapshotArchiver, selection: str) -> None:
    """Confirm explicit access outside enabled publication destinations."""
    if not archiver.selection_enabled(selection):
        message = (
            "Warning: the given snapshot location is not enabled in the config.toml. "
            "Would you like to proceed anyway? [Y/n]:"
        )
        while True:
            notice(message)
            answer = sys.stdin.readline()
            if not answer:
                raise typer.Abort()
            answer = answer.strip().lower()
            if answer in {"", "y", "yes"}:
                return
            if answer in {"n", "no"}:
                raise typer.Abort()
            notice("Please enter Y or n.")


@snapshot_app.callback()
def snapshot(
    ctx: typer.Context,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    no_pin: Annotated[
        bool, typer.Option("--no-pin", help="Create an unpinned manual snapshot.")
    ] = False,
    automatic: Annotated[
        bool,
        typer.Option(
            "--automatic",
            help="Capture only due scheduled/weekly obligations without pinning.",
        ),
    ] = False,
) -> None:
    """Capture and pin when no management subcommand is selected."""
    if ctx.invoked_subcommand is not None:
        ctx.obj = config
        return
    configuration, backend = configured_backend(config)
    try:
        with spinner(configuration):
            archiver = snapshot_archiver(configuration)
            if not automatic and not archiver.destination_uris:
                archiver = SnapshotArchiver(
                    str(configuration.snapshots.local.path),
                    timeout_seconds=configuration.snapshots.timeout_seconds,
                )
            uri = archiver.write(
                backend,
                run_id="scheduled" if automatic else "manual",
                manual=not automatic,
                pin=not automatic and not no_pin,
            )
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    finally:
        close_backend(backend, context="writing a snapshot")
    if uri is None:
        notice("Snapshot skipped: no obligation is due or an archive is reserved.")
    else:
        typer.echo(
            f"Created private raw snapshot at {uri}"
            f"{' (pinned)' if not no_pin and not automatic else ''}."
        )


def _config(ctx: typer.Context, config: Path | None) -> Path | None:
    """Allow configuration options before or after a management subcommand."""
    return (
        config if config is not None else ctx.obj if isinstance(ctx.obj, Path) else None
    )


@snapshot_app.command(name="list")
def list_snapshots(
    ctx: typer.Context,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    selection: Annotated[
        str,
        typer.Option(
            "--from-snapshot",
            help="latest, snapshot ID, directory, manifest path, or URI.",
        ),
    ] = "latest",
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit JSON instead of YAML.")
    ] = False,
) -> None:
    """List every available snapshot copy globally by capture time."""
    try:
        archiver = read_archiver(_config(ctx, config))
        rows = archiver.reader.listing(selection)
        disabled = next(
            (
                str(row["uri"])
                for row in rows
                if not archiver.selection_enabled(str(row["uri"]))
            ),
            None,
        )
        if disabled is not None:
            confirm_location(archiver, disabled)
        render_records(
            rows,
            columns=(),
            format="json" if json_output else "yaml",
            save=None,
            json_payload={"snapshots": rows},
        )
    except typer.Abort:
        raise
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error


@snapshot_app.command(name="inspect")
def inspect_snapshot(
    ctx: typer.Context,
    selection: Annotated[
        str,
        typer.Option(
            "--from-snapshot",
            help="latest, snapshot ID, directory, manifest path, or URI.",
        ),
    ] = "latest",
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit JSON instead of YAML.")
    ] = False,
) -> None:
    """Explain manifest provenance and compatibility without backend writes."""
    try:
        archiver = read_archiver(_config(ctx, config))
        reader = archiver.reader
        candidates, _ = reader.candidates(selection, recovery=True, warning=notice)
        manifest = reader.raw_manifest(candidates[0])[0]
        confirm_location(archiver, candidates[0].uri)
        render_records(
            [],
            columns=(),
            format="json" if json_output else "yaml",
            save=None,
            json_payload={
                "uri": candidates[0].uri,
                "manifest": manifest,
                "compatibility": reader.compatibility(candidates[0]),
            },
        )
    except typer.Abort:
        raise
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error


@snapshot_app.command(name="audit")
def audit_snapshot(
    ctx: typer.Context,
    selection: Annotated[
        str,
        typer.Option(
            "--from-snapshot",
            help="latest, snapshot ID, directory, manifest path, or URI.",
        ),
    ] = "latest",
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit JSON instead of YAML.")
    ] = False,
) -> None:
    """Audit completeness, object integrity, schemas, and recoverability."""
    try:
        archiver = read_archiver(_config(ctx, config))
        with archiver.reader.prepare(selection, warning=notice) as prepared:
            confirm_location(archiver, prepared.candidate.uri)
            payload: dict[str, object] = {
                "id": prepared.candidate.identifier,
                "uri": prepared.candidate.uri,
                "captured_at": prepared.candidate.captured_at,
                "valid": True,
                "rows": prepared.rows,
                "warnings": prepared.warnings,
            }
            render_records(
                [],
                columns=(),
                format="json" if json_output else "yaml",
                save=None,
                json_payload=payload,
            )
    except typer.Abort:
        raise
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error


@snapshot_app.command(name="pin")
def pin_snapshot(
    ctx: typer.Context,
    identifier: str,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """Pin every configured copy of an ID, or only an explicitly selected URI."""
    try:
        read_archiver(_config(ctx, config)).pin(identifier)
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(f"Pinned {identifier}.")


@snapshot_app.command(name="delete")
def delete_snapshot(
    ctx: typer.Context,
    identifier: str,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """Retire selected copies after displaying their exact locations."""
    archiver = read_archiver(_config(ctx, config))
    try:
        candidates = archiver.reader.lifecycle_candidates(identifier)
        for candidate in candidates:
            state, _ = candidate.bucket.read_json(f"{candidate.identifier}/state.json")
            notice(
                f"Delete {candidate.uri}: "
                f"pinned={state.get('pinned') if state else 'unknown'}, "
                f"roles={state.get('roles') if state else 'unknown'}, "
                f"retired={state.get('retired') if state else 'unknown'}"
            )
        if (
            typer.prompt(
                "This permanently removes the selected copies. Type DELETE",
                default="",
                show_default=False,
            )
            != "DELETE"
        ):
            raise typer.Abort()
        archiver.delete_candidates(candidates)
    except typer.Abort:
        raise
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(f"Deleted {identifier}.")


@snapshot_app.command(name="copy")
def copy_snapshot(
    ctx: typer.Context,
    destination: str,
    selection: Annotated[
        str,
        typer.Option(
            "--from-snapshot",
            help="latest, snapshot ID, directory, manifest path, or URI.",
        ),
    ] = "latest",
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """Verify and copy a complete snapshot between local and GCS archives."""
    try:
        archiver = read_archiver(_config(ctx, config))
        with archiver.reader.prepare(
            selection, warning=notice, transform=False
        ) as prepared:
            confirm_location(archiver, prepared.candidate.uri)
        uri = archiver.copy(selection, destination)
    except typer.Abort:
        raise
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(f"Copied snapshot to {uri}.")


@snapshot_app.command(name="repair")
def repair_catalog(
    ctx: typer.Context,
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """Reconstruct missing catalogs from audited complete snapshot directories."""
    archiver = read_archiver(_config(ctx, config))
    try:
        for bucket in archiver.reader.buckets:
            catalog = Catalog(bucket, archiver.max_snapshots)
            with catalog.hold():
                catalog.repair()
                catalog.cleanup_abandoned()
    except (OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo("Reconstructed archive catalogs.")


@snapshot_app.command(name="policy")
def archive_policy(
    ctx: typer.Context,
    max_snapshots: Annotated[int, typer.Option("--max-snapshots", min=1)],
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
) -> None:
    """Explicitly reconcile the shared destination retention ceiling."""
    archiver = read_archiver(_config(ctx, config))
    for bucket in archiver.reader.buckets:
        with Catalog(bucket, max_snapshots).hold() as catalog:

            def change(document: dict[str, object]) -> None:
                """Reconcile policy under the same CAS as reservation authority."""
                document["policy"] = {"max_snapshots": max_snapshots, "weekly_slots": 4}

            catalog.mutate(change)
    typer.echo(
        f"Archive policy now retains {max_snapshots} unpinned scheduled snapshots "
        "and four weekly slots."
    )
