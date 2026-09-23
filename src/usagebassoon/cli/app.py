# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""app.py — Typer application assembly for the UsageBassoon command line."""

from typing import Annotated

import typer

from usagebassoon.cli.audit import audit
from usagebassoon.cli.collect import collect
from usagebassoon.cli.doctor import doctor
from usagebassoon.cli.export import export
from usagebassoon.cli.init import init
from usagebassoon.cli.note import note_app
from usagebassoon.cli.query import query
from usagebassoon.cli.report import report_app
from usagebassoon.cli.restore import restore
from usagebassoon.cli.schedule import schedule_app
from usagebassoon.cli.snapshot import snapshot
from usagebassoon.cli.tag import tag_app
from usagebassoon.version import __version__

app = typer.Typer(
    name="usagebassoon",
    help="Persistent analytics over tokscale's stateless JSON exports.",
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    """Print the installed version and exit when requested."""
    if value:
        typer.echo(f"usagebassoon {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show the UsageBassoon version and exit.",
        ),
    ] = False,
) -> None:
    """Run the UsageBassoon command-line interface."""


app.command(name="doctor")(doctor)
app.command(name="collect")(collect)
app.command(name="export")(export)
app.command(name="init")(init)
app.add_typer(tag_app, name="tag")
app.add_typer(note_app, name="note")
app.command(name="query")(query)
app.add_typer(report_app, name="report")
app.command(name="restore")(restore)
app.command(name="audit")(audit)
app.command(name="snapshot")(snapshot)
app.add_typer(schedule_app, name="schedule")
