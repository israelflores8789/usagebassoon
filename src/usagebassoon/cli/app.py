# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""app.py — Typer application assembly for the UsageBassoon command line."""

import typer

from usagebassoon.cli.collect import collect
from usagebassoon.cli.curation import note, tag
from usagebassoon.cli.doctor import doctor
from usagebassoon.cli.export import export
from usagebassoon.cli.init import init
from usagebassoon.cli.query import query
from usagebassoon.cli.report import report
from usagebassoon.cli.restore import restore
from usagebassoon.cli.runs import runs
from usagebassoon.cli.snapshot import snapshot

app = typer.Typer(
    name="usagebassoon",
    help="Persistent analytics over tokscale's stateless JSON exports.",
    no_args_is_help=True,
)


@app.callback()
def main() -> None:
    """Run the UsageBassoon command-line interface."""


app.command(name="doctor")(doctor)
app.command(name="collect")(collect)
app.command(name="export")(export)
app.command(name="init")(init)
app.command(name="tag")(tag)
app.command(name="note")(note)
app.command(name="query")(query)
app.command(name="report")(report)
app.command(name="restore")(restore)
app.command(name="runs")(runs)
app.command(name="snapshot")(snapshot)
