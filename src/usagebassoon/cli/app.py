# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""app.py — Typer application assembly for the UsageBassoon command line."""

import typer

from usagebassoon.cli.doctor import doctor

app = typer.Typer(
    name="usagebassoon",
    help="Persistent analytics over tokscale's stateless JSON exports.",
    no_args_is_help=True,
)


@app.callback()
def main() -> None:
    """Run the UsageBassoon command-line interface."""


app.command(name="doctor")(doctor)
