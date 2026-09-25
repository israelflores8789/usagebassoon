# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""report.py — Typer subapplication for personal-use terminal reports."""

from __future__ import annotations

import typer

from usagebassoon.cli.reports.daily import daily
from usagebassoon.cli.reports.graph import graph
from usagebassoon.cli.reports.models import models
from usagebassoon.cli.reports.sessions import sessions
from usagebassoon.cli.reports.summary import summary

report_app = typer.Typer(
    help="Render terminal usage reports.",
    no_args_is_help=True,
)

report_app.command("daily")(daily)
report_app.command("models")(models)
report_app.command("sessions")(sessions)
report_app.command("graph")(graph)
report_app.command("summary")(summary)
