# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_output.py — Tests for application-owned Rich presentation."""

from __future__ import annotations

from io import StringIO

from usagebassoon.cli._output import output_console


def test_output_console_records_plain_and_styled_rendering() -> None:
    """Allow dedicated presentation tests to inspect style independently of text."""
    console = output_console(
        file=StringIO(),
        record=True,
        force_terminal=True,
        color_system="standard",
    )
    console.print("warning", style="yellow")

    assert console.export_text(styles=True, clear=False) == "\x1b[33mwarning\x1b[0m\n"
    assert console.export_text(clear=False) == "warning\n"
