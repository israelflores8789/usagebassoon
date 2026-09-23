# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_app.py — Typer application registration tests."""

from importlib.metadata import version as distribution_version

import pytest
from typer.testing import CliRunner

import usagebassoon
from usagebassoon.cli.app import app

COMMANDS = (
    "audit",
    "collect",
    "doctor",
    "export",
    "init",
    "note",
    "query",
    "report",
    "restore",
    "snapshot",
    "schedule",
    "tag",
)


def test_root_help_lists_every_registered_command() -> None:
    """Expose every public command from the root help page."""
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in COMMANDS:
        assert command in result.output
    assert "--version" in result.output


def test_version_matches_the_python_api_and_distribution() -> None:
    """Report the installed version consistently across CLI and Python API."""
    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0
    expected = distribution_version("usagebassoon")
    assert result.output.strip() == f"usagebassoon {expected}"
    assert usagebassoon.__version__ == expected


@pytest.mark.parametrize("command", COMMANDS)
def test_each_command_has_working_help(command: str) -> None:
    """Keep every registered command discoverable through Typer help."""
    result = CliRunner().invoke(app, [command, "--help"])

    assert result.exit_code == 0
