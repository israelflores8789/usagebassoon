# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_collect.py — Typer integration tests for collection delegation."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon.cli.app import app
from usagebassoon.config import UsageBassoonConfig
from usagebassoon.persistence import PersistSummary

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _write_config(path: Path, database: Path) -> None:
    """Write a local-DuckDB configuration for collection command tests."""
    path.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\n'
        f'local_database = "{database}"\n'
    )


def test_collect_reports_the_delegated_merge_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Display the run identity and merge counts returned by the collector."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usagebassoon.duckdb")

    def collect_run(configuration: UsageBassoonConfig) -> tuple[str, PersistSummary]:
        """Return a deterministic collector outcome for the CLI boundary."""
        assert configuration.path == config
        return "run-123", PersistSummary(inserted=4, updated=2, per_table={})

    monkeypatch.setattr("usagebassoon.cli.collect.collect_run", collect_run)

    result = CliRunner().invoke(app, ["collect", "--config", str(config)])

    assert result.exit_code == 0
    assert result.output == "Collected run run-123: 4 inserted, 2 updated.\n"


def test_collect_formats_configuration_errors_as_cli_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid exposing a traceback when the requested configuration is absent."""
    missing = tmp_path / "missing.toml"
    monkeypatch.setenv("FORCE_COLOR", "1")
    monkeypatch.setenv("TERM", "xterm-256color")

    result = CliRunner().invoke(app, ["collect", "--config", str(missing)])

    assert result.exit_code != 0
    assert "--config" in plain_cli_output(result.output)


def test_collect_formats_unexpected_errors_without_a_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log unexpected command errors and return a clean nonzero CLI status."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usagebassoon.duckdb")

    def collect_run(_configuration: UsageBassoonConfig) -> tuple[str, PersistSummary]:
        """Raise an unexpected exception from the delegated collector."""
        raise TypeError("unexpected collector failure")

    monkeypatch.setattr("usagebassoon.cli.collect.collect_run", collect_run)

    with caplog.at_level(logging.ERROR, logger="usagebassoon"):
        result = CliRunner().invoke(app, ["collect", "--config", str(config)])

    assert result.exit_code == 1
    assert "Collection failed unexpectedly; see the operational log." in result.output
    assert "Traceback" not in result.output
    assert "unexpected collection command failure" in caplog.text
