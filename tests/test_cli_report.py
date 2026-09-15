# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_report.py — Typer integration tests for terminal reports."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create a populated local warehouse for report command tests."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\ndatabase = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.connection.execute(
        "INSERT INTO sessions "
        "(source_id, client, session_id, cost_usd, first_seen_at, last_seen_at, "
        "last_updated_at) "
        "VALUES (?, ?, ?, ?, NOW(), NOW(), NOW())",
        [SOURCE_ID, "codex", "ses_1", 1.25],
    )
    backend.connection.execute(
        "INSERT INTO session_model_stats "
        "(source_id, client, session_id, model, total_tokens, cost_usd, "
        "first_seen_at, last_seen_at, last_updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW(), NOW())",
        [SOURCE_ID, "codex", "ses_1", "gpt-test", 42, 1.25],
    )
    return config, backend


def test_report_renders_summary_and_interactive_sharing_reminder(
    tmp_path: Path,
) -> None:
    """Show warehouse totals and the raw-output sharing warning by default."""
    config, backend = _configured_store(tmp_path)
    backend.close()

    result = CliRunner().invoke(app, ["report", "--config", str(config)])

    assert result.exit_code == 0
    assert "UsageBassoon report" in result.output
    assert "1.250000" in result.output
    assert "gpt-test" in result.output
    assert "Re-run with --sanitize" in result.output


def test_report_save_and_sanitize_suppress_interactive_reminder(tmp_path: Path) -> None:
    """Keep saved and explicitly sanitized reports ready for deliberate sharing."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    saved = tmp_path / "report.txt"
    runner = CliRunner()

    saved_result = runner.invoke(
        app, ["report", "--save", str(saved), "--config", str(config)]
    )
    sanitized_result = runner.invoke(
        app, ["report", "--sanitize", "--config", str(config)]
    )

    assert saved_result.exit_code == 0
    assert "Re-run with --sanitize" not in saved.read_text()
    assert sanitized_result.exit_code == 0
    assert "Re-run with --sanitize" not in sanitized_result.output
