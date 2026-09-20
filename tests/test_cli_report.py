# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_report.py — Typer integration tests for terminal reports."""

from __future__ import annotations

from itertools import pairwise
from pathlib import Path

from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.cli.reports.graph import _tick_positions

LOCAL_SOURCE_ID = "11111111-1111-4111-8111-111111111111"
REMOTE_SOURCE_ID = "22222222-2222-4222-8222-222222222222"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create a populated local warehouse for terminal report tests."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{LOCAL_SOURCE_ID}"\n'
        f'backend = "duckdb"\n'
        f'database = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.connection.executemany(
        "INSERT INTO sessions "
        "(source_id, client, session_id, workspace, last_active, first_seen_at, "
        "last_seen_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, NOW(), NOW(), NOW())",
        [
            (
                LOCAL_SOURCE_ID,
                "codex",
                "ses_local_demonstration_identifier",
                "/work/atlas",
                "2026-09-11 14:30:00+00",
            ),
            (
                LOCAL_SOURCE_ID,
                "opencode",
                "ses_local_other",
                "/work/meteor",
                "2026-09-09 10:00:00+00",
            ),
            (
                REMOTE_SOURCE_ID,
                "codex",
                "ses_remote",
                "/work/atlas",
                "2026-09-12 09:00:00+00",
            ),
        ],
    )
    backend.connection.executemany(
        "INSERT INTO daily_stats "
        "(source_id, day, client, session_id, model, input_tokens, output_tokens, "
        "cache_read, cache_write, reasoning, total_tokens, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())",
        [
            (
                LOCAL_SOURCE_ID,
                "2026-09-10",
                "codex",
                "ses_local_demonstration_identifier",
                "gpt-test",
                100,
                20,
                50,
                10,
                5,
                185,
            ),
            (
                LOCAL_SOURCE_ID,
                "2026-09-11",
                "codex",
                "ses_local_demonstration_identifier",
                "gpt-mini",
                50,
                10,
                25,
                5,
                3,
                93,
            ),
            (
                LOCAL_SOURCE_ID,
                "2026-09-09",
                "opencode",
                "ses_local_other",
                "claude-test",
                80,
                16,
                40,
                8,
                4,
                148,
            ),
            (
                REMOTE_SOURCE_ID,
                "2026-09-12",
                "codex",
                "ses_remote",
                "gpt-test",
                999,
                99,
                499,
                50,
                10,
                1_657,
            ),
        ],
    )
    backend.connection.executemany(
        "INSERT INTO price_versions "
        "(source_id, day, model, source, price_input_per_token, "
        "price_output_per_token, price_cache_read_per_token, "
        "price_cache_write_per_token, observed_at, updated_at) "
        "VALUES (?, ?, ?, 'fixture', 0.000001, 0.000002, 0.0000005, "
        "0.0000007, NOW(), NOW())",
        [
            (LOCAL_SOURCE_ID, "2026-09-10", "gpt-test"),
            (LOCAL_SOURCE_ID, "2026-09-11", "gpt-mini"),
            (LOCAL_SOURCE_ID, "2026-09-09", "claude-test"),
            (REMOTE_SOURCE_ID, "2026-09-12", "gpt-test"),
        ],
    )
    backend.connection.execute(
        "INSERT INTO tags "
        "(scope, source_id, client, workspace, session_id, tag, created_at, "
        "updated_at) "
        "VALUES ('workspace', ?, '', '/work/atlas', '', 'focused', NOW(), NOW())",
        [LOCAL_SOURCE_ID],
    )
    return config, backend


def test_report_group_lists_v1_commands() -> None:
    """Show report help instead of implicitly rendering the legacy summary."""
    result = CliRunner().invoke(app, ["report", "--help"])

    assert result.exit_code == 0
    assert "daily" in result.output
    assert "sessions" in result.output
    assert "graph" in result.output


def test_daily_report_filters_local_tagged_usage(tmp_path: Path) -> None:
    """Aggregate only the configured source's effective-tag usage by day."""
    config, backend = _configured_store(tmp_path)
    backend.close()

    result = CliRunner().invoke(
        app,
        [
            "report",
            "daily",
            "--config",
            str(config),
            "--source",
            "local",
            "--tag",
            "focused",
            "--sanitize",
        ],
    )

    assert result.exit_code == 0
    assert "Daily Token Usage" in result.output
    assert "Cache \N{MULTIPLICATION SIGN}" in result.output
    assert "Cache R" in result.output
    assert "Cost/1M" in result.output
    assert "2026-09-10" in result.output
    assert "2026-09-11" in result.output
    assert "2026-09-12" not in result.output
    assert "Re-run with --sanitize" not in result.output


def test_daily_report_falls_back_to_tokscale_cost_for_unpriced_usage(
    tmp_path: Path,
) -> None:
    """Keep daily cost and Cost/1M available when the rate card is incomplete."""
    config, backend = _configured_store(tmp_path)
    backend.connection.execute(
        "UPDATE daily_stats SET tokscale_cost_usd = 1.234 "
        "WHERE source_id = ? AND day = '2026-09-10' AND model = 'gpt-test'",
        [LOCAL_SOURCE_ID],
    )
    backend.connection.execute(
        "DELETE FROM price_versions "
        "WHERE source_id = ? AND day = '2026-09-10' AND model = 'gpt-test'",
        [LOCAL_SOURCE_ID],
    )
    backend.close()

    result = CliRunner().invoke(
        app,
        [
            "report",
            "daily",
            "--config",
            str(config),
            "--source",
            "local",
            "--model",
            "gpt-test",
            "--sanitize",
        ],
    )

    assert result.exit_code == 0
    assert "$1.234" in result.output
    assert "$6,670.270" in result.output


def test_sessions_report_defaults_to_session_and_can_split_models(
    tmp_path: Path,
) -> None:
    """Show one session by default and one row per model on request."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    runner = CliRunner()

    session_result = runner.invoke(
        app,
        [
            "report",
            "sessions",
            "--config",
            str(config),
            "--source",
            "local",
            "--client",
            "codex",
            "--sanitize",
        ],
    )
    model_result = runner.invoke(
        app,
        [
            "report",
            "sessions",
            "--config",
            str(config),
            "--source",
            "local",
            "--client",
            "codex",
            "--by-model",
            "--width",
            "max",
            "--sanitize",
        ],
    )

    assert session_result.exit_code == 0
    assert "Session Token Usage" in session_result.output
    assert "Last Active" in session_result.output
    assert "Model" in session_result.output
    assert "Cost/1M" in session_result.output
    assert "$0.00" in session_result.output
    assert " +1" in session_result.output
    assert "…" in session_result.output
    assert model_result.exit_code == 0
    assert "Session Token Usage by Model" in model_result.output
    assert "gpt-test" in model_result.output
    assert "gpt-mini" in model_result.output


def test_graph_test_mode_needs_no_configuration_and_selects_metric() -> None:
    """Render a deterministic, bounded token graph without warehouse access."""
    result = CliRunner().invoke(
        app,
        [
            "report",
            "graph",
            "--test",
            "--metric",
            "total",
            "--since",
            "2026-09-01",
            "--until",
            "2026-09-10",
            "--sanitize",
        ],
    )

    assert result.exit_code == 0
    assert "Total Tokens" in result.output
    assert "2026-09-01" in result.output
    assert "2026-09-10" in result.output
    assert "01" in result.output
    assert "10" in result.output


def test_report_test_mode_uses_golden_fixture_statistics() -> None:
    """Render the newest golden daily totals with their unit and cost formatting."""
    result = CliRunner().invoke(
        app,
        [
            "report",
            "daily",
            "--test",
            "--limit",
            "1",
            "--sanitize",
        ],
    )

    assert result.exit_code == 0
    assert "2026-09-10" in result.output
    assert "391.5K" in result.output
    assert "6.7M" in result.output
    assert "$1.119" in result.output


def test_graph_ticks_are_uniformly_spaced_for_a_bounded_terminal() -> None:
    """Use explicit numeric ticks rather than uneven categorical auto-ticks."""
    positions = _tick_positions(20, 100)

    assert positions == list(range(0, 20, 2))
    assert all(right - left == 2 for left, right in pairwise(positions))


def test_report_test_mode_saves_without_sharing_reminder(tmp_path: Path) -> None:
    """Keep saved deterministic reports suitable for deliberate file sharing."""
    saved = tmp_path / "daily.txt"
    result = CliRunner().invoke(
        app,
        ["report", "daily", "--test", "--save", str(saved)],
    )

    assert result.exit_code == 0
    assert "Re-run with --sanitize" not in saved.read_text()
    assert "Daily Token Usage" in saved.read_text()
