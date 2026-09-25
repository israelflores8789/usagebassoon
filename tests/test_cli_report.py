# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_report.py — Typer integration tests for terminal reports."""

from __future__ import annotations

from itertools import pairwise
from pathlib import Path

from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.cli.reports.graph import _tick_positions
from usagebassoon.cli.reports.sessions import _model_label

LOCAL_SOURCE_ID = "11111111-1111-4111-8111-111111111111"
REMOTE_SOURCE_ID = "22222222-2222-4222-8222-222222222222"


def test_gemini_flash_model_labels_preserve_the_version_when_width_allows() -> None:
    """Keep Gemini Flash versions identifiable in the bounded session table."""
    assert _model_label("gemini-3.8-flash", True, 94) == "gemin…-flash"
    assert _model_label("gemini-3.8-flash", True, 100) == "ge…3.8-flash"
    assert _model_label("gemini-3.8-flash", True, 101) == "gem…3.8-flash"
    assert _model_label("gemini-3.8-flash", True, None) == "gemini-3.8-flash"
    assert _model_label("gpt-5.6-terra", True, 100) == "gpt-5…-terra"
    assert (
        _model_label("gemini-3.8-flash, gemini-3.7-flash", False, 100)
        == "ge…3.8-flash+1"
    )
    assert _model_label("gpt-5.6-luna, gpt-5.6-terra", False, 100) == "gpt-5.6-luna+1"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create a populated local warehouse for terminal report tests."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{LOCAL_SOURCE_ID}"\n'
        f'backend = "duckdb"\n'
        f'local_database = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.connection.executemany(
        "INSERT INTO sessions "
        "(source_id, client, session_id, workspace, created_at, last_active, "
        "first_seen_at, last_seen_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW(), NOW())",
        [
            (
                LOCAL_SOURCE_ID,
                "codex",
                "ses_local_demonstration_identifier",
                "/work/atlas",
                "2026-09-08 08:00:00+00",
                "2026-09-11 14:30:00+00",
            ),
            (
                LOCAL_SOURCE_ID,
                "opencode",
                "ses_local_other",
                "/work/meteor",
                "2026-09-09 08:00:00+00",
                "2026-09-09 10:00:00+00",
            ),
            (
                REMOTE_SOURCE_ID,
                "codex",
                "ses_remote",
                "/work/atlas",
                "2026-09-12 08:00:00+00",
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
    assert "models" in result.output
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
    assert "$1.23" in result.output
    assert "$6,670.270" in result.output


def test_models_report_weights_timing_after_source_and_tag_filters(
    tmp_path: Path,
) -> None:
    """Aggregate timing by model/client from the filtered daily components."""
    config, backend = _configured_store(tmp_path)
    backend.connection.execute(
        "UPDATE daily_stats SET perf_duration_ms = 100, perf_timed_tokens = 100 "
        "WHERE source_id = ? AND model = 'gpt-test'",
        [LOCAL_SOURCE_ID],
    )
    backend.connection.execute(
        "UPDATE daily_stats SET perf_duration_ms = 1000, perf_timed_tokens = 10000 "
        "WHERE source_id = ? AND model = 'gpt-test'",
        [REMOTE_SOURCE_ID],
    )
    backend.close()
    runner = CliRunner()
    command = [
        "report",
        "models",
        "--config",
        str(config),
        "--model",
        "gpt-test",
        "--width",
        "max",
        "--sanitize",
    ]

    all_sources = runner.invoke(app, command)
    local_tagged = runner.invoke(
        app, [*command, "--source", "local", "--tag", "focused"]
    )

    assert all_sources.exit_code == 0
    all_sources_text = plain_cli_output(all_sources.output)
    assert "Model Token Usage" in all_sources_text
    assert "Cache R" in all_sources_text
    assert "Cache \N{MULTIPLICATION SIGN}" in all_sources_text
    assert "ms/1K" in all_sources_text
    assert "Cost/1M" in all_sources_text
    assert "109" in all_sources_text
    assert local_tagged.exit_code == 0
    local_tagged_text = plain_cli_output(local_tagged.output)
    assert "1,000" in local_tagged_text
    assert "109" not in local_tagged_text


def test_models_report_test_mode_uses_daily_fixture_timing() -> None:
    """Show model timing from golden daily fixtures without a configured backend."""
    result = CliRunner().invoke(
        app, ["report", "models", "--test", "--width", "max", "--sanitize"]
    )

    assert result.exit_code == 0
    assert "gemini-3.7-flash" in result.output
    assert "gpt-5.6-terra" in result.output
    assert "ms/1K" in result.output
    assert "—" not in result.output


def test_models_report_excludes_duration_without_timed_tokens(tmp_path: Path) -> None:
    """Keep zero-token timing rows out of the grouped performance rate."""
    config, backend = _configured_store(tmp_path)
    backend.connection.execute(
        "UPDATE daily_stats SET perf_duration_ms = 100, perf_timed_tokens = 100 "
        "WHERE source_id = ? AND model = 'gpt-test'",
        [LOCAL_SOURCE_ID],
    )
    backend.connection.execute(
        "UPDATE daily_stats SET perf_duration_ms = 1000, perf_timed_tokens = 0 "
        "WHERE source_id = ? AND model = 'gpt-test'",
        [REMOTE_SOURCE_ID],
    )
    backend.close()

    result = CliRunner().invoke(
        app,
        [
            "report",
            "models",
            "--config",
            str(config),
            "--model",
            "gpt-test",
            "--width",
            "max",
            "--sanitize",
        ],
    )

    assert result.exit_code == 0
    assert "1,000" in plain_cli_output(result.output)


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
    assert "+1" in session_result.output
    assert "…" in session_result.output
    assert model_result.exit_code == 0
    assert "Session Token Usage by Model" in model_result.output
    assert "gpt-test" in model_result.output
    assert "gpt-mini" in model_result.output


def test_daily_and_models_date_bounds_compose_with_source_and_client(
    tmp_path: Path,
) -> None:
    """Apply usage-day bounds alongside source and client filters."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    runner = CliRunner()
    options = [
        "--config",
        str(config),
        "--source",
        "local",
        "--client",
        "codex",
        "--since",
        "2026-09-10",
        "--until",
        "2026-09-10",
        "--width",
        "max",
    ]

    daily = runner.invoke(app, ["report", "daily", *options])
    models = runner.invoke(app, ["report", "models", *options])

    assert daily.exit_code == 0
    daily_text = plain_cli_output(daily.output)
    assert "2026-09-10" in daily_text
    assert "2026-09-11" not in daily_text
    assert "2026-09-12" not in daily_text
    assert models.exit_code == 0
    models_text = plain_cli_output(models.output)
    assert "gpt-test" in models_text
    assert "gpt-mini" not in models_text
    assert "claude-test" not in models_text
    assert "185" in models_text
    assert "1,657" not in models_text


def test_session_date_bounds_select_whole_sessions_and_creation_mode(
    tmp_path: Path,
) -> None:
    """Filter complete session totals by the selected session timestamp."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    runner = CliRunner()
    options = [
        "report",
        "sessions",
        "--config",
        str(config),
        "--source",
        "local",
        "--client",
        "codex",
        "--width",
        "max",
    ]

    last_active = runner.invoke(
        app, [*options, "--since", "2026-09-11", "--until", "2026-09-11"]
    )
    by_model = runner.invoke(
        app,
        [
            *options,
            "--by-model",
            "--since",
            "2026-09-11",
            "--until",
            "2026-09-11",
        ],
    )
    by_created_at = runner.invoke(
        app,
        [
            *options,
            "--by-created-at",
            "--since",
            "2026-09-08",
            "--until",
            "2026-09-08",
        ],
    )
    created_by_model = runner.invoke(
        app,
        [
            *options,
            "--by-model",
            "--by-created-at",
            "--since",
            "2026-09-08",
            "--until",
            "2026-09-08",
        ],
    )
    created_at_excluded = runner.invoke(
        app,
        [*options, "--by-created-at", "--since", "2026-09-11"],
    )

    assert last_active.exit_code == 0
    last_active_text = plain_cli_output(last_active.output)
    assert "ses_local_demonstration_identifier" in last_active_text
    assert "278" in last_active_text
    assert "ses_local_other" not in last_active_text
    assert "ses_remote" not in last_active_text
    assert by_model.exit_code == 0
    by_model_text = plain_cli_output(by_model.output)
    assert "gpt-test" in by_model_text
    assert "gpt-mini" in by_model_text
    assert "185" in by_model_text
    assert "93" in by_model_text
    assert by_created_at.exit_code == 0
    created_text = plain_cli_output(by_created_at.output)
    assert "Created At" in created_text
    assert "Last Active" not in created_text
    assert "2026-09-08 08:00" in created_text
    assert "278" in created_text
    assert created_by_model.exit_code == 0
    created_by_model_text = plain_cli_output(created_by_model.output)
    assert "Created At" in created_by_model_text
    assert "gpt-test" in created_by_model_text
    assert "gpt-mini" in created_by_model_text
    assert created_at_excluded.exit_code == 0
    assert "ses_local_demonstration_identifier" not in plain_cli_output(
        created_at_excluded.output
    )


def test_session_creation_mode_changes_ordering(tmp_path: Path) -> None:
    """Order complete sessions by the timestamp named in the last column."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    runner = CliRunner()
    options = [
        "report",
        "sessions",
        "--config",
        str(config),
        "--source",
        "local",
        "--width",
        "max",
    ]

    active = runner.invoke(app, options)
    created = runner.invoke(app, [*options, "--by-created-at"])

    assert active.exit_code == 0
    active_text = plain_cli_output(active.output)
    assert active_text.index("ses_local_demonstration_identifier") < active_text.index(
        "ses_local_other"
    )
    assert created.exit_code == 0
    created_text = plain_cli_output(created.output)
    assert created_text.index("ses_local_other") < created_text.index(
        "ses_local_demonstration_identifier"
    )


def test_session_values_and_model_counts_remain_whole_at_eighty_columns() -> None:
    """Protect numeric values and aggregate-model counts at the minimum width."""
    result = CliRunner().invoke(
        app,
        [
            "report",
            "sessions",
            "--test",
            "--width",
            "80",
            "--limit",
            "8",
            "--sanitize",
        ],
    )

    assert result.exit_code == 0
    assert "25.1K" in result.output
    assert "178.2K" in result.output
    assert "$0.11" in result.output
    assert "$0.535" in result.output
    assert "luna+1" in result.output
    assert "flash+1" in result.output


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


def test_cost_graph_uses_two_decimal_y_axis_labels() -> None:
    """Render graph currency labels with the same two-decimal precision as Cost."""
    result = CliRunner().invoke(
        app,
        ["report", "graph", "--test", "--days", "3", "--sanitize"],
    )

    assert result.exit_code == 0
    assert "$6.99" in result.output
    assert "$6.990" not in result.output


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
    assert "$1.12" in result.output


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
