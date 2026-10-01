# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_report.py — Typer integration tests for terminal reports."""

from __future__ import annotations

from datetime import datetime
from itertools import pairwise
from pathlib import Path

import pytest
from rich.cells import cell_len
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.cli.reports._common import (
    SAMPLE_LOCAL_SOURCE_ID,
    ReportFilters,
    SessionSort,
    load_session_usage,
    numeric_value,
    sample_session_usage,
    truncate_middle,
)
from usagebassoon.cli.reports.graph import _tick_positions
from usagebassoon.cli.reports.sessions import (
    _format_duration,
    _model_label,
)

LOCAL_SOURCE_ID = "11111111-1111-4111-8111-111111111111"
REMOTE_SOURCE_ID = "22222222-2222-4222-8222-222222222222"


def test_gemini_flash_model_labels_preserve_the_version_when_width_allows() -> None:
    """Keep Gemini Flash versions identifiable in the bounded session table."""
    assert _model_label("gemini-3.8-flash", True, 100) == "ge…3.8-flash"
    assert _model_label("gemini-3.8-flash", True, 101) == "gem…3.8-flash"
    assert _model_label("gemini-3.8-flash", True, None) == "gemini-3.8-flash"
    assert _model_label("gpt-5.6-terra", True, 100) == "gpt-5…-terra"
    assert _model_label("gpt-5.6-terra", True, 105) == "gpt-5.6-terra"
    assert _model_label("gpt-5.6-terra, gpt-5.6-luna", False, 105) == "gpt-5.6-terra+1"
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
        "(event_id, source_id, client, session_id, workspace, created_at, last_active, "
        "first_seen_at, last_seen_at, collected_at) "
        "VALUES (UUID(), ?, ?, ?, ?, ?, ?, NOW(), NOW(), NOW())",
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
        "(event_id, source_id, day, client, session_id, model, "
        "input_tokens, output_tokens, "
        "cache_read, cache_write, reasoning, total_tokens, collected_at) "
        "VALUES (UUID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())",
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
        "(event_id, source_id, day, model, source, price_input_per_token, "
        "price_output_per_token, price_cache_read_per_token, "
        "price_cache_write_per_token, collected_at) "
        "VALUES (UUID(), ?, ?, ?, 'fixture', 0.000001, 0.000002, 0.0000005, "
        "0.0000007, NOW())",
        [
            (LOCAL_SOURCE_ID, "2026-09-10", "gpt-test"),
            (LOCAL_SOURCE_ID, "2026-09-11", "gpt-mini"),
            (LOCAL_SOURCE_ID, "2026-09-09", "claude-test"),
            (REMOTE_SOURCE_ID, "2026-09-12", "gpt-test"),
        ],
    )
    backend.connection.execute(
        "INSERT INTO tags "
        "(event_id, scope, source_id, client, workspace, session_id, tag, created_at, "
        "updated_at, collected_at) "
        "VALUES (UUID(), 'workspace', ?, '', '/work/atlas', '', "
        "'focused', NOW(), NOW(), NOW())",
        [LOCAL_SOURCE_ID],
    )
    return config, backend


def test_report_group_lists_supported_commands() -> None:
    """Expose the supported terminal reports in group help."""
    result = CliRunner().invoke(app, ["report", "--help"])

    assert result.exit_code == 0
    commands = plain_cli_output(result.output).split()
    for command in ("activity", "daily", "graph", "models", "sessions", "summary"):
        assert command in commands


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
    assert "Daily Token Usage" in plain_cli_output(result.output)
    assert "Cache \N{MULTIPLICATION SIGN}" in plain_cli_output(result.output)
    assert "Cache R" in plain_cli_output(result.output)
    assert "Cost/1M" in plain_cli_output(result.output)
    assert "2026-09-10" in plain_cli_output(result.output)
    assert "2026-09-11" in plain_cli_output(result.output)
    assert "2026-09-12" not in plain_cli_output(result.output)
    assert "Re-run with --sanitize" not in plain_cli_output(result.output)


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
    assert "$1.23" in plain_cli_output(result.output)
    assert "$6,670.270" in plain_cli_output(result.output)


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
    assert "gemini-3.7-flash" in plain_cli_output(result.output)
    assert "gpt-5.6-terra" in plain_cli_output(result.output)
    assert "ms/1K" in plain_cli_output(result.output)
    assert "—" not in plain_cli_output(result.output)


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
    assert "Session Token Usage" in plain_cli_output(session_result.output)
    assert "Last Active" in plain_cli_output(session_result.output)
    assert "Model" in plain_cli_output(session_result.output)
    assert "Cost/1M" in plain_cli_output(session_result.output)
    assert "$0.00" in plain_cli_output(session_result.output)
    assert "+1" in plain_cli_output(session_result.output)
    assert model_result.exit_code == 0
    assert "Session Token Usage by Model" in plain_cli_output(model_result.output)
    assert "gpt-test" in plain_cli_output(model_result.output)
    assert "gpt-mini" in plain_cli_output(model_result.output)


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
            "--sort",
            "created-at",
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
            "--sort",
            "created-at",
            "--since",
            "2026-09-08",
            "--until",
            "2026-09-08",
        ],
    )
    created_at_excluded = runner.invoke(
        app,
        [*options, "--sort", "created-at", "--since", "2026-09-11"],
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
    created = runner.invoke(app, [*options, "--sort", "created-at"])

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


def test_session_values_and_model_counts_remain_whole_at_floor() -> None:
    """Protect numeric values and aggregate-model counts at the minimum width."""
    result = CliRunner().invoke(
        app,
        [
            "report",
            "sessions",
            "--test",
            "--width",
            "100",
            "--limit",
            "8",
            "--sanitize",
        ],
    )

    assert result.exit_code == 0
    assert "25.1K" in plain_cli_output(result.output)
    assert "178.2K" in plain_cli_output(result.output)
    assert "$0.11" in plain_cli_output(result.output)
    assert "$0.535" in plain_cli_output(result.output)
    assert "luna+1" in plain_cli_output(result.output)
    assert "flash+1" in plain_cli_output(result.output)


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
    assert "Total Tokens" in plain_cli_output(result.output)
    assert "2026-09-01" in plain_cli_output(result.output)
    assert "2026-09-10" in plain_cli_output(result.output)
    assert "01" in plain_cli_output(result.output)
    assert "10" in plain_cli_output(result.output)


def test_cost_graph_uses_two_decimal_y_axis_labels() -> None:
    """Render graph currency labels with the same two-decimal precision as Cost."""
    result = CliRunner().invoke(
        app,
        ["report", "graph", "--test", "--days", "3", "--sanitize"],
    )

    assert result.exit_code == 0
    assert "$6.99" in plain_cli_output(result.output)
    assert "$6.990" not in plain_cli_output(result.output)


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
    assert "2026-09-10" in plain_cli_output(result.output)
    assert "391.5K" in plain_cli_output(result.output)
    assert "6.7M" in plain_cli_output(result.output)
    assert "$1.12" in plain_cli_output(result.output)


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


@pytest.mark.parametrize(
    ("milliseconds", "expected"),
    [
        (None, "—"),
        (0, "00s"),
        (89999, "89s"),
        (90000, "01m30s"),
        (5399999, "89m59s"),
        (5400000, "1h30m00s"),
        (36000000, "10h00m00s"),
    ],
)
def test_session_duration_thresholds(milliseconds: int | None, expected: str) -> None:
    """Use whole seconds and omit units below their requested thresholds."""
    assert _format_duration(milliseconds) == expected


@pytest.mark.parametrize("by_model", [False, True])
def test_session_duration_aggregation_and_sorting(
    tmp_path: Path, by_model: bool
) -> None:
    """Sum all daily durations and sort before limiting, independent of display."""
    config, backend = _configured_store(tmp_path)
    backend.connection.execute(
        "UPDATE daily_stats SET perf_duration_ms = CASE model "
        "WHEN 'gpt-test' THEN 60000 WHEN 'gpt-mini' THEN 30000 "
        "ELSE 5400000 END WHERE source_id = ?",
        [LOCAL_SOURCE_ID],
    )
    backend.connection.execute(
        "INSERT INTO daily_stats "
        "(event_id, source_id, day, client, session_id, model, total_tokens, "
        "perf_duration_ms, collected_at) "
        "VALUES (UUID(), ?, '2026-09-09', 'codex', "
        "'ses_local_demonstration_identifier', 'gpt-test', 1, 30000, NOW())",
        [LOCAL_SOURCE_ID],
    )
    backend.close()
    runner = CliRunner()
    command = [
        "report",
        "sessions",
        "--config",
        str(config),
        "--source",
        "local",
        "--width",
        "max",
    ]
    if by_model:
        command.append("--by-model")
    default = runner.invoke(app, command)
    assert default.exit_code == 0
    assert "Duration" not in plain_cli_output(default.output)
    displayed = runner.invoke(app, [*command, "--with-duration"])
    assert displayed.exit_code == 0
    text = plain_cli_output(displayed.output)
    assert text.index("Total") < text.index("Duration") < text.index("Cost")
    assert ("01m30s" if by_model else "02m00s") in text
    if by_model:
        assert "30s" in text
    sorted_result = runner.invoke(app, [*command, "--sort", "duration", "--limit", "1"])
    assert sorted_result.exit_code == 0
    sorted_text = plain_cli_output(sorted_result.output)
    assert "ses_local_other" in sorted_text
    assert "ses_local_demonstration_identifier" not in sorted_text
    assert "Duration" not in sorted_text
    bounded = runner.invoke(
        app,
        [
            *command,
            "--sort",
            "duration",
            "--with-duration",
            "--since",
            "2026-09-11",
            "--until",
            "2026-09-11",
        ],
    )
    assert bounded.exit_code == 0
    bounded_text = plain_cli_output(bounded.output)
    assert ("01m30s" if by_model else "02m00s") in bounded_text
    assert "ses_local_other" not in bounded_text


def test_sessions_reject_width_below_floor() -> None:
    """Fail before opening a backend when session width is below 100."""
    result = CliRunner().invoke(app, ["report", "sessions", "--width", "99"])
    assert result.exit_code == 2
    assert "at least 100" in plain_cli_output(result.output)


@pytest.mark.parametrize(
    ("width", "metrics", "by_model"),
    [
        pytest.param(105, (), False, id="models-expand"),
        pytest.param(100, ("--with-duration",), False, id="compact-dates"),
        pytest.param(104, ("--with-duration",), False, id="compact-upper-bound"),
        pytest.param(105, ("--with-duration",), False, id="timestamps-resume"),
        pytest.param(105, ("--with-duration",), True, id="model-detail"),
        pytest.param(100, ("--with-performance",), False, id="performance-at-floor"),
        pytest.param(
            100,
            ("--with-duration", "--with-performance"),
            False,
            id="both-metrics-width-floor",
        ),
        pytest.param(
            140,
            ("--with-duration", "--with-performance"),
            False,
            id="both-metrics-wide",
        ),
    ],
)
def test_session_optional_metric_layout(
    tmp_path: Path, by_model: bool, width: int, metrics: tuple[str, ...]
) -> None:
    """Preserve model versions/counts and timestamps at the supported widths."""
    saved = tmp_path / "sessions.txt"
    command = [
        "report",
        "sessions",
        "--test",
        "--sanitize",
        "--limit",
        "8",
        "--width",
        str(width),
        "--save",
        str(saved),
        *metrics,
    ]
    if by_model:
        command.append("--by-model")
    result = CliRunner().invoke(app, command)
    assert result.exit_code == 0
    text = plain_cli_output(result.output)
    effective_width = max(width, 120) if len(metrics) == 2 else width
    compact = "--with-duration" in metrics and effective_width < 105
    assert ("Active" if compact else "Last Active") in text
    rows = [line.split() for line in text.splitlines() if "$" in line]
    assert len(rows) == 8
    for row in rows:
        for identifier in row[:2]:
            assert not identifier.endswith("…")
    if width >= 105 or len(metrics) == 2:
        assert "gpt-5.6-terra" in text
    if compact:
        header = next(line for line in text.splitlines() if "Active" in line)
        active_end = header.index("Active") + len("Active")
        assert all(
            line.rstrip().endswith(line[active_end - 5 : active_end])
            and "-" in line[active_end - 5 : active_end]
            for line in text.splitlines()
            if "$" in line
        )
        assert "Last Active" not in text
        assert all(len(row[-1]) == 5 and "-" in row[-1] for row in rows)
    else:
        assert all(len(row[-1]) == 5 and ":" in row[-1] for row in rows)
    assert "3.8-flash" in text
    if not by_model:
        assert "luna+1" in text
        assert "flash+1" in text
    if "--with-performance" in metrics:
        assert "ms/1K" in text
    assert all(len(line) <= effective_width for line in saved.read_text().splitlines())


@pytest.mark.parametrize(
    ("sort", "column"),
    [
        (SessionSort.LAST_ACTIVE, "last_active"),
        (SessionSort.CREATED_AT, "created_at"),
        (SessionSort.DURATION, "perf_duration_ms"),
        (SessionSort.INPUT, "input_tokens"),
        (SessionSort.OUTPUT, "output_tokens"),
        (SessionSort.CACHE_READ, "cache_read"),
        (SessionSort.CACHE_WRITE, "cache_write"),
        (SessionSort.REASONING, "reasoning_tokens"),
        (SessionSort.TOTAL, "total_tokens"),
        (SessionSort.COST, "cost_usd"),
        (SessionSort.COST_PER_MILLION, "cost_per_million"),
        (SessionSort.PERFORMANCE, "ms_per_1k_tokens"),
    ],
)
def test_session_sort_orders_metric_before_limit(
    tmp_path: Path, sort: SessionSort, column: str
) -> None:
    """Order every supported metric without relying on optional visible columns."""
    _, backend = _configured_store(tmp_path)
    backend.connection.execute(
        "UPDATE daily_stats SET perf_duration_ms = CASE model "
        "WHEN 'gpt-test' THEN 11 WHEN 'gpt-mini' THEN 0 ELSE 12 END, "
        "perf_timed_tokens = CASE model WHEN 'gpt-mini' THEN 0 ELSE 10000 END"
    )
    filters = ReportFilters(source=LOCAL_SOURCE_ID)
    records = load_session_usage(backend, filters, by_model=False, sort=sort)
    limited = load_session_usage(backend, filters, by_model=False, sort=sort, limit=1)
    backend.close()

    def value_as_number(value: object) -> float | None:
        """Compare numeric and timestamp sort values without formatting."""
        return (
            value.timestamp() if isinstance(value, datetime) else numeric_value(value)
        )

    values = [value_as_number(record[column]) for record in records]
    assert all(value is not None for value in values)
    assert values == sorted(
        (value for value in values if value is not None), reverse=True
    )
    if sort == SessionSort.PERFORMANCE:
        # Both rates display as 1; ordering must retain their fractional values.
        assert values == [1.2, 1.1]
        assert records[0]["session_id"] == "ses_local_other"
    assert limited == records[:1]
    samples = sample_session_usage(
        ReportFilters(source=SAMPLE_LOCAL_SOURCE_ID), by_model=False, sort=sort
    )
    sample_values = [value_as_number(record[column]) for record in samples]
    present = [value for value in sample_values if value is not None]
    assert sample_values == sorted(present, reverse=True) + [None] * (
        len(sample_values) - len(present)
    )


@pytest.mark.parametrize("by_model", [False, True])
def test_session_performance_weights_only_paired_components(
    tmp_path: Path, by_model: bool
) -> None:
    """Keep execution duration separate from timing pairs used for performance."""
    config, backend = _configured_store(tmp_path)
    backend.connection.execute(
        "UPDATE daily_stats SET perf_duration_ms = 100, perf_timed_tokens = 100 "
        "WHERE source_id = ? AND model = 'gpt-test'",
        [LOCAL_SOURCE_ID],
    )
    backend.connection.execute(
        "UPDATE daily_stats SET perf_duration_ms = 90000, perf_timed_tokens = 0 "
        "WHERE source_id = ? AND model = 'gpt-mini'",
        [LOCAL_SOURCE_ID],
    )
    backend.connection.execute(
        "INSERT INTO daily_stats "
        "(event_id, source_id, day, client, session_id, model, total_tokens, "
        "perf_duration_ms, perf_timed_tokens, collected_at) "
        "VALUES (UUID(), ?, '2026-09-09', 'codex', "
        "'ses_local_demonstration_identifier', 'gpt-test', 1, 900, 9900, NOW())",
        [LOCAL_SOURCE_ID],
    )
    records = load_session_usage(
        backend,
        ReportFilters(source=LOCAL_SOURCE_ID, client="codex"),
        by_model=by_model,
        sort=SessionSort.PERFORMANCE,
    )
    backend.close()
    timed = records[0]
    assert timed["ms_per_1k_tokens"] == 100
    assert timed["perf_duration_ms"] == (1000 if by_model else 91000)
    if by_model:
        assert records[1]["ms_per_1k_tokens"] is None
    command = [
        "report",
        "sessions",
        "--config",
        str(config),
        "--source",
        "local",
        "--client",
        "codex",
        "--with-performance",
        "--with-duration",
        "--sort",
        "performance",
        "--width",
        "max",
    ]
    if by_model:
        command.append("--by-model")
    result = CliRunner().invoke(app, command)
    assert result.exit_code == 0
    text = plain_cli_output(result.output)
    assert "ms/1K" in text
    timed_line = next(
        line
        for line in text.splitlines()
        if "ses_local_demonstration_identifier" in line
        and (not by_model or "gpt-test" in line)
    )
    assert timed_line.split()[9] == "100"
    assert ("01s" if by_model else "01m31s") in text


def test_session_sort_and_performance_options() -> None:
    """Expose sort and performance options while rejecting invalid sort values."""
    runner = CliRunner()
    help_result = runner.invoke(app, ["report", "sessions", "--help"])
    help_text = plain_cli_output(help_result.output)
    assert "--sort" in help_text
    assert "--with-performance" in help_text
    assert (
        runner.invoke(
            app, ["report", "sessions", "--test", "--sort", "invalid"]
        ).exit_code
        == 2
    )


def test_session_identifiers_expand_client_before_session(tmp_path: Path) -> None:
    """Reveal full identifiers progressively while retaining both ends when cut."""
    config, backend = _configured_store(tmp_path)
    session = "session-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-end"
    client = "client-ABCDEFGHIJKLM-end"
    for table in ["sessions", "daily_stats"]:
        backend.connection.execute(
            f"UPDATE {table} SET session_id = ?, client = ? "
            "WHERE source_id = ? AND session_id = 'ses_local_demonstration_identifier'",
            [session, client, LOCAL_SOURCE_ID],
        )
    backend.close()
    command = [
        "report",
        "sessions",
        "--config",
        str(config),
        "--source",
        "local",
        "--model",
        "gpt-test",
        "--with-duration",
        "--with-performance",
    ]
    values: list[tuple[str, str]] = []
    widths = [120, 122, 130, 180]
    for width in widths:
        result = CliRunner().invoke(app, [*command, "--width", str(width)])
        assert result.exit_code == 0
        text = plain_cli_output(result.output)
        line = next(line for line in text.splitlines() if "$" in line)
        rendered_session, rendered_client = line.split()[:2]
        for full, rendered in [(session, rendered_session), (client, rendered_client)]:
            if "…" in rendered:
                prefix, suffix = rendered.split("…")
                assert prefix and suffix
                assert full.startswith(prefix) and full.endswith(suffix)
            else:
                assert rendered == full
        values.append((rendered_session, rendered_client))
        assert len(line) == width
    assert values[-1] == (session, client)
    for previous, current in pairwise(values):
        if current[1] != client:
            assert current[0] == previous[0]
        assert len(current[0]) >= len(previous[0])
        assert len(current[1]) >= len(previous[1])


def test_middle_truncation_counts_terminal_cells() -> None:
    """Keep Unicode identifiers within their allotted terminal cell width."""
    value = "開始ABCDEFGHIJK終了"
    rendered = truncate_middle(value, 100, 12)
    assert cell_len(rendered) <= 12
    prefix, suffix = rendered.split("…")
    assert prefix and suffix
    assert value.startswith(prefix) and value.endswith(suffix)


def test_session_empty_results_support_explicit_widths() -> None:
    """Render an empty bounded session report without losing its message."""
    result = CliRunner().invoke(
        app,
        [
            "report",
            "sessions",
            "--test",
            "--client",
            "missing-client",
            "--sanitize",
            "--with-duration",
        ],
    )
    assert result.exit_code == 0
    assert "No matching usage data." in plain_cli_output(result.output)
