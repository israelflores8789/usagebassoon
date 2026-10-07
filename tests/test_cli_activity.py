# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_activity.py — Calendar semantics, exports, filters, and rendering."""

from __future__ import annotations

import csv
import io
import json
from datetime import date
from pathlib import Path

import pytest
import typer
from rich.console import Console
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from tests.test_cli_report import LOCAL_SOURCE_ID, REMOTE_SOURCE_ID, _configured_store
from usagebassoon.cli._colorterm import blend_palette
from usagebassoon.cli.app import app
from usagebassoon.cli.reports._common import (
    ReportFilters,
    ReportRecord,
    load_daily_usage,
)
from usagebassoon.cli.reports.activity import (
    _calendar,
    _daily_records,
    _intensity,
    _metrics,
    _window,
)


def test_defaults_and_export_equivalence(tmp_path: Path) -> None:
    """Keep CSV and JSON values identical over the default 120-day calendar."""
    runner = CliRunner()
    result = runner.invoke(app, ["report", "activity", "--test", "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["metric"] == "total"
    assert payload["bins"] == 7
    assert payload["scale"] == "linear"
    assert len(payload["days"]) == 120
    assert payload["days"][-1]["day"] == "2026-09-10"
    assert any(row["value"] > 0 for row in payload["days"])
    assert payload["days"][0]["intensity"] == 0
    destination = tmp_path / "activity.csv"
    result = runner.invoke(
        app, ["report", "activity", "--test", "--csv", "--save", str(destination)]
    )
    assert result.exit_code == 0, result.output
    assert plain_cli_output(result.stdout) == ""
    exported = list(csv.DictReader(io.StringIO(destination.read_text())))
    for expected, actual in zip(payload["days"], exported, strict=True):
        assert actual["day"] == expected["day"]
        assert int(actual["value"]) == expected["value"]
        assert int(actual["intensity"]) == expected["intensity"]
    result = runner.invoke(
        app, ["report", "activity", "--test", "--json", "--sanitize"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == payload
    json_destination = tmp_path / "activity.json"
    result = runner.invoke(
        app, ["report", "activity", "--test", "--json", "--save", str(json_destination)]
    )
    assert result.exit_code == 0, result.output
    assert plain_cli_output(result.stdout) == ""
    assert json.loads(json_destination.read_text()) == payload


@pytest.mark.parametrize("bins", [3, 7, 10])
def test_intensity_bands(bins: int) -> None:
    """Reach every band and preserve missing, zero, and logarithmic values."""
    assert [
        _intensity(level, bins - 1, bins, "linear") for level in range(bins)
    ] == list(range(bins))
    assert _intensity(None, 0, bins, "log") is None
    assert _intensity(0, 0, bins, "linear") == 0
    assert _intensity(5, 5, bins, "log") == bins - 1
    logarithmic = _intensity(1, 1000, bins, "log")
    linear = _intensity(1, 1000, bins, "linear")
    assert logarithmic is not None and linear is not None
    assert logarithmic >= linear


@pytest.mark.parametrize(
    "selection",
    [
        ["total,input"],
        ["input,input"],
        ["billable-output,reasoning"],
        ["unknown"],
    ],
)
def test_invalid_metric_combinations(selection: list[str]) -> None:
    """Reject unknown, repeated, overlapping, and dimensionally mixed metrics."""
    with pytest.raises(typer.BadParameter):
        _metrics(selection)


def test_calendar_leap_day_unknown_and_partial_time() -> None:
    """Distinguish missing measurements, no records, future days, and partial time."""
    records: list[ReportRecord] = [
        {
            "day": date(2024, 2, 29),
            "total_tokens": 100,
            "cost_usd": 2,
            "perf_duration_ms": 3000,
            "measured_fact_count": 1,
            "total_fact_count": 2,
        }
    ]
    start, end, today = date(2024, 2, 28), date(2024, 3, 1), date(2024, 2, 29)
    daily = _daily_records(records, start, end, today, ("session-time",))
    assert [row["value"] for row in daily] == [0, 3000, None]
    assert [row["status"] for row in daily] == [
        "no-recorded-usage",
        "partial",
        "future",
    ]
    ratio = _daily_records(records, start, end, today, ("cost-per-million",))
    assert ratio[0]["value"] is None
    assert ratio[1]["value"] == 20_000
    unknown = _daily_records(
        [{"day": start, "total_fact_count": 1, "measured_fact_count": 0}],
        start,
        start,
        today,
        ("session-time",),
    )[0]
    assert unknown["value"] is None
    assert unknown["status"] == "unknown"
    assert _window(None, None, None, today) == (date(2023, 11, 2), today)
    with pytest.raises(typer.BadParameter):
        _window(10, start, None, today)


@pytest.mark.parametrize(
    "options",
    [
        ["--json", "--csv"],
        ["--days", "2", "--since", "2026-09-01"],
        ["--since", "2026-09-11"],
        ["--source", "invalid"],
        ["--color", "purple"],
        ["--color", "#ff00ff"],
        ["--color", "default"],
    ],
)
def test_cli_validation(options: list[str]) -> None:
    """Reject invalid options before producing a report."""
    result = CliRunner().invoke(app, ["report", "activity", "--test", *options])
    assert result.exit_code != 0


def test_rich_panels_and_unnumbered_legend() -> None:
    """Retain all weekday rows and colors when wrapping at week boundaries."""
    start, end = date(2024, 2, 28), date(2024, 3, 20)
    records = _daily_records([], start, end, end, ("total",))
    for index, record in enumerate(records):
        record["intensity"] = index % 7
    stream = io.StringIO()
    console = Console(
        file=stream,
        width=25,
        force_terminal=True,
        color_system="truecolor",
        no_color=False,
        record=True,
    )
    _calendar(
        console,
        records,
        start,
        end,
        blend_palette(7, (160, 110, 210)),
        "linear",
        plain=False,
    )
    colored = stream.getvalue()
    assert "48;2;" in colored
    text = plain_cli_output(console.export_text())
    assert text.count("Sun ") == 1
    assert text.count("Sat ") == 1
    legend = text.partition("Less ")[2].partition("Intensity")[0]
    assert not any(character.isdigit() for character in legend)
    assert "More" in legend
    stream = io.StringIO()
    console = Console(file=stream, width=13)
    _calendar(console, records, start, end, blend_palette(7), "linear", plain=True)
    text = stream.getvalue()
    assert text.count("Sun ") == 2
    assert text.count("Sat ") == 2
    assert "░░" in text


def test_filtered_backend_duration_and_cost(tmp_path: Path) -> None:
    """Apply all dimensions before totals, preserve time, and avoid tag fanout."""
    config, backend = _configured_store(tmp_path)
    for table in ("sessions", "daily_stats"):
        backend.connection.execute(
            f"UPDATE {table} SET session_id = 'ses_local_demonstration_identifier' "
            "WHERE source_id = ?",
            [REMOTE_SOURCE_ID],
        )
    backend.connection.execute(
        "UPDATE daily_stats SET perf_duration_ms = 3000 WHERE source_id = ?",
        [LOCAL_SOURCE_ID],
    )
    backend.connection.execute(
        "INSERT INTO tags (event_id, scope, source_id, client, workspace, "
        "session_id, tag, created_at, updated_at, collected_at) "
        "VALUES (UUID(), 'client', ?, 'codex', '', '', 'focused', "
        "NOW(), NOW(), NOW())",
        [LOCAL_SOURCE_ID],
    )
    # The same effective tag at two scopes must not multiply the local fact.
    result = load_daily_usage(
        backend,
        ReportFilters(
            source=LOCAL_SOURCE_ID,
            client="codex",
            model="gpt-test",
            workspace="/work/atlas",
            tag="focused",
        ),
        since=date(2026, 9, 10),
        until=date(2026, 9, 12),
    )
    assert len(result) == 1
    assert result[0]["total_tokens"] == 185
    assert result[0]["perf_duration_ms"] == 3000
    assert result[0]["total_fact_count"] == 1
    assert result[0]["cost_basis"] == "observed"
    backend.close()
    runner = CliRunner()
    base = [
        "report",
        "activity",
        "--config",
        str(config),
        "--since",
        "2026-09-09",
        "--until",
        "2026-09-12",
        "--json",
    ]
    all_sources = runner.invoke(app, base)
    assert all_sources.exit_code == 0, all_sources.output
    assert sum(row["value"] for row in json.loads(all_sources.stdout)["days"]) == 2083
    local = runner.invoke(app, [*base, "--source", "local", "--metric", "session-time"])
    assert local.exit_code == 0, local.output
    assert sum(row["value"] for row in json.loads(local.stdout)["days"]) == 9000
    remote = runner.invoke(
        app,
        [
            *base,
            "--source",
            REMOTE_SOURCE_ID,
            "--metric",
            "output",
            "--metric",
            "reasoning",
        ],
    )
    assert remote.exit_code == 0, remote.output
    assert sum(row["value"] for row in json.loads(remote.stdout)["days"]) == 109


@pytest.mark.sql_parity
def test_daily_activity_query_matches_bigquery_replays() -> None:
    """Aggregate retained observations identically despite repeated raw appends."""
    from tests._bigquery_replay import BigQueryReplayBackend
    from tests._sql_parity import seed_synthetic_data
    from usagebassoon.backends.duckdb_local import DuckDBBackend

    local = DuckDBBackend(":memory:")
    remote = BigQueryReplayBackend()
    try:
        local.apply_ddl()
        seed_synthetic_data(local)
        seed_synthetic_data(remote)
        seed_synthetic_data(remote)
        filters = ReportFilters(client="codex")
        left = load_daily_usage(local, filters)
        right = load_daily_usage(remote, filters)
        assert left == right
        assert left[0]["perf_duration_ms"] == 144
        assert left[0]["total_tokens"] == 25
        assert left[0]["total_fact_count"] == 3
        assert left[0]["cost_basis"] == "mixed"
    finally:
        local.close()
        remote.close()


@pytest.mark.parametrize(
    ("metrics", "expected"),
    [
        (("total",), 185),
        (("input", "output", "reasoning"), 125),
        (("cache-read", "cache-write"), 60),
        (("billable-output",), 25),
        (("cost",), 2.0),
        (("cost-per-million",), 2.0 * 1_000_000 / 185),
    ],
)
def test_metric_component_calculations(
    metrics: tuple[str, ...], expected: int | float
) -> None:
    """Calculate selected token components, cost, and cost per million."""
    day = date(2026, 9, 10)
    facts: list[ReportRecord] = [
        {
            "day": day,
            "total_tokens": 185,
            "input_tokens": 100,
            "raw_output_tokens": 20,
            "reasoning_tokens": 5,
            "output_tokens": 25,
            "cache_read": 50,
            "cache_write": 10,
            "cost_usd": 2.0,
            "total_fact_count": 1,
        }
    ]
    result = _daily_records(facts, day, day, day, metrics)[0]
    assert result["value"] == expected
    assert result["status"] == "complete"


def test_inactive_and_shaded_cells() -> None:
    """Leave inactive squares unfilled and distinguish every active shade."""
    from usagebassoon.cli.reports.activity import _cell

    palette = blend_palette(10)
    empty = _cell(0, palette, True)
    assert empty.plain == "  "
    assert not empty.style
    cells = [_cell(level, palette, False) for level in range(1, 10)]
    assert len({cell.plain for cell in cells}) == 9
    assert all(not cell.style for cell in cells)
    solid = _cell(9, palette, True)
    assert solid.plain == "  "
    assert f"on {palette[-1]}" in str(solid.style)


@pytest.mark.parametrize(
    ("start", "end", "year_month"),
    [
        (date(2025, 10, 1), date(2026, 2, 28), "Jan 2026"),
        (date(2026, 5, 1), date(2026, 8, 31), "May 2026"),
        (date(2025, 12, 31), date(2026, 1, 1), "Jan 2026"),
    ],
)
def test_calendar_year_precedence_and_title_spacing(
    start: date, end: date, year_month: str
) -> None:
    """Show the year once at January or the first month and center the title."""
    records = _daily_records([], start, end, end, ("total",))
    for record in records:
        record["intensity"] = 0
    stream = io.StringIO()
    console = Console(file=stream, width=100)
    _calendar(console, records, start, end, blend_palette(7), "linear", plain=True)
    lines = stream.getvalue().splitlines()
    assert lines[0].strip() == "Daily Activity"
    chart_row = next(line for line in lines if line.startswith("Sun "))
    assert len(lines[0]) - len(lines[0].lstrip()) == max(
        0, (len(chart_row) - len("Daily Activity")) // 2
    )
    assert lines[1] == ""
    month_line = next(line for line in lines if year_month in line)
    assert month_line.count("2026") == 1
    assert "Oct 2025" not in month_line
    assert "Feb 2026" not in month_line
    assert "Jun 2026" not in month_line


def test_ascii_option_and_text_save_use_shading(tmp_path: Path) -> None:
    """Force the fallback explicitly and preserve shaded activity in text files."""
    runner = CliRunner()
    result = runner.invoke(app, ["report", "activity", "--test", "--use-ascii"])
    assert result.exit_code == 0, result.output
    output = plain_cli_output(result.stdout)
    assert any(character in output for character in "░▒▓█")
    saved = tmp_path / "activity.txt"
    result = runner.invoke(app, ["report", "activity", "--test", "--save", str(saved)])
    assert result.exit_code == 0, result.output
    assert saved.read_text() == output
    invalid = runner.invoke(app, ["report", "activity", "--test", "--use-asci"])
    assert invalid.exit_code != 0


@pytest.mark.parametrize("use_256", [False, True])
def test_solid_and_ascii_mode_selection(
    monkeypatch: pytest.MonkeyPatch, use_256: bool
) -> None:
    """Use solid truecolor or 256-color cells and skip discovery for forced ASCII."""
    import importlib

    from usagebassoon.cli.reports.activity import _render

    module = importlib.import_module("usagebassoon.cli.reports.activity")
    monkeypatch.delenv("WT_SESSION", raising=False)
    monkeypatch.delenv("COLORTERM", raising=False)
    stream = io.StringIO()
    console = Console(
        file=stream,
        width=80,
        force_terminal=True,
        color_system="256" if use_256 else "truecolor",
        no_color=False,
    )

    def console_factory(*, record: bool) -> Console:
        """Return a terminal console for the renderer's unsaved output."""
        assert not record
        return console

    monkeypatch.setattr(module, "themed_console", console_factory)
    calls: list[bool] = []

    def colors(
        output: Console, color: str = "magenta"
    ) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
        """Return a sample theme and record when the renderer requests discovery."""
        assert output is console
        assert color == "magenta"
        calls.append(True)
        return (128, 64, 192), (24, 24, 24)

    monkeypatch.setattr(module, "resolve_theme_colors", colors)
    day = date(2026, 9, 10)
    records = _daily_records([], day, day, day, ("total",))
    records[0]["intensity"] = 6
    _render(records, day, day, blend_palette(7), "linear", 80, None)
    assert calls == [True]
    assert ("48;5;" if use_256 else "48;2;128;64;192") in stream.getvalue()
    assert not any(character in stream.getvalue() for character in "░▒▓█")
    stream.seek(0)
    stream.truncate()
    _render(records, day, day, blend_palette(7), "linear", 80, None, True)
    assert calls == [True]
    assert "48;2;" not in stream.getvalue()
    assert "48;5;" not in stream.getvalue()
    assert "██" in stream.getvalue()


@pytest.mark.parametrize("color", ["blue", "bright_cyan", "bright-red"])
def test_bins_and_color_exports(color: str) -> None:
    """Export the selected ANSI name and bin count in JSON and CSV."""
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["report", "activity", "--test", "--bins", "10", "--color", color, "--json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["bins"] == 10
    assert payload["color"] == color.replace("-", "_")
    assert all(
        row["intensity"] is None or 0 <= row["intensity"] < 10
        for row in payload["days"]
    )
    result = runner.invoke(
        app, ["report", "activity", "--test", "--bins", "3", "--color", color, "--csv"]
    )
    assert result.exit_code == 0, result.output
    rows = list(csv.DictReader(io.StringIO(result.stdout)))
    assert all(
        row["bins"] == "3" and row["color"] == color.replace("-", "_") for row in rows
    )


@pytest.mark.parametrize("name,index", [("red", 1), ("blue", 4), ("bright_cyan", 14)])
def test_selected_ansi_query_and_character_color(name: str, index: int) -> None:
    """Query the selected theme slot, ignore other slots, and color the fallback."""
    from usagebassoon.cli._colorterm import _decode_colors, _probe_colors
    from usagebassoon.cli.reports.activity import (
        _ansi_color,
        _cell,
    )

    assert _ansi_color(name) == name
    sent: list[str] = []
    reply = f"\x1b]4;{index};rgb:10/20/30\x07\x1b]11;rgb:18/18/18\x07"
    replies = iter([reply])
    assert _probe_colors(
        sent.append, lambda _timeout: next(replies, ""), ansi_index=index
    ) == ((16, 32, 48), (24, 24, 24))
    assert sent == [f"\x1b]4;{index};?\x1b\\\x1b]11;?\x1b\\"]
    assert _decode_colors(reply, (index + 1) % 16) is None
    cell = _cell(6, blend_palette(7), False, shade_color=True, ansi_color=name)
    assert cell.style == name
    assert cell.plain == "██"
