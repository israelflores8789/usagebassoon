# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""activity.py — Daily activity calendar, metric normalization, and exports."""

from __future__ import annotations

import math
import os
import struct
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal, cast

import typer
from rich.console import Console
from rich.text import Text

from usagebassoon.cli._output import output_console
from usagebassoon.cli.reports._common import (
    SAMPLE_LATEST_DAY,
    SAMPLE_LOCAL_SOURCE_ID,
    ReportFilters,
    ReportRecord,
    integer_value,
    load_configured_daily_usage,
    numeric_value,
    parse_report_dates,
    parse_width,
    resolve_filters,
    sample_daily_usage,
    sanitize_records,
)
from usagebassoon.cli.reports._render import (
    DataFormat,
    output_format,
    render_records,
    write_output,
)

type Scale = Literal["linear", "log"]

_TOKEN_FIELDS = {
    "total": "total_tokens",
    "input": "input_tokens",
    "output": "raw_output_tokens",
    "reasoning": "reasoning_tokens",
    "cache-read": "cache_read",
    "cache-write": "cache_write",
    "billable-output": "output_tokens",
}
_OTHER_METRICS = {"cost", "cost-per-million", "session-time"}
_WEEKDAYS = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")


def _metrics(values: list[str] | None) -> tuple[str, ...]:
    """Validate repeated or comma-separated metrics without token overlap."""
    selected = tuple(
        part.strip() for value in values or ["total"] for part in value.split(",")
    )
    allowed = set(_TOKEN_FIELDS) | _OTHER_METRICS
    if any(value not in allowed for value in selected):
        raise typer.BadParameter(
            "Choose from " + ", ".join(sorted(allowed)), param_hint="--metric"
        )
    if len(set(selected)) != len(selected):
        raise typer.BadParameter(
            "Duplicate metrics are not allowed", param_hint="--metric"
        )
    if len(selected) > 1 and (set(selected) & ({"total"} | _OTHER_METRICS)):
        raise typer.BadParameter(
            "total, cost, cost-per-million, and session-time must be used alone",
            param_hint="--metric",
        )
    if "billable-output" in selected and set(selected) & {"output", "reasoning"}:
        raise typer.BadParameter(
            "billable-output overlaps output and reasoning", param_hint="--metric"
        )
    return selected


def _window(
    days: int | None, since: date | None, until: date | None, today: date
) -> tuple[date, date]:
    """Resolve inclusive UTC calendar bounds, defaulting to 120 days."""
    if days is not None and (since is not None or until is not None):
        raise typer.BadParameter("--days cannot be combined with --since or --until")
    end = until or today
    try:
        start = since or end - timedelta(days=(days or 120) - 1)
    except OverflowError as error:
        raise typer.BadParameter(
            "Date range exceeds supported calendar dates"
        ) from error
    if start > end:
        raise typer.BadParameter("--since must be on or before --until")
    return start, end


def _intensity(
    value: float | int | None, maximum: float, colors: int, scale: Scale
) -> int | None:
    """Map a known nonnegative value into zero plus positive intensity bands."""
    if value is None:
        return None
    if value <= 0 or maximum <= 0:
        return 0
    fraction = (
        math.log1p(value) / math.log1p(maximum) if scale == "log" else value / maximum
    )
    return min(colors - 1, max(1, math.ceil((colors - 1) * fraction)))


def _daily_records(
    records: list[ReportRecord],
    start: date,
    end: date,
    today: date,
    metrics: tuple[str, ...],
) -> list[ReportRecord]:
    """Fill calendar days and compute raw selected values and measurement status."""
    by_day = {str(record["day"]): record for record in records}
    result: list[ReportRecord] = []
    unit = (
        "ms"
        if metrics == ("session-time",)
        else "USD/1M tokens"
        if metrics == ("cost-per-million",)
        else "USD"
        if metrics == ("cost",)
        else "tokens"
    )
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        fact = by_day.get(day.isoformat())
        total = integer_value(fact.get("total_tokens")) if fact else 0
        cost = numeric_value(fact.get("cost_usd")) if fact else 0.0
        duration = (
            integer_value(fact["perf_duration_ms"])
            if fact and fact.get("perf_duration_ms") is not None
            else None
        )
        measured = integer_value(fact.get("measured_fact_count")) if fact else 0
        count = integer_value(fact.get("total_fact_count")) if fact else 0
        value: int | float | None
        status = "complete" if fact else "no-recorded-usage"
        if metrics == ("cost",):
            value = cost
        elif metrics == ("cost-per-million",):
            value = cost * 1_000_000 / total if cost is not None and total > 0 else None
        elif metrics == ("session-time",):
            value = duration if fact else 0
            if 0 < measured < count:
                status = "partial"
        else:
            value = (
                sum(
                    integer_value(fact.get(_TOKEN_FIELDS[metric])) for metric in metrics
                )
                if fact
                else 0
            )
        if value is None:
            status = "unknown"
        if day > today:
            value, status = None, "future"
        result.append(
            {
                "day": day.isoformat(),
                "metric": ",".join(metrics),
                "unit": unit,
                "value": value,
                "status": status,
                "total_tokens": total,
                "cost_usd": cost,
                "cost_basis": fact.get("cost_basis") if fact else None,
                "duration_ms": duration,
                "measured_fact_count": measured,
                "total_fact_count": count,
            }
        )
    return result


def _terminal_purple() -> tuple[int, int, int] | None:
    """Read Windows' configured magenta slot without changing terminal settings."""
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes

    try:
        loader = cast(type[ctypes.CDLL], vars(ctypes)["WinDLL"])
        kernel = loader("kernel32", use_last_error=True)
        kernel.GetStdHandle.argtypes = [wintypes.DWORD]
        kernel.GetStdHandle.restype = wintypes.HANDLE
        kernel.GetConsoleScreenBufferInfoEx.argtypes = [
            wintypes.HANDLE,
            ctypes.c_void_p,
        ]
        kernel.GetConsoleScreenBufferInfoEx.restype = wintypes.BOOL
        # CONSOLE_SCREEN_BUFFER_INFOEX: fixed 32-byte header and 16 COLORREFs.
        buffer = ctypes.create_string_buffer(96)
        struct.pack_into("<I", buffer, 0, 96)
        if not kernel.GetConsoleScreenBufferInfoEx(kernel.GetStdHandle(-11), buffer):
            return None
        purple = struct.unpack_from("<I", buffer, 32 + 5 * 4)[0]
        return purple & 255, (purple >> 8) & 255, (purple >> 16) & 255
    except (AttributeError, KeyError, OSError, ValueError):
        return None


def _palette(colors: int, purple: tuple[int, int, int] | None = None) -> list[str]:
    """Build a purple gradient; ANSI magenta is the portable themed fallback."""
    if purple is None:
        return ["magenta"] * colors
    light = tuple(round(channel + (255 - channel) * 0.75) for channel in purple)
    dark = tuple(round(channel * 0.45) for channel in purple)
    return [
        "#"
        + "".join(
            f"{round(a + (b - a) * level / (colors - 1)):02x}"
            for a, b in zip(light, dark, strict=True)
        )
        for level in range(colors)
    ]


def _cell(
    level: int | None, palette: list[str], color: bool, partial: bool = False
) -> Text:
    """Render one square or an unknown marker, retaining intensity without color."""
    if level is None:
        return Text("? ")
    if level == 0:
        return Text("  ")
    if color and palette[level] == "magenta":
        # Density preserves each band when the theme exposes one purple color.
        shades = ("░░", "░▒", "▒▒", "░█", "▒▓", "▓▓", "▒█", "▓█", "██")
        index = round((level - 1) * 8 / max(1, len(palette) - 2))
        return Text("· " if partial else shades[index], style="magenta")
    if color:
        foreground = "white" if level > len(palette) // 2 else "black"
        return Text(
            "· " if partial else "  ",
            style=f"{foreground} on {palette[level]}",
        )
    return Text(f"{level}{'*' if partial else ' '}")


def _render(
    records: list[ReportRecord],
    start: date,
    end: date,
    palette: list[str],
    scale: Scale,
    width: int | None,
    save: Path | None,
) -> None:
    """Render consecutive week panels and an intensity legend without numbers."""
    console = output_console(record=save is not None)
    available = (
        min(console.width, width)
        if width is not None
        else max(console.width, 4 + 3 * (((end - start).days + 13) // 7))
    )
    if available < 10:
        raise typer.BadParameter(
            "--width must be at least 10 for the activity grid", param_hint="--width"
        )
    console.width = available
    purple = _terminal_purple() if console.color_system == "truecolor" else None
    palette = _palette(len(palette), purple)
    _calendar(
        console,
        records,
        start,
        end,
        palette,
        scale,
        plain=save is not None or console.color_system is None or console.no_color,
    )
    if save is not None:
        write_output(console.export_text(), save=save)


def _calendar(
    console: Console,
    records: list[ReportRecord],
    start: date,
    end: date,
    palette: list[str],
    scale: Scale,
    *,
    plain: bool,
) -> None:
    """Lay out Sunday-first calendar rows without splitting weeks between panels."""
    first = start.toordinal() - (start.weekday() + 1) % 7
    weeks = (end.toordinal() - first) // 7 + 1
    capacity = max(1, (console.width - 4) // 3)
    chart_width = 4 + 3 * min(weeks, capacity)
    title = Text("Daily Activity", style="bold")
    title.pad_left(max(0, (chart_width - len(title)) // 2))
    console.print(title)
    console.print()
    subtitle = Text(
        f"{records[0]['metric']} ({records[0]['unit']}) | {start} to {end} | {scale}"
    )
    subtitle.pad_left(max(0, (chart_width - len(subtitle)) // 2))
    console.print(subtitle)
    by_day = {str(record["day"]): record for record in records}
    color = not plain
    has_january = any(
        date.fromisoformat(str(record["day"])).month == 1 for record in records
    )
    first_label = True
    for panel in range(0, weeks, capacity):
        columns = min(capacity, weeks - panel)
        console.print()
        # Repeat month labels at each panel's first week; suppress overlapping labels.
        labels = [" "] * max(columns * 3, min(8, console.width - 4))
        previous: tuple[int, int] | None = None
        occupied = 0
        for column in range(columns):
            ordinal = max(start.toordinal(), first + 7 * (panel + column))
            day = date.fromordinal(ordinal)
            week_end = date.fromordinal(
                min(end.toordinal(), first + 7 * (panel + column) + 6)
            )
            if week_end.month == 1 and day.month != 1:
                day = week_end
            month = (day.year, day.month)
            if month != previous:
                show_year = day.month == 1 if has_january else first_label
                label = day.strftime("%b %Y" if show_year else "%b")
                position = column * 3
                if show_year and len(label) <= len(labels):
                    position = min(position, len(labels) - len(label))
                    # The year takes precedence over an overlapping month label.
                    if position < occupied:
                        labels[position:] = [" "] * (len(labels) - position)
                        occupied = position
                if position >= occupied and position + len(label) <= len(labels):
                    labels[position : position + len(label)] = label
                    occupied = position + len(label) + 1
                    first_label = False
            previous = month
        console.print(Text("    " + "".join(labels)))
        for weekday, name in enumerate(_WEEKDAYS):
            row = Text(name + " ")
            for column in range(columns):
                ordinal = first + 7 * (panel + column) + weekday
                record = (
                    by_day.get(date.fromordinal(ordinal).isoformat())
                    if start.toordinal() <= ordinal <= end.toordinal()
                    else None
                )
                if record is None:
                    row.append("  ")
                else:
                    level = record["intensity"]
                    row.append_text(
                        _cell(
                            level if isinstance(level, int) else None,
                            palette,
                            color,
                            record["status"] == "partial",
                        )
                    )
                row.append(" ")
            console.print(row)
    legend = Text("Less ")
    for level in range(len(palette)):
        if color:
            legend.append_text(_cell(level, palette, True))
        else:
            shades = "░▒▓█"
            legend.append(
                "□ "
                if level == 0
                else shades[round(level * 3 / (len(palette) - 1))] * 2
            )
        legend.append(" ")
    legend.append("More")
    console.print()
    console.print(legend)
    console.print(
        "Intensity is relative to this selection. "
        "Unfilled squares show zero recorded usage."
    )
    if any(record["status"] in {"partial", "unknown", "future"} for record in records):
        console.print(
            "? unavailable; · (color) or * (plain text) marks partial session time."
        )


def _export(
    records: list[ReportRecord],
    start: date,
    end: date,
    scale: Scale,
    colors: int,
    maximum: float,
    format: DataFormat,
    save: Path | None,
) -> None:
    """Serialize identical calendar values to JSON or CSV without terminal styling."""
    metadata: ReportRecord = {
        "title": "Daily Activity",
        "since": start.isoformat(),
        "until": end.isoformat(),
        "scale": scale,
        "colors": colors,
        "maximum": maximum,
        "metric": records[0]["metric"],
        "unit": records[0]["unit"],
    }
    rows = [
        {**record, "scale": scale, "colors": colors, "maximum": maximum}
        for record in records
    ]
    render_records(
        rows,
        columns=list(rows[0]),
        format=format,
        save=save,
        json_payload={**metadata, "days": records},
    )


def activity(
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    days: Annotated[
        int | None,
        typer.Option("--days", min=1, help="Trailing calendar days; default 120."),
    ] = None,
    since: Annotated[
        str | None, typer.Option("--since", help="Inclusive YYYY-MM-DD start.")
    ] = None,
    until: Annotated[
        str | None, typer.Option("--until", help="Inclusive YYYY-MM-DD end.")
    ] = None,
    metric: Annotated[
        list[str] | None,
        typer.Option(
            "--metric",
            help=(
                "Default total; input, output, reasoning, cache-read, cache-write, "
                "billable-output (repeat or comma-separate); cost, "
                "cost-per-million, session-time (alone)."
            ),
        ),
    ] = None,
    scale: Annotated[
        Literal["linear", "log"],
        typer.Option("--scale", help="Intensity normalization: linear or log."),
    ] = "linear",
    colors: Annotated[
        int,
        typer.Option(
            "--colors",
            min=3,
            max=10,
            help="Number of intensity colors, including zero.",
        ),
    ] = 7,
    client: Annotated[
        str | None, typer.Option("--client", help="Filter by exact client.")
    ] = None,
    model: Annotated[
        str | None, typer.Option("--model", help="Filter by exact model.")
    ] = None,
    workspace: Annotated[
        str | None, typer.Option("--workspace", help="Filter by exact workspace.")
    ] = None,
    tag: Annotated[
        str | None, typer.Option("--tag", help="Filter by an effective curation tag.")
    ] = None,
    source: Annotated[
        str | None,
        typer.Option("--source", help="Source UUID or 'local'; default all sources."),
    ] = None,
    width: Annotated[
        str, typer.Option("--width", help="Maximum terminal width, or 'max'.")
    ] = "100",
    test: Annotated[
        bool,
        typer.Option(
            "--test", help="Render deterministic sample data without a backend."
        ),
    ] = False,
    sanitize: Annotated[
        bool,
        typer.Option(
            "--sanitize", "--obfuscate", help="Obfuscate identifiers for sharing."
        ),
    ] = False,
    save: Annotated[
        Path | None,
        typer.Option("--save", help="Save the selected text, JSON, or CSV format."),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Export daily values and intensity as JSON.")
    ] = False,
    csv_output: Annotated[
        bool, typer.Option("--csv", help="Export daily values and intensity as CSV.")
    ] = False,
) -> None:
    """Render Daily Activity across all sources, using total tokens by default."""
    format = output_format(json_output, csv_output)
    selected = _metrics(metric)
    today = SAMPLE_LATEST_DAY if test else datetime.now(UTC).date()
    start, end = _window(days, *parse_report_dates(since, until), today)
    parsed_width = parse_width(width)
    filters = ReportFilters(
        source=source, client=client, model=model, workspace=workspace, tag=tag
    )
    records = (
        sample_daily_usage(
            resolve_filters(filters, SAMPLE_LOCAL_SOURCE_ID), since=start, until=end
        )
        if test
        else load_configured_daily_usage(config, filters, since=start, until=end)
    )
    daily = _daily_records(records, start, end, today, selected)
    maximum = max(
        (numeric_value(record["value"]) or 0.0 for record in daily), default=0.0
    )
    for record in daily:
        record["intensity"] = _intensity(
            numeric_value(record["value"]), maximum, colors, scale
        )
    # Aggregation removes all identifiers and free text, so sanitization is inherent.
    daily = sanitize_records(daily, sanitize)
    if format != "text":
        _export(daily, start, end, scale, colors, maximum, format, save)
    else:
        _render(daily, start, end, _palette(colors), scale, parsed_width, save)
