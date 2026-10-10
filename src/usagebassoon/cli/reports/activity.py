# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""activity.py — Daily activity calendar, metric normalization, and exports."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal

import typer
from rich.console import Console
from rich.text import Text

from usagebassoon.cli._colorterm import (
    adapt_palette,
    blend_palette,
    color_enabled,
    contrasting_foreground,
    fallback_accent,
    normalize_ansi_color,
    resolve_theme_colors,
    supports_gradients,
    themed_console,
)
from usagebassoon.cli._output import render_records
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
    cost_estimate_note,
    output_format,
    write_output,
)

type Scale = Literal["linear", "log"]
_SHADES = ("░░", "░▒", "▒▒", "░█", "▒▓", "▓▓", "▒█", "▓█", "██")


def _ansi_color(value: str) -> str:
    """Validate the activity color option through the shared terminal helpers."""
    try:
        return normalize_ansi_color(value)
    except ValueError as error:
        raise typer.BadParameter(str(error), param_hint="--color") from error


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
    value: float | int | None, maximum: float, bins: int, scale: Scale
) -> int | None:
    """Map a known nonnegative value into zero plus positive intensity bands."""
    if value is None:
        return None
    if value <= 0 or maximum <= 0:
        return 0
    fraction = (
        math.log1p(value) / math.log1p(maximum) if scale == "log" else value / maximum
    )
    return min(bins - 1, max(1, math.ceil((bins - 1) * fraction)))


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


def _cell(
    level: int | None,
    palette: list[str],
    color: bool,
    partial: bool = False,
    *,
    shade_color: bool = False,
    ansi_color: str = "magenta",
) -> Text:
    """Render one square or an unknown marker, retaining intensity without color."""
    if level is None:
        return Text("? ")
    if level == 0:
        return Text("  ")
    if color:
        foreground = contrasting_foreground(palette[level])
        return Text(
            "· " if partial else "  ",
            style=f"{foreground} on {palette[level]}",
        )
    index = round((level - 1) * 8 / max(1, len(palette) - 2))
    return Text(
        "* " if partial else _SHADES[index],
        style=ansi_color if shade_color else "",
    )


def _render(
    records: list[ReportRecord],
    start: date,
    end: date,
    palette: list[str],
    scale: Scale,
    width: int | None,
    save: Path | None,
    use_ascii: bool = False,
    ansi_color: str = "magenta",
) -> None:
    """Render consecutive week panels and an intensity legend without numbers."""
    console = themed_console(record=save is not None)
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
    plain = use_ascii or save is not None or not supports_gradients(console)
    if not plain:
        accent, background = resolve_theme_colors(console, ansi_color)
        palette = adapt_palette(
            console, blend_palette(len(palette), accent, background)
        )
    _calendar(
        console,
        records,
        start,
        end,
        palette,
        scale,
        plain=plain,
        ansi_color=ansi_color,
    )
    if any(record.get("metric") in {"cost", "cost-per-million"} for record in records):
        console.print(cost_estimate_note())
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
    ansi_color: str = "magenta",
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
    shade_color = plain and color_enabled(console)
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
                            shade_color=shade_color,
                            ansi_color=ansi_color,
                        )
                    )
                row.append(" ")
            console.print(row)
    legend = Text("Less ")
    for level in range(len(palette)):
        legend.append_text(
            _cell(level, palette, color, shade_color=shade_color, ansi_color=ansi_color)
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
    bins: int,
    maximum: float,
    format: DataFormat,
    save: Path | None,
    ansi_color: str = "magenta",
) -> None:
    """Serialize identical calendar values to JSON or CSV without terminal styling."""
    metadata: ReportRecord = {
        "title": "Daily Activity",
        "since": start.isoformat(),
        "until": end.isoformat(),
        "scale": scale,
        "bins": bins,
        "color": ansi_color,
        "maximum": maximum,
        "metric": records[0]["metric"],
        "unit": records[0]["unit"],
    }
    rows = [
        {
            **record,
            "scale": scale,
            "bins": bins,
            "color": ansi_color,
            "maximum": maximum,
        }
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
    bins: Annotated[
        int,
        typer.Option(
            "--bins",
            min=3,
            max=10,
            help="Number of intensity bins, including zero.",
        ),
    ] = 7,
    color: Annotated[
        str,
        typer.Option(
            "--color",
            help=(
                "ANSI theme color: black, red, green, yellow, blue, magenta, cyan, "
                "white, or bright_ variants."
            ),
        ),
    ] = "magenta",
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
    use_ascii: Annotated[
        bool,
        typer.Option(
            "--use-ascii",
            help="Force character shading instead of solid terminal colors.",
        ),
    ] = False,
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
    ansi_color = _ansi_color(color)
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
            numeric_value(record["value"]), maximum, bins, scale
        )
    # Aggregation removes all identifiers and free text, so sanitization is inherent.
    daily = sanitize_records(daily, sanitize)
    if format != "text":
        _export(daily, start, end, scale, bins, maximum, format, save, ansi_color)
    else:
        _render(
            daily,
            start,
            end,
            blend_palette(bins, fallback_accent(ansi_color)),
            scale,
            parsed_width,
            save,
            use_ascii,
            ansi_color,
        )
