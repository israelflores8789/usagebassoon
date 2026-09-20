# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""graph.py — Daily token-usage terminal bar graph command."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated

import plotext
import typer

from usagebassoon.cli.reports._common import (
    SAMPLE_LATEST_DAY,
    SAMPLE_LOCAL_SOURCE_ID,
    ReportFilters,
    format_cost,
    format_tokens,
    load_configured_daily_usage,
    numeric_value,
    parse_width,
    render_graph,
    resolve_filters,
    sample_daily_usage,
)

_METRICS: dict[str, tuple[str, str]] = {
    "cost": ("cost_usd", "Cost (USD)"),
    "total": ("total_tokens", "Total Tokens"),
    "input": ("input_tokens", "Input Tokens"),
    "output": ("raw_output_tokens", "Output Tokens"),
    "reasoning": ("reasoning_tokens", "Reasoning Tokens"),
    "cache-read": ("cache_read", "Cache Read Tokens"),
    "cache-write": ("cache_write", "Cache Write Tokens"),
    "billable-output": ("output_tokens", "Billable Output Tokens"),
}


def _window(
    days: int | None,
    since: date | None,
    until: date | None,
    *,
    reference_day: date | None = None,
) -> tuple[date, date]:
    """Resolve one inclusive graph day window with a 31-day maximum."""
    today = reference_day or datetime.now(UTC).date()
    if days is not None and (since is not None or until is not None):
        raise typer.BadParameter(
            "--days cannot be combined with --since or --until", param_hint="--days"
        )
    if since is None and until is None:
        count = days or 10
        return today - timedelta(days=count - 1), today
    end = until or today
    start = since or end - timedelta(days=9)
    if start > end:
        raise typer.BadParameter("--since must be on or before --until")
    if (end - start).days + 1 > 31:
        raise typer.BadParameter("graph ranges may not exceed 31 days")
    return start, end


def _parse_day(value: str | None, option: str) -> date | None:
    """Parse one optional strict ISO-8601 day option.

    Args:
        value: Raw option value, when supplied.
        option: CLI option name for an actionable validation error.

    Returns:
        Parsed calendar date, or ``None`` when omitted.
    """
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise typer.BadParameter(
            f"{option} must use YYYY-MM-DD", param_hint=option
        ) from error


def _metric(value: str) -> tuple[str, str]:
    """Return one graph metric field and professional y-axis label."""
    try:
        return _METRICS[value]
    except KeyError as error:
        raise typer.BadParameter(
            "--metric must be one of " + ", ".join(_METRICS),
            param_hint="--metric",
        ) from error


def _graph_text(
    labels: list[str],
    values: list[float],
    title: str,
    width: int | None,
    value_label: Callable[[object], str],
) -> str:
    """Build an uncolored Plotext bar graph suitable for terminal text output."""
    figure = plotext.figure
    figure.clear()
    try:
        plot_width = width if width is not None else 160
        figure.plot_size(plot_width, 18)
        positions = list(range(len(values)))
        tick_positions = _tick_positions(len(labels), plot_width)
        figure.draw(figure.bar(positions, values, marker="full"))
        figure.ruler(0).lim(-0.5, len(values) - 0.5).ticks(
            tick_positions, [labels[position] for position in tick_positions]
        )
        y_ticks = _y_tick_positions(values)
        figure.ruler(1).ticks(y_ticks, [value_label(value) for value in y_ticks])
        figure.title(title)
        rendered = plotext.uncolorize(str(figure.build()))
        return "" if rendered is None else str(rendered)
    finally:
        figure.clear()


def _tick_positions(days: int, width: int) -> list[int]:
    """Choose uniformly spaced date ticks that fit the rendered graph width."""
    maximum_ticks = max(2, (width - 14) // 7)
    stride = max(1, (days + maximum_ticks - 1) // maximum_ticks)
    return list(range(0, days, stride))


def _y_tick_positions(values: list[float]) -> list[float]:
    """Return five evenly distributed y-axis positions, including zero and max."""
    maximum = max(values, default=0.0)
    if maximum <= 0:
        return [0.0]
    return [maximum * step / 4 for step in range(5)]


def graph(
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    days: Annotated[
        int | None,
        typer.Option("--days", min=1, max=31, help="Trailing calendar days to show."),
    ] = None,
    since: Annotated[
        str | None, typer.Option("--since", help="Inclusive YYYY-MM-DD start.")
    ] = None,
    until: Annotated[
        str | None, typer.Option("--until", help="Inclusive YYYY-MM-DD end.")
    ] = None,
    metric: Annotated[
        str,
        typer.Option(
            "--metric",
            help=(
                "Y-axis: cost, total, input, output, reasoning, cache-read, "
                "cache-write, or billable-output."
            ),
        ),
    ] = "cost",
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
        typer.Option("--source", help="Filter by source UUID, or use 'local'."),
    ] = None,
    width: Annotated[
        str,
        typer.Option("--width", help="Maximum terminal width, or 'max'."),
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
        Path | None, typer.Option("--save", help="Write the rendered graph as text.")
    ] = None,
) -> None:
    """Render daily usage as a bar graph for one selected metric."""
    start, end = _window(
        days,
        _parse_day(since, "--since"),
        _parse_day(until, "--until"),
        reference_day=SAMPLE_LATEST_DAY if test else None,
    )
    field, label = _metric(metric)
    filters = ReportFilters(
        source=source,
        client=client,
        model=model,
        workspace=workspace,
        tag=tag,
    )
    records = (
        sample_daily_usage(
            resolve_filters(filters, SAMPLE_LOCAL_SOURCE_ID),
            since=start,
            until=end,
        )
        if test
        else load_configured_daily_usage(config, filters, since=start, until=end)
    )
    by_day = {record["day"]: record for record in records}
    values: list[float] = []
    labels: list[str] = []
    missing_cost = False
    cursor = start
    while cursor <= end:
        record = by_day.get(cursor)
        value = None if record is None else record[field]
        if metric == "cost" and value is None and record is not None:
            missing_cost = True
        numeric = numeric_value(value)
        values.append(0.0 if numeric is None else numeric)
        labels.append(cursor.strftime("%d"))
        cursor += timedelta(days=1)
    if missing_cost:
        typer.echo("Warning: unpriced usage is shown as zero cost.", err=True)
    text = _graph_text(
        labels,
        values,
        f"{label}: {start.isoformat()} to {end.isoformat()}",
        parse_width(width),
        format_cost if metric == "cost" else format_tokens,
    )
    render_graph(text, save=save, sanitize=sanitize)
