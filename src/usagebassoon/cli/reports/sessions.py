# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""sessions.py — Session token-usage terminal report command."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Annotated

import typer
from rich.cells import cell_len

from usagebassoon.cli._output import render_records
from usagebassoon.cli.reports._common import (
    CACHE_MULTIPLIER_HEADER,
    SAMPLE_LOCAL_SOURCE_ID,
    ReportFilters,
    ReportRecord,
    SessionSort,
    format_cache_multiplier,
    format_cost,
    format_cost_per_million,
    format_ms_per_1k_tokens,
    format_timestamp,
    format_tokens,
    load_configured_session_usage,
    numeric_ratio,
    numeric_value,
    parse_report_dates,
    parse_width,
    resolve_filters,
    sample_session_usage,
    sanitize_records,
    truncate_middle,
)
from usagebassoon.cli.reports._render import (
    ReportColumn,
    output_format,
    render_table,
)
from usagebassoon.display import sanitize_display

_EXPORT_COLUMNS = (
    "source_id",
    "client",
    "session_id",
    "model",
    "raw_output_tokens",
    "reasoning_tokens",
    "input_tokens",
    "output_tokens",
    "cache_read",
    "cache_write",
    "total_tokens",
    "cost_usd",
    "cache_multiplier",
    "cost_per_million",
    "created_at",
    "last_active",
    "last_usage_day",
    "activity_day",
    "last_active_stale",
)


def sessions(
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, help="Maximum rows after sorting."),
    ] = 16,
    by_model: Annotated[
        bool,
        typer.Option("--by-model", help="Show one row per session and model."),
    ] = False,
    with_duration: Annotated[
        bool, typer.Option("--with-duration", help="Show aggregated session duration.")
    ] = False,
    with_performance: Annotated[
        bool,
        typer.Option(
            "--with-performance", help="Show milliseconds per 1K timed tokens."
        ),
    ] = False,
    sort: Annotated[
        SessionSort,
        typer.Option("--sort", help="Sort descending; missing values last."),
    ] = SessionSort.LAST_ACTIVE,
    since: Annotated[
        str | None, typer.Option("--since", help="Inclusive YYYY-MM-DD start.")
    ] = None,
    until: Annotated[
        str | None, typer.Option("--until", help="Inclusive YYYY-MM-DD end.")
    ] = None,
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
        typer.Option(
            "--width",
            help="Report width (minimum 100), or 'max'; both metrics need 120.",
        ),
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
        bool, typer.Option("--json", help="Output report rows as JSON.")
    ] = False,
    csv_output: Annotated[
        bool, typer.Option("--csv", help="Output report rows as CSV.")
    ] = False,
) -> None:
    """Render sortable session usage, optionally split by model."""
    format = output_format(json_output, csv_output)
    output_width = parse_width(width) if format == "text" else None
    if output_width is not None:
        if output_width < 100:
            raise typer.BadParameter(
                "sessions width must be at least 100", param_hint="--width"
            )
        if with_duration and with_performance:
            output_width = max(output_width, 120)
    start, end = parse_report_dates(since, until)
    filters = ReportFilters(
        source=source,
        client=client,
        model=model,
        workspace=workspace,
        tag=tag,
    )
    records = (
        sample_session_usage(
            resolve_filters(filters, SAMPLE_LOCAL_SOURCE_ID),
            by_model=by_model,
            sort=sort,
            limit=limit,
            since=start,
            until=end,
        )
        if test
        else load_configured_session_usage(
            config,
            filters,
            by_model=by_model,
            sort=sort,
            limit=limit,
            since=start,
            until=end,
        )
    )
    if format != "text":
        render_records(
            sanitize_records(_export_rows(records), sanitize),
            columns=_EXPORT_COLUMNS,
            format=format,
            save=save,
        )
        return
    records = sanitize_records(records, sanitize)
    created = sort == SessionSort.CREATED_AT
    date_only = with_duration and output_width is not None and output_width < 105
    timestamp_column = "Created At" if created else "Last Active"
    if date_only:
        timestamp_column = "Created" if created else "Active"
    timestamp_key = "created_at" if created else "last_active"
    rows: list[dict[str, str]] = []
    for record in records:
        row = {
            "Session": sanitize_display(record["session_id"]),
            "Client": sanitize_display(record["client"]),
            "Model": _model_label(record["model"], by_model, output_width),
            "Input": format_tokens(record["input_tokens"]),
            "Output": format_tokens(record["output_tokens"]),
            "Cache R": format_tokens(record["cache_read"]),
            CACHE_MULTIPLIER_HEADER: format_cache_multiplier(
                record["cache_read"], record["input_tokens"]
            ),
            "Total": format_tokens(record["total_tokens"]),
            "Cost": format_cost(record["cost_usd"]),
            "Cost/1M": format_cost_per_million(
                record["cost_usd"], record["total_tokens"]
            ),
            timestamp_column: (
                str(record["activity_day"])
                if not created and record.get("last_active_stale")
                else format_timestamp(
                    record[timestamp_key], compact=output_width is not None
                )
            ),
        }
        if with_duration:
            row["Duration"] = _format_duration(record["perf_duration_ms"])
        if with_performance:
            row["ms/1K"] = format_ms_per_1k_tokens(
                record["perf_timed_duration_ms"], record["perf_timed_tokens"]
            )
        if date_only:
            row[timestamp_column] = row[timestamp_column].split(" ")[0]
        rows.append(row)
    columns = (
        ("Session", "left"),
        ("Client", "left"),
        ("Model", "left"),
        ("Input", "right"),
        ("Output", "right"),
        ("Cache R", "right"),
        (CACHE_MULTIPLIER_HEADER, "right"),
        ("Total", "right"),
        *(("Duration", "right"),) * with_duration,
        *(("ms/1K", "right"),) * with_performance,
        ("Cost", "right"),
        ("Cost/1M", "right"),
        (timestamp_column, "right" if date_only else "left"),
    )
    column_widths = _identifier_layout(rows, columns, output_width)
    render_table(
        "Session Token Usage by Model" if by_model else "Session Token Usage",
        columns,
        rows,
        width=output_width,
        save=save,
        sanitize=sanitize,
        column_widths=column_widths,
    )


def _model_label(value: object, by_model: bool, width: int | None) -> str:
    """Render one model, adding the count of additional session models when needed."""
    models = sanitize_display(value).split(", ")
    model = models[0]
    additional = "" if by_model or len(models) == 1 else f"+{len(models) - 1}"
    if width is None:
        return f"{model}{additional}"

    gemini_label = _gemini_flash_label(model, width)
    identifier = gemini_label or truncate_middle(model, width, 12 + max(0, width - 100))
    return f"{identifier}{additional}"


def _gemini_flash_label(model: str, width: int) -> str | None:
    """Preserve the version of a bounded Gemini Flash model label when possible."""
    prefix = "gemini-"
    suffix = "-flash"
    if not model.startswith(prefix) or not model.endswith(suffix):
        return None

    version = model.removeprefix(prefix).removesuffix(suffix)
    if not version:
        return None

    visible_prefix = min(len("gemini"), 2 + max(0, width - 100))
    if visible_prefix == len("gemini"):
        return model
    return f"{model[:visible_prefix]}…{version}{suffix}"


def _identifier_layout(
    rows: list[dict[str, str]], columns: Sequence[ReportColumn], width: int | None
) -> dict[str, int] | None:
    """Budget identifiers once, revealing Client before Session as width grows.

    Args:
        rows: Formatted rows whose full identifiers are shortened in place.
        columns: Ordered report headers and alignments.
        width: Requested terminal width, or None for complete identifiers.

    Returns:
        Exact column widths, or None for unbounded rendering.
    """
    if width is None:
        return None
    widths = {
        header: max((cell_len(header), *(cell_len(row[header]) for row in rows)))
        for header, _ in columns
    }
    # SIMPLE_HEAVY uses one separator between columns and two outer edges.
    available = (
        width
        - (len(columns) + 1)
        - sum(
            size
            for header, size in widths.items()
            if header not in {"Session", "Client"}
        )
    )
    session_width = min(widths["Session"], 10)
    client_width = min(widths["Client"], 7)
    minimums = {
        header: max(
            (
                3,
                *(
                    cell_len(row[header][:1]) + cell_len(row[header][-1:]) + 1
                    for row in rows
                ),
            )
        )
        for header in ("Session", "Client")
    }
    deficit = max(0, session_width + client_width - available)
    shrink = min(deficit, session_width - minimums["Session"])
    session_width -= shrink
    deficit -= shrink
    client_width -= min(deficit, client_width - minimums["Client"])
    if session_width + client_width > available:
        raise typer.BadParameter(
            "width is too small for the displayed metrics; increase --width",
            param_hint="--width",
        )
    extra = available - session_width - client_width
    grow = min(extra, widths["Client"] - client_width)
    client_width += grow
    extra -= grow
    session_width += extra
    widths["Session"] = session_width
    widths["Client"] = client_width
    for row in rows:
        row["Session"] = truncate_middle(row["Session"], width, session_width)
        row["Client"] = truncate_middle(row["Client"], width, client_width)
    return widths


def _format_duration(value: object) -> str:
    """Format milliseconds as whole seconds with 90-second/minute thresholds."""
    duration = numeric_value(value)
    if duration is None:
        return "—"
    seconds = int(duration / 1000)
    if seconds < 90:
        return f"{seconds:02d}s"
    if seconds < 90 * 60:
        minutes, seconds = divmod(seconds, 60)
        return f"{minutes:02d}m{seconds:02d}s"
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h{minutes:02d}m{seconds:02d}s"


def _export_rows(records: list[ReportRecord]) -> list[ReportRecord]:
    """Build numeric report rows with full identifiers and unrounded rates."""
    rows: list[ReportRecord] = []
    for record in records:
        enriched = {
            **record,
            "cache_multiplier": numeric_ratio(
                record.get("cache_read"), record.get("input_tokens")
            ),
            "cost_per_million": numeric_ratio(
                record.get("cost_usd"), record.get("total_tokens"), 1_000_000
            ),
        }
        rows.append({column: enriched.get(column) for column in _EXPORT_COLUMNS})
    return rows
