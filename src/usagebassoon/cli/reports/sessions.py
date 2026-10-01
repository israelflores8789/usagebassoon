# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""sessions.py — Session token-usage terminal report command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.cli.reports._common import (
    CACHE_MULTIPLIER_HEADER,
    SAMPLE_LOCAL_SOURCE_ID,
    ReportFilters,
    ReportRecord,
    format_cache_multiplier,
    format_cost,
    format_cost_per_million,
    format_timestamp,
    format_tokens,
    load_configured_session_usage,
    numeric_ratio,
    parse_report_dates,
    parse_width,
    resolve_filters,
    sample_session_usage,
    sanitize_records,
    truncate_middle,
)
from usagebassoon.cli.reports._render import output_format, render_records, render_table

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
)


def sessions(
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, help="Maximum most-recent sessions to display."),
    ] = 16,
    by_model: Annotated[
        bool,
        typer.Option("--by-model", help="Show one row per session and model."),
    ] = False,
    by_created_at: Annotated[
        bool,
        typer.Option(
            "--by-created-at", help="Filter and sort by session creation time."
        ),
    ] = False,
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
    """Render newest-first session usage, optionally split by model."""
    format = output_format(json_output, csv_output)
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
            by_created_at=by_created_at,
            limit=limit,
            since=start,
            until=end,
        )
        if test
        else load_configured_session_usage(
            config,
            filters,
            by_model=by_model,
            by_created_at=by_created_at,
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
    output_width = parse_width(width)
    timestamp_column = "Created At" if by_created_at else "Last Active"
    timestamp_key = "created_at" if by_created_at else "last_active"
    rows: list[dict[str, str]] = []
    for record in records:
        row = {
            "Session": truncate_middle(record["session_id"], output_width, 10),
            "Client": truncate_middle(record["client"], output_width, 7),
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
            timestamp_column: format_timestamp(
                record[timestamp_key], compact=output_width is not None
            ),
        }
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
        ("Cost", "right"),
        ("Cost/1M", "right"),
        (timestamp_column, "left"),
    )
    render_table(
        "Session Token Usage by Model" if by_model else "Session Token Usage",
        columns,
        rows,
        width=output_width,
        save=save,
        sanitize=sanitize,
    )


def _model_label(value: object, by_model: bool, width: int | None) -> str:
    """Render one model, adding the count of additional session models when needed."""
    models = str(value).split(", ")
    model = models[0]
    additional = "" if by_model or len(models) == 1 else f"+{len(models) - 1}"
    if width is None:
        return f"{model}{additional}"

    gemini_label = _gemini_flash_label(model, width) if width >= 95 else None
    maximum = 12 if width >= 95 else max(1, 12 - len(additional))
    identifier = gemini_label or truncate_middle(model, width, maximum)
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
