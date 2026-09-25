# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""models.py — Model and client token usage with calculated timing rates."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.cli.reports._common import (
    CACHE_MULTIPLIER_HEADER,
    SAMPLE_LOCAL_SOURCE_ID,
    ReportFilters,
    format_cache_multiplier,
    format_cost,
    format_cost_per_million,
    format_ms_per_1k_tokens,
    format_tokens,
    load_configured_model_usage,
    parse_report_dates,
    parse_width,
    render_table,
    resolve_filters,
    sample_model_usage,
    sanitize_records,
    truncate_middle,
)


def models(
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
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
        Path | None, typer.Option("--save", help="Write the rendered report as text.")
    ] = None,
) -> None:
    """Render token, cost, and timing totals for each model and client."""
    start, end = parse_report_dates(since, until)
    filters = ReportFilters(
        source=source,
        client=client,
        model=model,
        workspace=workspace,
        tag=tag,
    )
    records = (
        sample_model_usage(
            resolve_filters(filters, SAMPLE_LOCAL_SOURCE_ID), since=start, until=end
        )
        if test
        else load_configured_model_usage(config, filters, since=start, until=end)
    )
    report_width = parse_width(width)
    rows = [
        {
            "Model": truncate_middle(record["model"], report_width, 24),
            "Client": truncate_middle(record["client"], report_width, 12),
            "Input": format_tokens(record["input_tokens"]),
            "Output": format_tokens(record["output_tokens"]),
            "Cache R": format_tokens(record["cache_read"]),
            CACHE_MULTIPLIER_HEADER: format_cache_multiplier(
                record["cache_read"], record["input_tokens"]
            ),
            "Total": format_tokens(record["total_tokens"]),
            "ms/1K": format_ms_per_1k_tokens(
                record["perf_duration_ms"], record["perf_timed_tokens"]
            ),
            "Cost/1M": format_cost_per_million(
                record["cost_usd"], record["total_tokens"]
            ),
            "Cost": format_cost(record["cost_usd"]),
        }
        for record in sanitize_records(records, sanitize)
    ]
    render_table(
        "Model Token Usage",
        (
            ("Model", "left"),
            ("Client", "left"),
            ("Input", "right"),
            ("Output", "right"),
            ("Cache R", "right"),
            (CACHE_MULTIPLIER_HEADER, "right"),
            ("Total", "right"),
            ("ms/1K", "right"),
            ("Cost/1M", "right"),
            ("Cost", "right"),
        ),
        rows,
        width=report_width,
        save=save,
        sanitize=sanitize,
    )
