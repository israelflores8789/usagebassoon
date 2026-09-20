# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""daily.py — Daily token-usage terminal report command."""

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
    format_tokens,
    load_configured_daily_usage,
    parse_width,
    render_table,
    resolve_filters,
    sample_daily_usage,
)


def daily(
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, help="Maximum newest-first dates to display."),
    ] = 16,
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
    """Render newest-first daily token usage and calculated cost."""
    filters = ReportFilters(
        source=source,
        client=client,
        model=model,
        workspace=workspace,
        tag=tag,
    )
    records = (
        sample_daily_usage(
            resolve_filters(filters, SAMPLE_LOCAL_SOURCE_ID), limit=limit
        )
        if test
        else load_configured_daily_usage(config, filters, limit=limit)
    )
    rows = [
        {
            "Date": str(record["day"]),
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
        }
        for record in records
    ]
    render_table(
        "Daily Token Usage",
        (
            ("Date", "left"),
            ("Input", "right"),
            ("Output", "right"),
            ("Cache R", "right"),
            (CACHE_MULTIPLIER_HEADER, "right"),
            ("Total", "right"),
            ("Cost", "right"),
            ("Cost/1M", "right"),
        ),
        rows,
        width=parse_width(width),
        save=save,
        sanitize=sanitize,
    )
