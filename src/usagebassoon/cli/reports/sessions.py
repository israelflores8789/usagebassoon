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
    format_cache_multiplier,
    format_cost,
    format_cost_per_million,
    format_timestamp,
    format_tokens,
    load_configured_session_usage,
    parse_width,
    render_table,
    resolve_filters,
    sample_session_usage,
    sanitize_records,
    truncate_middle,
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
    """Render newest-first session usage, optionally split by model."""
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
            limit=limit,
        )
        if test
        else load_configured_session_usage(
            config, filters, by_model=by_model, limit=limit
        )
    )
    records = sanitize_records(records, sanitize)
    output_width = parse_width(width)
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
            "Cost": format_cost(record["cost_usd"], precision=2),
            "Cost/1M": format_cost_per_million(
                record["cost_usd"], record["total_tokens"], precision=2
            ),
            "Last Active": format_timestamp(
                record["last_active"], compact=output_width is not None
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
        ("Last Active", "left"),
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
    label = (
        models[0] if by_model or len(models) == 1 else f"{models[0]} +{len(models) - 1}"
    )
    return truncate_middle(label, width, 12)
