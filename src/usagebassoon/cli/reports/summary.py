# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""summary.py — Backward-compatible warehouse summary terminal report command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from usagebassoon.cli.reports._common import (
    SAMPLE_LOCAL_SOURCE_ID,
    ReportFilters,
    ReportRecord,
    format_cost,
    format_tokens,
    integer_value,
    load_configured_session_usage,
    numeric_value,
    parse_width,
    render_tables,
    resolve_filters,
    sample_session_usage,
)


def _model_rows(model_records: list[ReportRecord]) -> list[dict[str, str]]:
    """Aggregate session/model rows into descending-cost model display rows."""
    models: dict[str, list[ReportRecord]] = {}
    for record in model_records:
        models.setdefault(str(record["model"]), []).append(record)
    ranked: list[tuple[float, dict[str, str]]] = []
    for name, records in models.items():
        costs = [numeric_value(record["cost_usd"]) for record in records]
        total_cost = (
            None
            if any(cost is None for cost in costs)
            else sum(cost for cost in costs if cost is not None)
        )
        ranked.append(
            (
                float("-inf") if total_cost is None else total_cost,
                {
                    "Model": name,
                    "Total Tokens": format_tokens(
                        sum(integer_value(record["total_tokens"]) for record in records)
                    ),
                    "Cost (USD)": format_cost(total_cost),
                },
            )
        )
    ranked.sort(key=_model_row_sort_key, reverse=True)
    return [row for _, row in ranked]


def _model_row_sort_key(item: tuple[float, dict[str, str]]) -> tuple[float, str]:
    """Return the cost and model tie-breaker for one summary model row."""
    return item[0], item[1]["Model"]


def summary(
    config: Annotated[
        Path | None, typer.Option("--config", help="Use this configuration file.")
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
    """Render a warehouse-wide session and model summary."""
    filters = ReportFilters(
        source=source,
        client=client,
        model=model,
        workspace=workspace,
        tag=tag,
    )
    if test:
        resolved = resolve_filters(filters, SAMPLE_LOCAL_SOURCE_ID)
        session_records = sample_session_usage(resolved, by_model=False)
        model_records = sample_session_usage(resolved, by_model=True)
    else:
        session_records = load_configured_session_usage(config, filters, by_model=False)
        model_records = load_configured_session_usage(config, filters, by_model=True)
    costs = [numeric_value(record["cost_usd"]) for record in session_records]
    summary_cost = (
        None
        if any(cost is None for cost in costs)
        else sum(cost for cost in costs if cost is not None)
    )
    model_rows = _model_rows(model_records)
    render_tables(
        (
            (
                "UsageBassoon Summary",
                (("Metric", "left"), ("Value", "right")),
                (
                    {"Metric": "Sessions", "Value": str(len(session_records))},
                    {"Metric": "Cost (USD)", "Value": format_cost(summary_cost)},
                ),
            ),
            (
                "Models",
                (
                    ("Model", "left"),
                    ("Total Tokens", "right"),
                    ("Cost (USD)", "right"),
                ),
                model_rows,
            ),
        ),
        width=parse_width(width),
        save=save,
        sanitize=sanitize,
    )
