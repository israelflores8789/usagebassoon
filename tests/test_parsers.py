# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_parsers.py — Parser-level golden-file tests (tokscale 4.15.1 fixture set)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from math import inf

import pytest
from pydantic import ValidationError

from tests.conftest import (
    EXPECTED_DAILY_ROWS,
    EXPECTED_DAILY_STATS_ROWS,
    EXPECTED_DAYS,
    EXPECTED_REPORT_ROWS,
    EXPECTED_TOKSCALE_VERSION,
)
from usagebassoon.json_types import JsonArray, JsonObject
from usagebassoon.parsers.daily import DailyModelsPayload, parse_daily, parse_models
from usagebassoon.parsers.graph import GraphPayload, parse_graph
from usagebassoon.parsers.pricing import PricingRow, parse_pricing
from usagebassoon.parsers.report import SessionRow, make_session_label, parse_report


def test_daily_models_shape(
    daily_models: dict[date, DailyModelsPayload],
) -> None:
    """Assert date-filtered models fixtures preserve the expected total grain."""
    assert sum(len(payload.entries) for payload in daily_models.values()) == (
        EXPECTED_DAILY_STATS_ROWS
    )


def test_models_field_promotion(
    daily_models: dict[date, DailyModelsPayload],
) -> None:
    """Assert the undocumented performance object flattens onto each row."""
    row = next(
        daily.stats
        for payload in daily_models.values()
        for daily in payload.entries
        if daily.stats.model == "gemini-3.8-flash"
    )
    assert row.ms_per_1k_tokens is not None
    assert row.perf_duration_ms is not None
    assert 0.0 <= (row.perf_token_coverage or 0.0) <= 1.0


def test_models_merged_clients_nullable(
    daily_models: dict[date, DailyModelsPayload],
) -> None:
    """Assert mergedClients nulls normalize to empty tuples."""
    assert all(
        entry.stats.merged_clients == ()
        for payload in daily_models.values()
        for entry in payload.entries
    )


def test_models_invalid_payload_type() -> None:
    """Assert non-object payloads are rejected."""
    with pytest.raises(ValueError, match="must be a JSON object"):
        parse_models(["not", "an", "object"])


def test_tokscale_metrics_reject_negative_and_non_finite_values(
    daily_raws: dict[date, JsonObject],
    pricing_raw: JsonObject,
) -> None:
    """Reject invalid token and pricing values before normalization."""
    day = min(daily_raws)
    invalid_models = dict(daily_raws[day])
    entries = invalid_models["entries"]
    assert isinstance(entries, list)
    first_entry = entries[0]
    assert isinstance(first_entry, dict)
    invalid_models["entries"] = [{**first_entry, "input": -1}, *entries[1:]]
    invalid_pricing = dict(pricing_raw)
    pricing = pricing_raw["pricing"]
    assert isinstance(pricing, dict)
    invalid_pricing["pricing"] = {**pricing, "inputCostPerToken": inf}

    with pytest.raises(ValidationError):
        parse_daily(invalid_models, day=day)
    with pytest.raises(ValidationError):
        parse_pricing(invalid_pricing)


def test_daily_models_attach_requested_days(
    daily_models: dict[date, DailyModelsPayload],
) -> None:
    """Persist each date-filtered models row at the requested day grain."""
    assert (
        sum(len(payload.entries) for payload in daily_models.values())
        == EXPECTED_DAILY_STATS_ROWS
    )
    assert all(
        row.day == day
        for day, payload in daily_models.items()
        for row in payload.entries
    )


def test_daily_models_reuse_the_models_contract(
    daily_raws: dict[date, JsonObject],
) -> None:
    """Reject non-model payloads through the shared strict models parser."""
    with pytest.raises(ValueError, match="must be a JSON object"):
        parse_daily([], day=date(2026, 9, 10))
    assert parse_daily(daily_raws[min(daily_raws)], day=min(daily_raws)).entries


def test_report_shape(report_rows: list[SessionRow]) -> None:
    """Assert the report payload carries the expected fixture shape."""
    assert len(report_rows) == EXPECTED_REPORT_ROWS


def test_daily_reports_are_partitioned_by_session_creation_date(
    report_raws: dict[date, JsonArray],
) -> None:
    """Keep report metadata daily by session creation, not usage-token day."""
    for day, payload in report_raws.items():
        assert all(
            row.created_at is not None and row.created_at.date() == day
            for row in parse_report(payload)
        )


def test_report_timestamps_parsed(report_rows: list[SessionRow]) -> None:
    """Assert epoch-millis fields become timezone-aware datetimes."""
    row = report_rows[0]
    assert isinstance(row.created_at, datetime)
    assert row.created_at.tzinfo is not None
    assert row.created_at.utcoffset() == UTC.utcoffset(row.created_at)


def test_report_no_llm_summary_fields(report_rows: list[SessionRow]) -> None:
    """Assert summary fields never reach the curated layer."""
    excluded = {
        "title",
        "task_category",
        "task_group",
        "description",
        "complexity",
        "summarized_at",
        "fm_version",
    }
    for row in report_rows:
        assert excluded.isdisjoint(row.model_fields_set | set(row.model_dump()))


def test_report_duplicate_session_rejected(report_raw: JsonArray) -> None:
    """Assert duplicate session keys are rejected."""
    with pytest.raises(ValueError, match="duplicate report session key"):
        parse_report([*report_raw, report_raw[0]])


def test_session_label_unique(report_rows: list[SessionRow]) -> None:
    """Assert deterministic labels are unique across all fixture sessions."""
    labels = [make_session_label(r) for r in report_rows]
    assert len(set(labels)) == len(labels)


def test_graph_shape(graph_payload: GraphPayload) -> None:
    """Assert the graph payload carries the expected fixture shape."""
    assert len(graph_payload.contributions) == EXPECTED_DAYS
    assert (
        sum(len(c.clients) for c in graph_payload.contributions) == EXPECTED_DAILY_ROWS
    )
    assert graph_payload.meta.version == EXPECTED_TOKSCALE_VERSION
    assert graph_payload.contributions[0].date == date(2026, 8, 22)


def test_graph_duplicate_dates_rejected(graph_raw: JsonObject) -> None:
    """Assert duplicate contribution dates are rejected."""
    dup = dict(graph_raw)
    contributions = graph_raw["contributions"]
    assert isinstance(contributions, list)
    dup["contributions"] = [*contributions, contributions[0]]
    with pytest.raises(ValueError, match="duplicate contribution date"):
        parse_graph(dup)


def test_pricing_shape(pricing_row: PricingRow) -> None:
    """Assert the pricing fixture carries provenance and nullable rates."""
    assert pricing_row.model_id == "gemini-3.8-flash"
    assert pricing_row.matched_key == "gemini-3.8-flash"
    assert pricing_row.source == "LiteLLM"
    assert pricing_row.resolution.kind == "exact"
    assert pricing_row.resolution.submission_safe is True
    assert pricing_row.pricing.input_cost_per_token == 7.5e-07
    assert pricing_row.pricing.output_cost_per_token == 3.75e-06
    assert pricing_row.pricing.cache_read_input_token_cost == 7.5e-08
    assert pricing_row.pricing.cache_write_input_token_cost is None
