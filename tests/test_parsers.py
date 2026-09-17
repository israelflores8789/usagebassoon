# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_parsers.py — Parser-level golden-file tests (tokscale 4.15.1 fixture set)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from tests.conftest import (
    EXPECTED_DAILY_ROWS,
    EXPECTED_DAILY_STATS_ROWS,
    EXPECTED_DAYS,
    EXPECTED_MODELS_ENTRIES,
    EXPECTED_REPORT_ROWS,
    EXPECTED_TOKSCALE_VERSION,
)
from usagebassoon.json_types import JsonArray, JsonObject
from usagebassoon.parsers.daily import DailyModelsPayload, parse_daily
from usagebassoon.parsers.graph import GraphPayload, parse_graph
from usagebassoon.parsers.models import ModelsPayload, parse_models
from usagebassoon.parsers.pricing import PricingRow
from usagebassoon.parsers.report import SessionRow, make_session_label, parse_report


def test_models_shape(models_payload: ModelsPayload) -> None:
    """Assert the models payload carries the expected fixture shape."""
    assert len(models_payload.entries) == EXPECTED_MODELS_ENTRIES
    assert models_payload.group_by == "client,session,model"
    assert models_payload.processing_time_ms is not None


def test_models_field_promotion(models_payload: ModelsPayload) -> None:
    """Assert the undocumented performance object flattens onto each row."""
    row = next(e for e in models_payload.entries if e.model == "gemini-3.8-flash")
    assert row.ms_per_1k_tokens is not None
    assert row.perf_duration_ms is not None
    assert 0.0 <= (row.perf_token_coverage or 0.0) <= 1.0


def test_models_merged_clients_nullable(models_payload: ModelsPayload) -> None:
    """Assert mergedClients nulls normalize to empty tuples."""
    assert all(e.merged_clients == () for e in models_payload.entries)


def test_models_invalid_payload_type() -> None:
    """Assert non-object payloads are rejected."""
    with pytest.raises(ValueError, match="must be a JSON object"):
        parse_models(["not", "an", "object"])


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


def test_daily_models_reuse_the_models_contract(models_raw: JsonObject) -> None:
    """Reject non-model payloads through the shared strict models parser."""
    with pytest.raises(ValueError, match="must be a JSON object"):
        parse_daily([], day=date(2026, 9, 10))
    assert parse_daily(models_raw, day=date(2026, 9, 10)).entries


def test_report_shape(report_rows: list[SessionRow]) -> None:
    """Assert the report payload carries the expected fixture shape."""
    assert len(report_rows) == EXPECTED_REPORT_ROWS


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
