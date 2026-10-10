# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_parsers.py — Parser-level golden-file tests (tokscale 4.18.0 fixture set)."""

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
from usagebassoon.json_types import JsonObject
from usagebassoon.parsers.daily import DailyModelsPayload, parse_daily, parse_models
from usagebassoon.parsers.graph import GraphPayload, parse_graph
from usagebassoon.parsers.pricing import PricingRow, parse_pricing
from usagebassoon.parsers.report import SessionRow, make_session_label, parse_report


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
    assert row.tokscale_ms_per_1k_tokens is not None
    assert row.perf_duration_ms is not None
    assert row.perf_timed_tokens is not None
    assert row.perf_sample_count is not None
    assert row.tokscale_ms_per_1k_tokens == pytest.approx(
        1_000 * row.perf_duration_ms / row.perf_timed_tokens
    )
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
    pricing_raws: dict[str, JsonObject],
) -> None:
    """Reject invalid token and pricing values before normalization."""
    day = min(daily_raws)
    invalid_models = dict(daily_raws[day])
    entries = invalid_models["entries"]
    assert isinstance(entries, list)
    first_entry = entries[0]
    assert isinstance(first_entry, dict)
    invalid_models["entries"] = [{**first_entry, "input": -1}, *entries[1:]]
    with pytest.raises(ValidationError):
        parse_daily(invalid_models, day=day)
    for pricing_raw in pricing_raws.values():
        pricing = pricing_raw["pricing"]
        assert isinstance(pricing, dict)
        invalid_pricing = {
            **pricing_raw,
            "pricing": {**pricing, "inputCostPerToken": inf},
        }
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


def test_report_rows_and_timestamps(report_rows: list[SessionRow]) -> None:
    """Preserve all golden sessions and parse timestamps as aware UTC values."""
    assert len(report_rows) == EXPECTED_REPORT_ROWS
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


def test_report_duplicate_session_rows_are_merged() -> None:
    """Merge repeated daily report rows using stable session metadata rules."""
    rows = parse_report(
        [
            {
                "client": "codex",
                "session_id": "session-1",
                "workspace": "/project",
                "workspace_label": "project",
                "created_at": "2026-09-10T12:00:00+00:00",
                "last_active": "2026-09-10T12:30:00+00:00",
                "models_used": ["gpt-5"],
                "message_count": 1,
                "total_cost": 0.1,
            },
            {
                "client": "codex",
                "session_id": "session-1",
                "workspace": "/project",
                "workspace_label": "project",
                "created_at": "2026-09-10T12:05:00+00:00",
                "last_active": "2026-09-10T13:00:00+00:00",
                "models_used": ["gpt-5", "gpt-5-mini"],
                "message_count": 2,
                "total_cost": 0.2,
            },
        ]
    )

    assert len(rows) == 1
    assert rows[0].created_at == datetime(2026, 9, 10, 12, tzinfo=UTC)
    assert rows[0].last_active == datetime(2026, 9, 10, 13, tzinfo=UTC)
    assert rows[0].client == "codex"
    assert rows[0].workspace == "/project"
    assert rows[0].workspace_label == "project"
    assert rows[0].models_used == ("gpt-5", "gpt-5-mini")
    assert rows[0].message_count == 2
    assert rows[0].tokscale_cost_usd == 0.2


@pytest.mark.parametrize("field", ["workspace", "workspace_label"])
def test_report_duplicate_stable_metadata_must_match(field: str) -> None:
    """Reject duplicate report rows with conflicting stable metadata."""
    first: JsonObject = {
        "client": "codex",
        "session_id": "session-1",
        "workspace": "/project",
        "workspace_label": "project",
    }
    duplicate: JsonObject = {
        **first,
        field: "/other" if field == "workspace" else "other",
    }

    with pytest.raises(ValueError, match=f"{field} must match"):
        parse_report([first, duplicate])


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


def test_pricing_shape(pricing_rows: dict[str, PricingRow]) -> None:
    """Assert the pricing fixture carries provenance and nullable rates."""
    pricing_row = pricing_rows["gemini-3.8-flash"]
    assert pricing_row.model_id == "gemini-3.8-flash"
    assert pricing_row.matched_key == "gemini-3.8-flash"
    assert pricing_row.source == "LiteLLM"
    assert pricing_row.resolution.kind == "exact"
    assert pricing_row.resolution.submission_safe is True
    assert pricing_row.pricing.input_cost_per_token == 7.5e-07
    assert pricing_row.pricing.output_cost_per_token == 3.75e-06
    assert pricing_row.pricing.cache_read_input_token_cost == 7.5e-08
    assert pricing_row.pricing.cache_write_input_token_cost is None

    payload = pricing_row.model_dump(by_alias=True)
    for source in ("Custom", "user"):
        payload["source"] = source
        assert parse_pricing(payload).source == source


def test_pricing_cache_creation_is_retained(
    pricing_rows: dict[str, PricingRow],
) -> None:
    """Preserve real GPT cache-creation prices under the canonical cache-write name."""
    assert pricing_rows["gpt-6-sol"].pricing.cache_write_input_token_cost == 2.5e-6
    assert pricing_rows["gpt-6-luna"].pricing.cache_write_input_token_cost == 1.25e-7
    assert pricing_rows["gemini-3.8-flash"].pricing.cache_write_input_token_cost is None


def test_pricing_captures_cover_used_models(
    daily_models: dict[date, DailyModelsPayload],
    pricing_raws: dict[str, JsonObject],
    pricing_rows: dict[str, PricingRow],
) -> None:
    """Require captured prices for used models and retain every card's identity."""
    used_models = {
        entry.stats.model
        for payload in daily_models.values()
        for entry in payload.entries
    }
    assert used_models <= pricing_raws.keys()
    assert pricing_raws.keys() == pricing_rows.keys()
    for model, row in pricing_rows.items():
        assert row.model_id == model == pricing_raws[model]["modelId"]
        assert row.matched_key == pricing_raws[model]["matchedKey"]
