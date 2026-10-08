# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""conftest.py — Shared fixtures for the usagebassoon test suite.

Golden payloads were captured from tokscale 4.18.0 and sanitized at daily
collection granularity.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest

from usagebassoon.ingest import CollectionBundle, IngestStatus
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.parsers.daily import DailyModelsPayload, parse_daily
from usagebassoon.parsers.graph import GraphPayload, parse_graph
from usagebassoon.parsers.pricing import PricingRow, parse_pricing
from usagebassoon.parsers.report import SessionRow, parse_report
from usagebassoon.reconcile import ReconciliationResult, reconcile_all

# Explicit expectations are independent test oracles that catch semantic
# regressions in parsing and normalization instead of merely rechecking the fixture.
# If tests were based on the fixture alone, a parser could silently produce a bug
# because the expected value would be compute from the same transformed fixture data.
EXPECTED_REPORT_ROWS = 181
EXPECTED_DAILY_ROWS = 57
EXPECTED_DAYS = 35
EXPECTED_DAILY_STATS_ROWS = 238
EXPECTED_TOTAL_INPUT = 109_900_335
EXPECTED_TOTAL_OUTPUT = 3_230_700
EXPECTED_TOTAL_CACHE_READ = 1_225_022_558
EXPECTED_TOTAL_CACHE_WRITE = 0
EXPECTED_TOTAL_REASONING = 4_389_637
EXPECTED_TOTAL_MESSAGES = 11_656
EXPECTED_TOTAL_COST = 255.2014217
EXPECTED_GOLDEN_DATE = "2026-09-30"
EXPECTED_PRICING_DATE = "2026-10-08"
EXPECTED_TOKSCALE_VERSION = "4.18.0"
FIXTURES = Path(__file__).parent / "fixtures" / f"tokscale-{EXPECTED_TOKSCALE_VERSION}"
FIXTURE_PREFIX = f"golden-{EXPECTED_GOLDEN_DATE}-tokscale-{EXPECTED_TOKSCALE_VERSION}"
SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _load(name: str) -> JsonValue:
    """Load a golden fixture by stem name.

    Args:
        name: Fixture stem, e.g. "graph".

    Returns:
        The decoded JSON payload.
    """
    return cast(
        JsonValue,
        json.loads((FIXTURES / f"{FIXTURE_PREFIX}.{name}.json").read_text()),
    )


def _load_object(name: str) -> JsonObject:
    """Load a golden fixture known to have an object top-level shape.

    Args:
        name: Fixture stem, e.g. "graph".

    Returns:
        The decoded JSON object.

    Raises:
        TypeError: If the fixture's top-level shape is not an object.
    """
    payload = _load(name)
    if not isinstance(payload, dict):
        raise TypeError(f"fixture {name} must be a JSON object")
    return payload


def _load_array(name: str) -> JsonArray:
    """Load a golden fixture known to have an array top-level shape.

    Args:
        name: Fixture stem, e.g. "report".

    Returns:
        The decoded JSON array.

    Raises:
        TypeError: If the fixture's top-level shape is not an array.
    """
    payload = _load(name)
    if not isinstance(payload, list):
        raise TypeError(f"fixture {name} must be a JSON array")
    return payload


def _load_daily(day: date) -> JsonObject:
    """Load one date-filtered models fixture.

    Args:
        day: Requested tokscale day.

    Returns:
        The decoded date-filtered models JSON object.
    """
    payload = cast(
        JsonValue,
        json.loads(
            (
                FIXTURES
                / (
                    f"golden-{day.isoformat()}-tokscale-"
                    f"{EXPECTED_TOKSCALE_VERSION}.daily.json"
                )
            ).read_text()
        ),
    )
    if not isinstance(payload, dict):
        raise TypeError(f"daily fixture {day.isoformat()} must be a JSON object")
    return payload


def _load_report(day: date) -> JsonArray:
    """Load one date-filtered tokscale report fixture."""
    payload = cast(
        JsonValue,
        json.loads(
            (
                FIXTURES
                / (
                    f"golden-{day.isoformat()}-tokscale-"
                    f"{EXPECTED_TOKSCALE_VERSION}.report.json"
                )
            ).read_text()
        ),
    )
    if not isinstance(payload, list):
        raise TypeError(f"report fixture {day.isoformat()} must be a JSON array")
    return payload


@pytest.fixture(scope="session")
def report_raw() -> JsonArray:
    """Return the complete report captured within the configured usage-date range."""
    return _load_array("report-full-history")


@pytest.fixture(scope="session")
def graph_raw() -> JsonObject:
    """Return the raw graph payload as decoded JSON."""
    return _load_object("graph")


@pytest.fixture(scope="session")
def pricing_raw() -> JsonObject:
    """Return the raw pricing payload as decoded JSON."""
    return _load_pricing("gemini-3.8-flash")


def _load_pricing(model: str) -> JsonObject:
    """Load one observed model rate card without rewriting its identity."""
    path = FIXTURES / (
        f"golden-{EXPECTED_PRICING_DATE}-tokscale-"
        f"{EXPECTED_TOKSCALE_VERSION}.pricing.{model}.json"
    )
    payload = cast(JsonValue, json.loads(path.read_text()))
    if not isinstance(payload, dict):
        raise TypeError(f"pricing fixture {model} must be a JSON object")
    return cast(JsonObject, payload)


@pytest.fixture(scope="session")
def pricing_raws() -> dict[str, JsonObject]:
    """Return the seven independent model rate cards captured in this batch."""
    return {
        model: _load_pricing(model)
        for model in (
            "gemini-3.7-flash",
            "gemini-3.8-flash",
            "gpt-5.6-luna",
            "gpt-5.6-terra",
            "gpt-6-luna",
            "gpt-6-sol",
            "gpt-6.1-sol",
        )
    }


@pytest.fixture(scope="session")
def report_rows(report_raw: JsonArray) -> list[SessionRow]:
    """Return the validated report rows."""
    return parse_report(report_raw)


@pytest.fixture(scope="session")
def graph_payload(graph_raw: JsonObject) -> GraphPayload:
    """Return the validated graph payload."""
    return parse_graph(graph_raw)


@pytest.fixture(scope="session")
def report_raws(graph_payload: GraphPayload) -> dict[date, JsonArray]:
    """Return daily tokscale report output for every graph candidate day."""
    return {
        contribution.date: _load_report(contribution.date)
        for contribution in graph_payload.contributions
    }


@pytest.fixture(scope="session")
def daily_raws(graph_payload: GraphPayload) -> dict[date, JsonObject]:
    """Return daily models payloads for every graph candidate day."""
    return {
        contribution.date: _load_daily(contribution.date)
        for contribution in graph_payload.contributions
    }


@pytest.fixture(scope="session")
def daily_models(daily_raws: dict[date, JsonObject]) -> dict[date, DailyModelsPayload]:
    """Return date-attached model statistics for graph candidate days."""
    return {day: parse_daily(payload, day=day) for day, payload in daily_raws.items()}


@pytest.fixture(scope="session")
def pricing_row(pricing_raw: JsonObject) -> PricingRow:
    """Return the validated pricing row."""
    return parse_pricing(pricing_raw)


@pytest.fixture(scope="session")
def pricing_rows(pricing_raws: dict[str, JsonObject]) -> dict[str, PricingRow]:
    """Return parsed rate cards preserving each model's actual prices."""
    return {model: parse_pricing(payload) for model, payload in pricing_raws.items()}


@pytest.fixture(scope="session")
def recon_result(daily_models: dict[date, DailyModelsPayload]) -> ReconciliationResult:
    """Run full reconciliation over the golden fixture set."""
    return reconcile_all(daily_models)


@pytest.fixture
def collection_bundle(
    report_rows: list[SessionRow],
    graph_payload: GraphPayload,
    pricing_rows: dict[str, PricingRow],
    daily_models: dict[date, DailyModelsPayload],
    recon_result: ReconciliationResult,
) -> CollectionBundle:
    """Build a complete, validated CollectionBundle from the fixtures."""
    run_id = str(uuid4())
    return CollectionBundle(
        run_id=run_id,
        source_id=SOURCE_ID,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC) + timedelta(seconds=2),
        host="pytest",
        daily_models=daily_models,
        report_rows=report_rows,
        graph=graph_payload,
        pricing_by_day={date.fromisoformat(EXPECTED_PRICING_DATE): pricing_rows},
        ingest_status=(
            *(
                IngestStatus(
                    day=day,
                    domain="models",
                    status="complete",
                    expected_count=1,
                    succeeded_count=1,
                    run_id=run_id,
                )
                for day in daily_models
            ),
            IngestStatus(
                day=date.fromisoformat(EXPECTED_PRICING_DATE),
                domain="pricing",
                status="complete",
                expected_count=len(pricing_rows),
                succeeded_count=len(pricing_rows),
                run_id=run_id,
            ),
        ),
        reconciliation=recon_result,
    )
