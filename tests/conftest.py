# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""conftest.py — Shared fixtures for the usagebassoon test suite.

Golden payloads were captured from tokscale 4.15.1 and sanitized at daily
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

FIXTURES = Path(__file__).parent / "fixtures"

# Explicit expectations are independent test oracles that catch semantic
# regressions in parsing and normalization instead of merely rechecking the fixture.
# If tests were based on the fixture alone, a parser could silently produce a bug
# because the expected value would be compute from the same transformed fixture data.
EXPECTED_REPORT_ROWS = 81
EXPECTED_DAILY_ROWS = 23
EXPECTED_DAYS = 18
EXPECTED_DAILY_STATS_ROWS = 97
EXPECTED_TOTAL_INPUT = 78_318_668
EXPECTED_TOTAL_OUTPUT = 1_415_099
EXPECTED_TOTAL_CACHE_READ = 496_893_790
EXPECTED_TOTAL_CACHE_WRITE = 0
EXPECTED_TOTAL_REASONING = 1_686_926
EXPECTED_TOTAL_MESSAGES = 4_988
EXPECTED_TOTAL_COST = 109.48238866000003
EXPECTED_GOLDEN_DATE = "2026-09-10"
EXPECTED_TOKSCALE_VERSION = "4.15.1"
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
def report_raw(report_raws: dict[date, JsonArray]) -> JsonArray:
    """Return all daily report rows as the collector's combined report input."""
    return [row for payload in report_raws.values() for row in payload]


@pytest.fixture(scope="session")
def graph_raw() -> JsonObject:
    """Return the raw graph payload as decoded JSON."""
    return _load_object("graph")


@pytest.fixture(scope="session")
def pricing_raw() -> JsonObject:
    """Return the raw pricing payload as decoded JSON."""
    return _load_object("pricing")


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
def recon_result() -> ReconciliationResult:
    """Run full reconciliation over the golden fixture set."""
    return reconcile_all()


@pytest.fixture
def collection_bundle(
    report_rows: list[SessionRow],
    graph_payload: GraphPayload,
    pricing_row: PricingRow,
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
        pricing_by_day={
            day: {
                row.stats.model: pricing_row.model_copy(
                    update={"model_id": row.stats.model}
                )
                for row in payload.entries
            }
            for day, payload in daily_models.items()
        },
        ingest_status=tuple(
            IngestStatus(
                day=day,
                domain=domain,
                status="complete",
                expected_count=(
                    1
                    if domain == "models"
                    else len({row.stats.model for row in payload.entries})
                ),
                succeeded_count=(
                    1
                    if domain == "models"
                    else len({row.stats.model for row in payload.entries})
                ),
                last_attempted_run=run_id,
                last_succeeded_run=run_id,
            )
            for day, payload in daily_models.items()
            for domain in ("models", "pricing")
        ),
        reconciliation=recon_result,
    )
