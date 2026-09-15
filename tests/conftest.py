# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""conftest.py — Shared fixtures for the usagebassoon test suite.

Golden payloads were captured from tokscale 4.15.1 on 2026-09-10 and
sanitized.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest

from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.normalizer import CollectionBundle
from usagebassoon.parsers.graph import GraphPayload, parse_graph
from usagebassoon.parsers.models import ModelsPayload, parse_models
from usagebassoon.parsers.pricing import PricingRow, parse_pricing
from usagebassoon.parsers.report import SessionRow, parse_report
from usagebassoon.reconcile import ReconciliationResult, reconcile_all

FIXTURES = Path(__file__).parent / "fixtures"

EXPECTED_MODELS_ENTRIES = 88
EXPECTED_REPORT_ROWS = 81
EXPECTED_DAILY_ROWS = 23
EXPECTED_DAYS = 18
EXPECTED_TOTAL_INPUT = 78_318_668
EXPECTED_TOTAL_OUTPUT = 1_415_099
EXPECTED_TOTAL_CACHE_READ = 496_893_790
EXPECTED_TOTAL_CACHE_WRITE = 0
EXPECTED_TOTAL_REASONING = 1_686_926
EXPECTED_TOTAL_MESSAGES = 4_988
EXPECTED_TOTAL_COST = 109.48238866000003
EXPECTED_TOKSCALE_VERSION = "4.15.1"
SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _load(name: str) -> JsonValue:
    """Load a golden fixture by stem name.

    Args:
        name: Fixture stem, e.g. "models".

    Returns:
        The decoded JSON payload.
    """
    return cast(
        JsonValue,
        json.loads((FIXTURES / f"golden-2026-09-10.{name}.json").read_text()),
    )


def _load_object(name: str) -> JsonObject:
    """Load a golden fixture known to have an object top-level shape.

    Args:
        name: Fixture stem, e.g. "models".

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


@pytest.fixture(scope="session")
def models_raw() -> JsonObject:
    """Return the raw models payload as decoded JSON."""
    return _load_object("models")


@pytest.fixture(scope="session")
def report_raw() -> JsonArray:
    """Return the raw report payload as decoded JSON."""
    return _load_array("report")


@pytest.fixture(scope="session")
def graph_raw() -> JsonObject:
    """Return the raw graph payload as decoded JSON."""
    return _load_object("graph")


@pytest.fixture(scope="session")
def pricing_raw() -> JsonObject:
    """Return the raw pricing payload as decoded JSON."""
    return _load_object("pricing")


@pytest.fixture(scope="session")
def models_payload(models_raw: JsonObject) -> ModelsPayload:
    """Return the validated models payload."""
    return parse_models(models_raw)


@pytest.fixture(scope="session")
def report_rows(report_raw: JsonArray) -> list[SessionRow]:
    """Return the validated report rows."""
    return parse_report(report_raw)


@pytest.fixture(scope="session")
def graph_payload(graph_raw: JsonObject) -> GraphPayload:
    """Return the validated graph payload."""
    return parse_graph(graph_raw)


@pytest.fixture(scope="session")
def pricing_row(pricing_raw: JsonObject) -> PricingRow:
    """Return the validated pricing row."""
    return parse_pricing(pricing_raw)


@pytest.fixture(scope="session")
def recon_result(
    models_payload: ModelsPayload,
    report_rows: list[SessionRow],
    graph_payload: GraphPayload,
) -> ReconciliationResult:
    """Run full reconciliation over the golden fixture set."""
    return reconcile_all(models_payload, report_rows, graph_payload)


@pytest.fixture
def collection_bundle(
    models_payload: ModelsPayload,
    report_rows: list[SessionRow],
    graph_payload: GraphPayload,
    pricing_row: PricingRow,
    models_raw: JsonObject,
    report_raw: JsonArray,
    graph_raw: JsonObject,
    pricing_raw: JsonObject,
    recon_result: ReconciliationResult,
) -> CollectionBundle:
    """Build a complete, validated CollectionBundle from the fixtures."""
    return CollectionBundle(
        run_id=str(uuid4()),
        source_id=SOURCE_ID,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC) + timedelta(seconds=2),
        host="pytest",
        models=models_payload,
        report_rows=report_rows,
        graph=graph_payload,
        pricing_by_model={"gemini-3.8-flash": pricing_row},
        reconciliation=recon_result,
    )
