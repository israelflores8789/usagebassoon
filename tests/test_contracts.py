# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_contracts.py — Tests validation over shipped contracts and synthetic drift."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from uuid import uuid4

import pytest

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.contracts import (
    ContractValidationError,
    build_contract,
    diff_contract,
    load_shipped_contracts,
    validate_payloads,
)
from usagebassoon.ingest import RawCollection, build_collection_bundle
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.merge import persist_run
from usagebassoon.normalizer import normalize

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _payloads(
    daily_raws: dict[date, JsonObject],
    report_raw: JsonArray,
    graph_raw: JsonObject,
    pricing_raw: JsonObject,
) -> dict[str, tuple[JsonValue, ...]]:
    """Build the raw payload mapping expected by contract validation."""
    return {
        "models": tuple(daily_raws.values()),
        "report": (report_raw,),
        "graph": (graph_raw,),
        "pricing": (pricing_raw,),
    }


def _raw_collection(
    *,
    day: date,
    daily_models: dict[date, JsonObject],
    report: JsonArray,
    graph: JsonObject,
    pricing: JsonObject,
) -> RawCollection:
    """Build one complete raw collection fixture for contract tests."""
    return RawCollection(
        daily_models=daily_models,
        report_by_day={day: report},
        report_days=frozenset({day}),
        report_fetch_failures=frozenset(),
        graph=graph,
        pricing_by_day={day: {"gemini-3.8-flash": pricing}},
        pricing_expected_models={day: frozenset({"gemini-3.8-flash"})},
        pricing_existing_models={},
        pricing_fetch_failures={},
        prior_statuses={},
    )


def test_shipped_contracts_accept_the_golden_payloads(
    daily_raws: dict[date, JsonObject],
    report_raw: JsonArray,
    graph_raw: JsonObject,
    pricing_raw: JsonObject,
) -> None:
    """Assert checked-in contracts reproduce their sanitized source payloads."""
    result = validate_payloads(
        _payloads(daily_raws, report_raw, graph_raw, pricing_raw),
        run_id=str(uuid4()),
    )
    assert result == type(result)(events=(), fatal=False)


def test_unknown_field_is_non_fatal_and_reaches_the_collection_bundle(
    daily_raws: dict[date, JsonObject],
    report_raw: JsonArray,
    graph_raw: JsonObject,
    pricing_raw: JsonObject,
) -> None:
    """Assert additive raw fields are preserved as non-fatal drift events."""
    day = min(daily_raws)
    changed_models = {**daily_raws[day], "futureMetric": 1}
    when = datetime.now(UTC)
    bundle = build_collection_bundle(
        _raw_collection(
            day=day,
            daily_models={day: changed_models},
            report=report_raw,
            graph=graph_raw,
            pricing=pricing_raw,
        ),
        run_id=str(uuid4()),
        source_id=SOURCE_ID,
        started_at=when,
        finished_at=when,
        host="pytest",
    )
    assert bundle.drift_fatal is False
    assert [(event.drift_kind, event.path) for event in bundle.contract_drift] == [
        ("unknown_field", "futureMetric")
    ]
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(bundle))
        assert backend.query(
            "SELECT payload_kind, drift_kind, path, resolved FROM schema_drift"
        ).to_pylist() == [
            {
                "payload_kind": "models",
                "drift_kind": "unknown_field",
                "path": "futureMetric",
                "resolved": False,
            }
        ]
    finally:
        backend.close()


def test_required_missing_field_blocks_parsing(
    daily_raws: dict[date, JsonObject],
    report_raw: JsonArray,
    graph_raw: JsonObject,
    pricing_raw: JsonObject,
) -> None:
    """Assert required contract loss raises before parser invocation."""
    day = min(daily_raws)
    changed_models = dict(daily_raws[day])
    del changed_models["groupBy"]
    when = datetime.now(UTC)
    with pytest.raises(ContractValidationError) as error:
        build_collection_bundle(
            _raw_collection(
                day=day,
                daily_models={day: changed_models},
                report=report_raw,
                graph=graph_raw,
                pricing=pricing_raw,
            ),
            run_id=str(uuid4()),
            source_id=SOURCE_ID,
            started_at=when,
            finished_at=when,
            host="pytest",
        )
    assert error.value.validation.fatal is True
    assert error.value.validation.events[0].drift_kind == "missing_field"


def test_non_nullable_required_field_rejects_null(
    daily_raws: dict[date, JsonObject],
) -> None:
    """Assert null is a type change unless the contract explicitly permits it."""
    contract = load_shipped_contracts()["models"]
    changed_models = {**daily_raws[min(daily_raws)], "totalInput": None}
    result = diff_contract(contract, changed_models, run_id=str(uuid4()))
    assert result.fatal is True
    assert any(
        event.path == "totalInput" and event.drift_kind == "type_change"
        for event in result.events
    )


def test_mixed_contract_types_are_explicit_not_silently_collapsed() -> None:
    """Assert contracts retain every observed type from array siblings."""
    payload: JsonArray = [{"value": 1}, {"value": "one"}]
    contract = build_contract("report", payload, "test-version")
    entry = next(item for item in contract.entries if item.path == "[].value")
    assert entry.expected_types == ("int", "str")
    observed: JsonArray = [{"value": True}]
    result = diff_contract(contract, observed, run_id=str(uuid4()))
    assert result.fatal is True
    assert result.events[0].drift_kind == "type_change"


def test_empty_report_is_a_successful_secondary_domain(
    daily_raws: dict[date, JsonObject],
    graph_raw: JsonObject,
    pricing_raw: JsonObject,
) -> None:
    """Accept an empty report response while recording completed coverage."""
    day = min(daily_raws)
    run_id = str(uuid4())
    when = datetime.now(UTC)

    bundle = build_collection_bundle(
        _raw_collection(
            day=day,
            daily_models={day: daily_raws[day]},
            report=[],
            graph=graph_raw,
            pricing=pricing_raw,
        ),
        run_id=run_id,
        source_id=SOURCE_ID,
        started_at=when,
        finished_at=when,
        host="pytest",
    )

    assert bundle.report_rows == []
    report_status = next(
        status for status in bundle.ingest_status if status.domain == "report"
    )
    assert (
        report_status.status,
        report_status.expected_count,
        report_status.succeeded_count,
        report_status.last_succeeded_run,
        report_status.failure_code,
    ) == ("complete", 1, 1, run_id, None)


def test_secondary_contract_failures_do_not_block_required_collection(
    daily_raws: dict[date, JsonObject],
    graph_raw: JsonObject,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Record failed report and pricing domains without discarding usage facts."""
    day = min(daily_raws)
    run_id = str(uuid4())
    when = datetime.now(UTC)

    with caplog.at_level(logging.WARNING, logger="usagebassoon"):
        bundle = build_collection_bundle(
            _raw_collection(
                day=day,
                daily_models={day: daily_raws[day]},
                report=[{}],
                graph=graph_raw,
                pricing={},
            ),
            run_id=run_id,
            source_id=SOURCE_ID,
            started_at=when,
            finished_at=when,
            host="pytest",
        )

    statuses = {status.domain: status for status in bundle.ingest_status}
    assert bundle.daily_models[day].entries
    assert bundle.report_rows == []
    assert bundle.pricing_by_day == {}
    assert (statuses["report"].status, statuses["report"].failure_code) == (
        "failed",
        "contract",
    )
    assert (statuses["pricing"].status, statuses["pricing"].failure_code) == (
        "failed",
        "contract",
    )
    assert "report contract validation failed" in caplog.text
    assert "pricing contract validation failed" in caplog.text
