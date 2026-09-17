# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_contracts.py — Tests validation over shipped contracts and synthetic drift."""

from __future__ import annotations

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
        RawCollection(
            daily_models={day: changed_models},
            report=report_raw,
            graph=graph_raw,
            pricing_by_day={day: {"gemini-3.8-flash": pricing_raw}},
            processed_targets=frozenset(
                {(day, "daily_stats"), (day, "price_versions")}
            ),
            failed_targets=frozenset(),
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
            RawCollection(
                daily_models={day: changed_models},
                report=report_raw,
                graph=graph_raw,
                pricing_by_day={day: {"gemini-3.8-flash": pricing_raw}},
                processed_targets=frozenset(),
                failed_targets=frozenset({(day, "daily_stats")}),
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
    models_raw: JsonObject,
) -> None:
    """Assert null is a type change unless the contract explicitly permits it."""
    contract = load_shipped_contracts()["models"]
    changed_models = {**models_raw, "totalInput": None}
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
