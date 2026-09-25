# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_contracts.py — Tests validation over shipped contracts and synthetic drift."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.collector import RawCollection
from usagebassoon.contracts import (
    ContractValidationError,
    PayloadContract,
    build_contract,
    diff_contract,
    load_shipped_contracts,
    validate_payloads,
)
from usagebassoon.drift import SchemaDriftState
from usagebassoon.ingest import (
    IngestEvidence,
    build_collection_bundle,
    plan_graph,
    plan_models,
)
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.normalizer import normalize
from usagebassoon.persistence import persist_run

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
        graph=graph,
        pricing_by_day={day: {"gemini-3.8-flash": pricing}},
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
    )
    assert result == type(result)(events=(), fatal=False)


def test_unused_graph_fields_are_optional_in_the_shipped_contract() -> None:
    """Graph aggregates and capture time are not ingestion requirements."""
    graph_contract = load_shipped_contracts()["graph"]
    optional_paths = frozenset(
        {
            "meta.generatedAt",
            "summary",
            "summary.activeDays",
            "summary.averagePerDay",
            "summary.clients",
            "summary.maxCostInSingleDay",
            "summary.models",
            "summary.totalCost",
            "summary.totalDays",
            "summary.totalTokens",
            "timeMetrics",
            "timeMetrics.longestContinuousMs",
            "timeMetrics.maxConcurrentSessions",
            "timeMetrics.sessionCount",
            "timeMetrics.totalActiveTimeMs",
        }
    )
    entries = {entry.path: entry for entry in graph_contract.entries}
    assert optional_paths <= frozenset(entries)
    optional_entries = tuple(entries[path] for path in sorted(optional_paths))
    assert all(not entry.required for entry in optional_entries)

    focused_contract = PayloadContract(
        payload_kind=graph_contract.payload_kind,
        tokscale_version=graph_contract.tokscale_version,
        entries=optional_entries,
    )
    validation = diff_contract(focused_contract, {})

    assert validation.events == ()
    assert validation.fatal is False


def test_models_performance_can_be_absent_without_losing_token_facts() -> None:
    """Keep timing optional while monitoring present performance field types."""
    models_contract = load_shipped_contracts()["models"]
    entries = {entry.path: entry for entry in models_contract.entries}
    performance = tuple(
        entry
        for path, entry in entries.items()
        if path == "entries[].performance" or path.startswith("entries[].performance.")
    )
    assert len(performance) == 6
    assert all(not entry.required for entry in performance)
    focused = PayloadContract(
        payload_kind=models_contract.payload_kind,
        tokscale_version=models_contract.tokscale_version,
        entries=performance,
    )
    assert diff_contract(focused, {}).events == ()


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
    graph_meta = graph_raw["meta"]
    assert isinstance(graph_meta, dict)
    observed_version = graph_meta["version"]
    assert isinstance(observed_version, str)
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(bundle))
        assert backend.query(
            "SELECT domain, tokscale_ver, drift_key, drift_kind, path, "
            "contract_tokscale_ver, resolved, observation_count "
            "FROM schema_drift_events"
        ).to_pylist() == [
            {
                "domain": "models",
                "tokscale_ver": observed_version,
                "drift_key": "unknown_field:futureMetric",
                "drift_kind": "unknown_field",
                "path": "futureMetric",
                "contract_tokscale_ver": load_shipped_contracts()[
                    "models"
                ].tokscale_version,
                "resolved": False,
                "observation_count": 1,
            }
        ]
    finally:
        backend.close()


def test_repeated_drift_observations_upsert_and_are_version_scoped(
    daily_raws: dict[date, JsonObject],
    report_raw: JsonArray,
    graph_raw: JsonObject,
    pricing_raw: JsonObject,
) -> None:
    """Keep one event per identity and count observations within each version."""
    day = min(daily_raws)
    changed_models = {**daily_raws[day], "futureMetric": 1}
    now = datetime.now(UTC)
    graph_meta = graph_raw["meta"]
    assert isinstance(graph_meta, dict)
    original_version = graph_meta["version"]
    assert isinstance(original_version, str)
    next_version = "4.15.2"
    next_meta = {**graph_meta, "version": next_version}
    next_graph = {**graph_raw, "meta": next_meta}
    bundles = tuple(
        build_collection_bundle(
            _raw_collection(
                day=day,
                daily_models={day: changed_models},
                report=report_raw,
                graph=graph,
                pricing=pricing_raw,
            ),
            run_id=str(uuid4()),
            source_id=SOURCE_ID,
            started_at=now + offset,
            finished_at=now + offset,
            host="pytest",
        )
        for graph, offset in (
            (graph_raw, timedelta(seconds=0)),
            (graph_raw, timedelta(seconds=1)),
            (next_graph, timedelta(seconds=2)),
        )
    )
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        for bundle in bundles:
            persist_run(backend, normalize(bundle))
        persist_run(backend, normalize(bundles[1]))
        rows = backend.query(
            "SELECT domain, tokscale_ver, drift_key, contract_tokscale_ver, "
            "created_at, updated_at, detected_run_id, updated_run_id, resolved, "
            "observation_count FROM schema_drift_events "
            "ORDER BY tokscale_ver"
        ).to_pylist()
        first, second, third = bundles
        assert rows == [
            {
                "domain": "models",
                "tokscale_ver": original_version,
                "drift_key": "unknown_field:futureMetric",
                "contract_tokscale_ver": load_shipped_contracts()[
                    "models"
                ].tokscale_version,
                "created_at": first.finished_at,
                "updated_at": second.finished_at,
                "detected_run_id": first.run_id,
                "updated_run_id": second.run_id,
                "resolved": False,
                "observation_count": 2,
            },
            {
                "domain": "models",
                "tokscale_ver": next_version,
                "drift_key": "unknown_field:futureMetric",
                "contract_tokscale_ver": load_shipped_contracts()[
                    "models"
                ].tokscale_version,
                "created_at": third.finished_at,
                "updated_at": third.finished_at,
                "detected_run_id": third.run_id,
                "updated_run_id": third.run_id,
                "resolved": False,
                "observation_count": 1,
            },
        ]
    finally:
        backend.close()


def test_same_run_payload_sightings_are_counted_in_one_event(
    daily_raws: dict[date, JsonObject], graph_raw: JsonObject
) -> None:
    """Aggregate the same deviation across command payloads in one collection."""
    changed_models = {
        day: {**payload, "futureMetric": 1} for day, payload in daily_raws.items()
    }
    now = datetime.now(UTC)
    raw = RawCollection(graph_raw, changed_models, {}, {})
    bundle = build_collection_bundle(
        raw,
        run_id=str(uuid4()),
        source_id=SOURCE_ID,
        started_at=now,
        finished_at=now,
        host="pytest",
    )
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(bundle))
        rows = backend.query(
            "SELECT count(*) AS event_count, min(observation_count) AS observations "
            "FROM schema_drift_events"
        ).to_pylist()
        assert rows == [{"event_count": 1, "observations": len(daily_raws)}]
    finally:
        backend.close()


def test_clean_complete_domain_resolves_existing_event(
    daily_raws: dict[date, JsonObject],
    report_raw: JsonArray,
    graph_raw: JsonObject,
    pricing_raw: JsonObject,
) -> None:
    """Close a rechecked event while preserving its first detection metadata."""
    day = min(daily_raws)
    when = datetime.now(UTC)
    first = build_collection_bundle(
        _raw_collection(
            day=day,
            daily_models={day: {**daily_raws[day], "futureMetric": 1}},
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
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(first))
        persisted = backend.query(
            "SELECT domain, tokscale_ver, drift_key, drift_kind, path, detail, "
            "contract_tokscale_ver, created_at, detected_run_id, observation_count "
            "FROM schema_drift_events"
        ).to_pylist()[0]
        state = SchemaDriftState(
            domain=persisted["domain"],
            tokscale_ver=persisted["tokscale_ver"],
            drift_key=persisted["drift_key"],
            drift_kind=persisted["drift_kind"],
            path=persisted["path"],
            detail=persisted["detail"],
            contract_tokscale_ver=persisted["contract_tokscale_ver"],
            created_at=persisted["created_at"],
            detected_run_id=persisted["detected_run_id"],
            observation_count=persisted["observation_count"],
        )
        next_time = when + timedelta(seconds=1)
        graph_plan = plan_graph(graph_raw)
        models_plan = plan_models({day: daily_raws[day]})
        evidence = IngestEvidence(
            graph_plan=graph_plan,
            models_plan=models_plan,
            report_days=frozenset(),
            report_fetch_failures=frozenset(),
            pricing_expected_models={},
            pricing_existing_models={},
            pricing_fetch_failures={},
            prior_statuses={},
            prior_schema_drift=(state,),
        )
        graph_meta = graph_raw["meta"]
        assert isinstance(graph_meta, dict)
        versioned_graph = {
            **graph_raw,
            "meta": {**graph_meta, "version": "4.15.2"},
        }
        versioned_evidence = IngestEvidence(
            graph_plan=plan_graph(versioned_graph),
            models_plan=models_plan,
            report_days=frozenset(),
            report_fetch_failures=frozenset(),
            pricing_expected_models={},
            pricing_existing_models={},
            pricing_fetch_failures={},
            prior_statuses={},
            prior_schema_drift=(state,),
        )
        version_changed = build_collection_bundle(
            RawCollection(versioned_graph, {day: daily_raws[day]}, {}, {}),
            run_id=str(uuid4()),
            source_id=SOURCE_ID,
            started_at=next_time,
            finished_at=next_time,
            host="pytest",
            evidence=versioned_evidence,
        )
        assert version_changed.resolved_schema_drift == ()
        second = build_collection_bundle(
            RawCollection(graph_raw, {day: daily_raws[day]}, {}, {}),
            run_id=str(uuid4()),
            source_id=SOURCE_ID,
            started_at=next_time,
            finished_at=next_time,
            host="pytest",
            evidence=evidence,
        )
        assert second.resolved_schema_drift == (state,)
        persist_run(backend, normalize(second))
        row = backend.query(
            "SELECT created_at, updated_at, detected_run_id, updated_run_id, "
            "resolved, observation_count FROM schema_drift_events"
        ).to_pylist()[0]
        assert row == {
            "created_at": first.finished_at,
            "updated_at": second.finished_at,
            "detected_run_id": first.run_id,
            "updated_run_id": second.run_id,
            "resolved": True,
            "observation_count": 1,
        }
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
    result = diff_contract(contract, changed_models)
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
    result = diff_contract(contract, observed)
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
