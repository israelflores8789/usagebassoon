# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_reconcile.py — Tests for independent daily payload authorities."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.collector import RawCollection
from usagebassoon.diagnostics import run_doctor
from usagebassoon.ingest import (
    CollectionBundle,
    IngestEvidence,
    IngestStatus,
    build_collection_bundle,
    plan_graph,
    plan_models,
)
from usagebassoon.json_types import JsonObject
from usagebassoon.normalizer import normalize
from usagebassoon.parsers.daily import DailyModelsPayload
from usagebassoon.persistence import persist_run
from usagebassoon.reconcile import (
    ReconciliationIssue,
    ReconciliationResult,
    reconcile_all,
)


def test_daily_collection_has_no_graph_models_reconciliation(
    recon_result: ReconciliationResult,
    daily_models: dict[date, DailyModelsPayload],
) -> None:
    """Reconcile each models response without comparing graph aggregates."""
    assert recon_result == reconcile_all(daily_models)
    assert recon_result.ok


def test_model_totals_identify_assertion_across_days(
    daily_models: dict[date, DailyModelsPayload],
) -> None:
    """Report one stable issue per failed assertion across affected days."""
    day = min(daily_models)
    next_day = day + timedelta(days=1)
    original = daily_models[day]
    altered = original.totals.model_copy(
        update={"total_input": original.totals.total_input + 1}
    )
    changed = DailyModelsPayload(day, original.entries, altered)
    result = reconcile_all({day: changed, next_day: changed})
    assert result.affected_days == {day, next_day}
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert (issue.check, issue.key) == (
        "models_payload_totals",
        "total_input_mismatch",
    )
    assert next_day.isoformat() in issue.message


def test_repeated_issue_updates_one_row(
    collection_bundle: CollectionBundle,
) -> None:
    """Retain first detection and update the latest run for one assertion."""
    issue = ReconciliationIssue(
        "models_payload_totals", "total_input_mismatch", "first observation"
    )
    first = replace(
        collection_bundle,
        reconciliation=ReconciliationResult((issue,)),
    )
    second = replace(
        first,
        run_id=str(uuid4()),
        finished_at=first.finished_at + timedelta(days=1),
        reconciliation=ReconciliationResult(
            (replace(issue, message="second observation"),)
        ),
    )
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(first))
        persist_run(backend, normalize(second))
        rows = backend.query(
            "SELECT check_name, issue_key, message, created_at, updated_at, "
            "detected_run_id, updated_run_id FROM reconciliation_issues"
        ).to_pylist()
        assert rows == [
            {
                "check_name": issue.check,
                "issue_key": issue.key,
                "message": "second observation",
                "created_at": first.finished_at,
                "updated_at": second.finished_at,
                "detected_run_id": first.run_id,
                "updated_run_id": second.run_id,
            }
        ]
        report = run_doctor(
            backend,
            backend_name="duckdb",
            database=":memory:",
            snapshot_enabled=False,
        )
        check = next(item for item in report.checks if item.name == "reconciliation")
        assert check.status == "warning"
        assert "models_payload_totals/total_input_mismatch" in check.details[0]
        assert first.run_id in check.details[0]
        assert second.run_id in check.details[0]
    finally:
        backend.close()


def test_mismatch_leaves_models_day_retryable(
    daily_raws: dict[date, JsonObject],
    graph_raw: JsonObject,
) -> None:
    """Keep a mismatched daily models response eligible for another collection."""
    day = min(daily_raws)
    raw_day = daily_raws[day]
    declared = raw_day["totalInput"]
    assert isinstance(declared, int)
    changed = {**raw_day, "totalInput": declared + 1}
    now = datetime.now(UTC)
    bundle = build_collection_bundle(
        RawCollection(graph_raw, {day: changed}, {}, {day: {}}),
        run_id=str(uuid4()),
        source_id="11111111-1111-4111-8111-111111111111",
        started_at=now,
        finished_at=now,
        host=None,
    )
    assert bundle.reconciliation.issues[0].key == "total_input_mismatch"
    assert {
        (status.domain, status.status, status.failure_code)
        for status in bundle.ingest_status
    } == {
        ("models", "partial", "reconciliation"),
        ("pricing", "partial", "reconciliation"),
    }


def test_duplicate_models_key_is_recorded_without_staging_ambiguous_facts(
    daily_raws: dict[date, JsonObject],
    graph_raw: JsonObject,
) -> None:
    """Record the duplicate and retry the day without an unsafe upsert."""
    day = min(daily_raws)
    raw_day = daily_raws[day]
    entries = raw_day["entries"]
    assert isinstance(entries, list)
    changed = {**raw_day, "entries": [*entries, entries[0]]}
    now = datetime.now(UTC)
    bundle = build_collection_bundle(
        RawCollection(graph_raw, {day: changed}, {}, {}),
        run_id=str(uuid4()),
        source_id="11111111-1111-4111-8111-111111111111",
        started_at=now,
        finished_at=now,
        host=None,
    )
    assert "duplicate_session_model" in {
        issue.key for issue in bundle.reconciliation.issues
    }
    assert bundle.daily_models == {}
    assert bundle.ingest_status[0].status == "partial"


def test_successful_recheck_resolves_persisted_issue(
    collection_bundle: CollectionBundle,
) -> None:
    """Close an observed assertion without losing its first failure detail."""
    issue = ReconciliationIssue(
        "models_payload_totals", "total_input_mismatch", "bad first total"
    )
    first = replace(collection_bundle, reconciliation=ReconciliationResult((issue,)))
    second = replace(
        first,
        run_id=str(uuid4()),
        finished_at=first.finished_at + timedelta(days=1),
        reconciliation=ReconciliationResult((), resolved=((issue.check, issue.key),)),
    )
    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        persist_run(backend, normalize(first))
        persist_run(backend, normalize(second))
        row = backend.query(
            "SELECT message, detected_run_id, updated_run_id, resolved_at "
            "FROM reconciliation_issues"
        ).to_pylist()[0]
        assert row == {
            "message": issue.message,
            "detected_run_id": first.run_id,
            "updated_run_id": second.run_id,
            "resolved_at": second.finished_at,
        }
        doctor = run_doctor(
            backend,
            backend_name="duckdb",
            database=":memory:",
            snapshot_enabled=False,
        )
        check = next(item for item in doctor.checks if item.name == "reconciliation")
        assert check.status == "ok"
        reopened = replace(
            first,
            run_id=str(uuid4()),
            finished_at=second.finished_at + timedelta(days=1),
        )
        persist_run(backend, normalize(reopened))
        assert backend.query(
            "SELECT count(*) AS n FROM reconciliation_issues WHERE resolved_at IS NULL"
        ).to_pylist() == [{"n": 1}]
    finally:
        backend.close()


def test_resolution_requires_all_previous_bad_days_to_be_rechecked(
    daily_raws: dict[date, JsonObject],
    graph_raw: JsonObject,
) -> None:
    """Avoid clearing an assertion while an older failed day remains pending."""
    day = min(daily_raws)
    later = day + timedelta(days=1)
    now = datetime.now(UTC)
    run_id = str(uuid4())
    graph_plan = plan_graph(graph_raw)
    models_plan = plan_models({day: daily_raws[day]})
    identity = ("models_payload_totals", "total_input_mismatch")
    pending = IngestStatus(
        later, "models", "partial", 1, 0, "old", None, "reconciliation"
    )
    evidence = IngestEvidence(
        graph_plan=graph_plan,
        models_plan=models_plan,
        report_days=frozenset(),
        report_fetch_failures=frozenset(),
        pricing_expected_models={},
        pricing_existing_models={},
        pricing_fetch_failures={},
        prior_statuses={(later, "models"): pending},
        prior_reconciliation_issues=frozenset({identity}),
    )
    raw = RawCollection(graph_raw, {day: daily_raws[day]}, {}, {})
    bundle = build_collection_bundle(
        raw,
        run_id=run_id,
        source_id="11111111-1111-4111-8111-111111111111",
        started_at=now,
        finished_at=now,
        host=None,
        evidence=evidence,
    )
    assert bundle.reconciliation.resolved == ()

    refreshed = replace(
        evidence,
        prior_statuses={(day, "models"): replace(pending, day=day)},
    )
    cleared = build_collection_bundle(
        raw,
        run_id=run_id,
        source_id="11111111-1111-4111-8111-111111111111",
        started_at=now,
        finished_at=now,
        host=None,
        evidence=refreshed,
    )
    assert cleared.reconciliation.resolved == (identity,)
