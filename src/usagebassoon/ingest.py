# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""ingest.py — Payload validation, collection assembly, and ingest records."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import TYPE_CHECKING

from usagebassoon.contracts import (
    ContractDrift,
    ContractValidationError,
    PayloadContract,
    PayloadKind,
    diff_contract,
    load_shipped_contracts,
    validate_payloads,
)
from usagebassoon.drift import SchemaDriftState, drift_identity
from usagebassoon.json_types import JsonArray, JsonObject
from usagebassoon.logger import LOGGER_NAME
from usagebassoon.parsers.daily import DailyModelsPayload, parse_daily
from usagebassoon.parsers.graph import GraphPayload, parse_graph
from usagebassoon.parsers.pricing import PricingRow, parse_pricing
from usagebassoon.parsers.report import SessionRow, parse_report
from usagebassoon.reconcile import (
    ReconciliationIdentity,
    ReconciliationResult,
    reconcile_all,
)
from usagebassoon.system_metadata import SystemMetadata, capture_system_metadata

if TYPE_CHECKING:
    from usagebassoon.collector import RawCollection

_LOG = logging.getLogger(LOGGER_NAME)


type IngestTarget = tuple[date, str]


@dataclass(frozen=True, slots=True)
class IngestStatus:
    """One domain-level daily collection status used by retry planning."""

    day: date
    domain: str
    status: str
    expected_count: int | None
    succeeded_count: int | None
    last_attempted_run: str
    last_succeeded_run: str | None
    failure_code: str | None = None


@dataclass(frozen=True, slots=True)
class IngestIssue:
    """A collection run whose persisted status needs attention.

    Attributes:
        run_id: Collection run identifier.
        finished_at: Collection completion time, when available.
        status: Persisted collection status.
        drift_events: Number of drift events recorded for the run.
    """

    run_id: str
    finished_at: datetime | None
    status: str | None
    drift_events: int | None


@dataclass(frozen=True, slots=True)
class GraphPlan:
    """Validated graph data used to select the collection's candidate days."""

    graph: GraphPayload
    candidate_days: tuple[date, ...]
    contract_drift: tuple[ContractDrift, ...]


@dataclass(frozen=True, slots=True)
class ModelsPlan:
    """Validated daily model payloads and the model identifiers they require."""

    daily_models: dict[date, DailyModelsPayload]
    models_by_day: dict[date, frozenset[str]]
    contract_drift: tuple[ContractDrift, ...]


@dataclass(frozen=True, slots=True)
class IngestEvidence:
    """Acquisition outcomes required to complete one ingest decision."""

    graph_plan: GraphPlan
    models_plan: ModelsPlan
    report_days: frozenset[date]
    report_fetch_failures: frozenset[date]
    pricing_expected_models: Mapping[date, frozenset[str]]
    pricing_existing_models: Mapping[date, frozenset[str]]
    pricing_fetch_failures: Mapping[date, frozenset[str]]
    prior_statuses: Mapping[IngestTarget, IngestStatus]
    prior_reconciliation_issues: frozenset[ReconciliationIdentity] = frozenset()
    prior_schema_drift: tuple[SchemaDriftState, ...] = ()


@dataclass(frozen=True, slots=True)
class CollectionBundle:
    """Parsed, contract-validated data from one completed collection cycle."""

    run_id: str
    source_id: str
    started_at: datetime
    finished_at: datetime
    host: str | None
    daily_models: dict[date, DailyModelsPayload]
    report_rows: list[SessionRow]
    graph: GraphPayload
    pricing_by_day: dict[date, dict[str, PricingRow]]
    ingest_status: tuple[IngestStatus, ...]
    reconciliation: ReconciliationResult
    contract_drift: tuple[ContractDrift, ...] = ()
    resolved_schema_drift: tuple[SchemaDriftState, ...] = ()
    fetch_summary: dict[str, int] | None = None
    drift_fatal: bool = False
    system_metadata: SystemMetadata | None = None


def plan_graph(
    payload: JsonObject,
    *,
    contracts: Mapping[PayloadKind, PayloadContract] | None = None,
) -> GraphPlan:
    """Validate and parse graph before collector planning consumes it."""
    validation = validate_payloads(
        {"graph": (payload,)},
        contracts=contracts,
        required_kinds=frozenset({"graph"}),
    )
    if validation.fatal:
        raise ContractValidationError(validation)
    graph = parse_graph(payload)
    return GraphPlan(
        graph=graph,
        candidate_days=tuple(sorted({item.date for item in graph.contributions})),
        contract_drift=validation.events,
    )


def plan_models(
    payloads: Mapping[date, JsonObject],
    *,
    contracts: Mapping[PayloadKind, PayloadContract] | None = None,
) -> ModelsPlan:
    """Validate and parse daily models before pricing planning consumes them."""
    validation = validate_payloads(
        {"models": tuple(payloads.values())},
        contracts=contracts,
        required_kinds=frozenset({"models"}) if payloads else frozenset(),
    )
    if validation.fatal:
        raise ContractValidationError(validation)
    daily_models = {
        day: parse_daily(payload, day=day) for day, payload in payloads.items()
    }
    return ModelsPlan(
        daily_models=daily_models,
        models_by_day={
            day: frozenset(row.stats.model for row in payload.entries)
            for day, payload in daily_models.items()
        },
        contract_drift=validation.events,
    )


def build_ingest_evidence(
    *,
    graph_plan: GraphPlan,
    models_plan: ModelsPlan,
    pricing_days: frozenset[date],
    persisted_models: Mapping[date, set[str]],
    persisted_prices: Mapping[date, set[str]],
    report_fetch_failures: frozenset[date],
    pricing_fetch_failures: Mapping[date, frozenset[str]],
    prior_statuses: Mapping[IngestTarget, IngestStatus],
    prior_reconciliation_issues: frozenset[ReconciliationIdentity] = frozenset(),
    prior_schema_drift: tuple[SchemaDriftState, ...] = (),
) -> IngestEvidence:
    """Construct ingest evidence from validated plans and acquisition outcomes."""
    expected_models = {
        day: models_plan.models_by_day.get(
            day, frozenset(persisted_models.get(day, set()))
        )
        for day in pricing_days
    }
    return IngestEvidence(
        graph_plan=graph_plan,
        models_plan=models_plan,
        report_days=frozenset(graph_plan.candidate_days),
        report_fetch_failures=report_fetch_failures,
        pricing_expected_models=expected_models,
        pricing_existing_models={
            day: frozenset(persisted_prices.get(day, set())) for day in pricing_days
        },
        pricing_fetch_failures=pricing_fetch_failures,
        prior_statuses=prior_statuses,
        prior_reconciliation_issues=prior_reconciliation_issues,
        prior_schema_drift=prior_schema_drift,
    )


def _status(
    prior_statuses: Mapping[IngestTarget, IngestStatus],
    *,
    day: date,
    domain: str,
    status: str,
    expected_count: int | None,
    succeeded_count: int | None,
    run_id: str,
    failure_code: str | None = None,
) -> IngestStatus:
    """Build one retry-ledger row while retaining prior full success linkage."""
    prior = prior_statuses.get((day, domain))
    return IngestStatus(
        day=day,
        domain=domain,
        status=status,
        expected_count=expected_count,
        succeeded_count=succeeded_count,
        last_attempted_run=run_id,
        last_succeeded_run=(
            run_id
            if status == "complete"
            else prior.last_succeeded_run
            if prior is not None
            else None
        ),
        failure_code=failure_code,
    )


def build_collection_bundle(
    raw: RawCollection,
    *,
    run_id: str,
    source_id: str,
    started_at: datetime,
    finished_at: datetime,
    host: str | None,
    evidence: IngestEvidence | None = None,
    system_metadata: SystemMetadata | None = None,
    contracts: Mapping[PayloadKind, PayloadContract] | None = None,
) -> CollectionBundle:
    """Validate raw payload contracts, then parse one collection bundle.

    Args:
        raw: Decoded tokscale command outputs.
        graph_plan: Contract-validated graph data used for day planning.
        models_plan: Contract-validated daily models data used for pricing.
        report_days: Days for which report collection was attempted.
        report_fetch_failures: Report days omitted after a command failure.
        pricing_expected_models: Full model coverage expected by usage day.
        pricing_existing_models: Persisted model-price coverage by usage day.
        pricing_fetch_failures: Requested price models omitted after failure.
        prior_statuses: Persisted retry-ledger status by day and domain.
        run_id: Owning collection run id.
        source_id: Stable namespace of the collector that observed this data.
        started_at: Collection start timestamp.
        finished_at: Collection completion timestamp.
        host: Hostname or container identifier when available.
        evidence: Validated acquisition outcomes for this ingest decision.
        system_metadata: Collector-host metadata, captured when omitted.
        contracts: Explicit contracts for tests or custom deployments.

    Returns:
        Parsed collection bundle carrying non-fatal drift events.

    Raises:
        ContractValidationError: If a required contract field is absent or has
            an incompatible type.
        ValueError: If a payload does not meet its parser's top-level shape.
    """
    graph_plan = (
        evidence.graph_plan
        if evidence is not None
        else plan_graph(raw.graph, contracts=contracts)
    )
    models_plan = (
        evidence.models_plan
        if evidence is not None
        else plan_models(
            raw.daily_models,
            contracts=contracts,
        )
    )
    if evidence is None:
        evidence = IngestEvidence(
            graph_plan=graph_plan,
            models_plan=models_plan,
            report_days=frozenset(raw.report_by_day),
            report_fetch_failures=frozenset(),
            pricing_expected_models={
                day: frozenset(prices) for day, prices in raw.pricing_by_day.items()
            },
            pricing_existing_models={},
            pricing_fetch_failures={},
            prior_statuses={},
        )
    expected_contracts = (
        contracts if contracts is not None else load_shipped_contracts()
    )
    daily_models = models_plan.daily_models
    drift_events: list[ContractDrift] = [
        *graph_plan.contract_drift,
        *models_plan.contract_drift,
    ]

    valid_reports: dict[date, JsonArray] = {}
    report_failures: dict[date, str] = {
        day: "fetch" for day in evidence.report_fetch_failures
    }
    for day, payload in raw.report_by_day.items():
        if not payload:
            valid_reports[day] = payload
            continue
        report_validation = diff_contract(
            expected_contracts["report"],
            payload,
        )
        drift_events.extend(report_validation.events)
        if report_validation.fatal:
            report_failures[day] = "contract"
            _LOG.warning(
                "report contract validation failed for %s: %s",
                day.isoformat(),
                "; ".join(
                    f"{event.path}: {event.detail}"
                    for event in report_validation.events
                ),
            )
        else:
            valid_reports[day] = payload
    combined_reports: JsonArray = [
        row for day in sorted(valid_reports) for row in valid_reports[day]
    ]
    try:
        report_rows = parse_report(combined_reports)
    except ValueError:
        report_rows: list[SessionRow] = []
        for day in valid_reports:
            report_failures[day] = "parse"
        _LOG.exception(
            "report parsing failed for %s",
            ", ".join(day.isoformat() for day in sorted(valid_reports)),
        )

    pricing_by_day: dict[date, dict[str, PricingRow]] = {}
    pricing_failures: dict[date, str] = {
        day: "fetch"
        for day, models in evidence.pricing_fetch_failures.items()
        if models
    }
    for day, prices in raw.pricing_by_day.items():
        parsed_prices: dict[str, PricingRow] = {}
        for model, payload in prices.items():
            pricing_validation = diff_contract(
                expected_contracts["pricing"],
                payload,
            )
            drift_events.extend(pricing_validation.events)
            if pricing_validation.fatal:
                pricing_failures[day] = "contract"
                _LOG.warning(
                    "pricing contract validation failed for %s on %s: %s",
                    day.isoformat(),
                    model,
                    "; ".join(
                        f"{event.path}: {event.detail}"
                        for event in pricing_validation.events
                    ),
                )
                continue
            try:
                parsed_prices[model] = parse_pricing(payload)
            except ValueError:
                pricing_failures[day] = "parse"
                _LOG.exception(
                    "pricing parsing failed for %s on %s", day.isoformat(), model
                )
        if parsed_prices:
            pricing_by_day[day] = parsed_prices

    reconciliation = reconcile_all(daily_models)
    statuses = [
        _status(
            evidence.prior_statuses,
            day=day,
            domain="models",
            status="partial" if day in reconciliation.affected_days else "complete",
            expected_count=1,
            succeeded_count=0 if day in reconciliation.affected_days else 1,
            run_id=run_id,
            failure_code=(
                "reconciliation" if day in reconciliation.affected_days else None
            ),
        )
        for day in sorted(daily_models)
    ]
    current_models_status = {status.day: status for status in statuses}
    pending_prior_days = {
        day
        for (day, domain), prior in evidence.prior_statuses.items()
        if domain == "models"
        and prior.failure_code == "reconciliation"
        and current_models_status.get(day, prior).status != "complete"
    }
    if daily_models and not pending_prior_days:
        active = {(issue.check, issue.key) for issue in reconciliation.issues}
        resolved = tuple(
            sorted(
                identity
                for identity in evidence.prior_reconciliation_issues - active
                if identity[0] in {"models_payload_totals", "models_payload_keys"}
            )
        )
        reconciliation = replace(reconciliation, resolved=resolved)
    statuses.extend(
        _status(
            evidence.prior_statuses,
            day=day,
            domain="report",
            status="failed" if day in report_failures else "complete",
            expected_count=1,
            succeeded_count=0 if day in report_failures else 1,
            run_id=run_id,
            failure_code=report_failures.get(day),
        )
        for day in sorted(evidence.report_days)
    )
    for day in sorted(evidence.pricing_expected_models):
        expected_models = evidence.pricing_expected_models[day]
        covered_models = set(evidence.pricing_existing_models.get(day, frozenset()))
        covered_models.update(pricing_by_day.get(day, {}))
        succeeded_count = len(expected_models & covered_models)
        failure_code = pricing_failures.get(day)
        if succeeded_count == len(expected_models) and failure_code is None:
            status = "complete"
        elif succeeded_count:
            status = "partial"
            failure_code = failure_code or "incomplete"
        else:
            status = "failed"
            failure_code = failure_code or "incomplete"
        if day in reconciliation.affected_days:
            status = "partial"
            failure_code = "reconciliation"
        statuses.append(
            _status(
                evidence.prior_statuses,
                day=day,
                domain="pricing",
                status=status,
                expected_count=len(expected_models),
                succeeded_count=succeeded_count,
                run_id=run_id,
                failure_code=failure_code,
            )
        )
    complete_domains = {"graph"}
    if daily_models:
        complete_domains.add("models")
    if (
        evidence.report_days
        and not report_failures
        and evidence.report_days <= valid_reports.keys()
    ):
        complete_domains.add("report")
    expected_price_count = sum(
        len(models) for models in evidence.pricing_expected_models.values()
    )
    if (
        evidence.pricing_expected_models
        and expected_price_count > 0
        and not pricing_failures
        and all(
            models <= raw.pricing_by_day.get(day, {}).keys()
            for day, models in evidence.pricing_expected_models.items()
        )
    ):
        complete_domains.add("pricing")
    tokscale_ver = graph_plan.graph.meta.version
    active_drift = {
        (event.domain, tokscale_ver, event.drift_key) for event in drift_events
    }
    resolved_schema_drift = tuple(
        state
        for state in evidence.prior_schema_drift
        if state.tokscale_ver == tokscale_ver
        and state.domain in complete_domains
        and drift_identity(state) not in active_drift
    )
    return CollectionBundle(
        run_id=run_id,
        source_id=source_id,
        started_at=started_at,
        finished_at=finished_at,
        host=host,
        daily_models={
            day: payload
            for day, payload in daily_models.items()
            if day not in reconciliation.unsafe_days
        },
        report_rows=report_rows,
        graph=graph_plan.graph,
        pricing_by_day=pricing_by_day,
        ingest_status=tuple(statuses),
        reconciliation=reconciliation,
        contract_drift=tuple(drift_events),
        resolved_schema_drift=resolved_schema_drift,
        fetch_summary={
            "rows_in": sum(len(payload.entries) for payload in daily_models.values())
            + len(report_rows)
            + len(graph_plan.graph.contributions)
        },
        system_metadata=system_metadata or capture_system_metadata(),
    )
