# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""ingest.py — Raw tokscale payload validation and collection bundle assembly."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import cast

from usagebassoon.contracts import (
    ContractDrift,
    ContractValidationError,
    PayloadContract,
    PayloadKind,
    diff_contract,
    load_shipped_contracts,
    validate_payloads,
)
from usagebassoon.json_types import JsonArray, JsonObject
from usagebassoon.logger import LOGGER_NAME
from usagebassoon.normalizer import CollectionBundle, IngestStatus, IngestTarget
from usagebassoon.parsers.daily import parse_daily
from usagebassoon.parsers.graph import parse_graph
from usagebassoon.parsers.pricing import PricingRow, parse_pricing
from usagebassoon.parsers.report import SessionRow, parse_report
from usagebassoon.reconcile import reconcile_all
from usagebassoon.system_metadata import SystemMetadata, capture_system_metadata

_LOG = logging.getLogger(LOGGER_NAME)


@dataclass(frozen=True, slots=True)
class RawCollection:
    """Raw JSON-decoded tokscale outputs for one collection run.

    Attributes:
        daily_models: Date-filtered models output keyed by requested UTC day.
        report_by_day: Successful report output keyed by requested UTC day.
        report_days: Days for which report collection was attempted.
        report_fetch_failures: Report days omitted after a command failure.
        graph: Output from tokscale graph.
        pricing_by_day: Pricing output keyed first by usage day, then request model.
        pricing_expected_models: Full model coverage required for each price day.
        pricing_existing_models: Existing persisted model-price coverage by day.
        pricing_fetch_failures: Requested price models omitted after command failure.
        prior_statuses: Prior retry-ledger state keyed by day and domain.
    """

    daily_models: Mapping[date, JsonObject]
    report_by_day: Mapping[date, JsonArray]
    report_days: frozenset[date]
    report_fetch_failures: frozenset[date]
    graph: JsonObject
    pricing_by_day: Mapping[date, Mapping[str, JsonObject]]
    pricing_expected_models: Mapping[date, frozenset[str]]
    pricing_existing_models: Mapping[date, frozenset[str]]
    pricing_fetch_failures: Mapping[date, frozenset[str]]
    prior_statuses: Mapping[IngestTarget, IngestStatus]


def _status(
    raw: RawCollection,
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
    prior = raw.prior_statuses.get((day, domain))
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
    system_metadata: SystemMetadata | None = None,
    contracts: Mapping[PayloadKind, PayloadContract] | None = None,
) -> CollectionBundle:
    """Validate raw payload contracts, then parse one collection bundle.

    Args:
        raw: Decoded tokscale command outputs.
        run_id: Owning collection run id.
        source_id: Stable namespace of the collector that observed this data.
        started_at: Collection start timestamp.
        finished_at: Collection completion timestamp.
        host: Hostname or container identifier when available.
        system_metadata: Collector-host metadata, captured when omitted.
        contracts: Explicit contracts for tests or custom deployments.

    Returns:
        Parsed collection bundle carrying non-fatal drift events.

    Raises:
        ContractValidationError: If a required contract field is absent or has
            an incompatible type.
        ValueError: If a payload does not meet its parser's top-level shape.
    """
    required_kinds: frozenset[PayloadKind] = frozenset(
        cast(PayloadKind, kind)
        for kind in (("graph", "models") if raw.daily_models else ("graph",))
    )
    validation = validate_payloads(
        {
            "models": tuple(raw.daily_models.values()),
            "graph": (raw.graph,),
        },
        run_id=run_id,
        contracts=contracts,
        detected_at=finished_at,
        required_kinds=required_kinds,
    )
    if validation.fatal:
        raise ContractValidationError(validation)
    daily_models = {
        day: parse_daily(payload, day=day) for day, payload in raw.daily_models.items()
    }
    expected_contracts = (
        contracts if contracts is not None else load_shipped_contracts()
    )
    drift_events: list[ContractDrift] = list(validation.events)

    valid_reports: dict[date, JsonArray] = {}
    report_failures: dict[date, str] = {
        day: "fetch" for day in raw.report_fetch_failures
    }
    for day, payload in raw.report_by_day.items():
        if not payload:
            valid_reports[day] = payload
            continue
        report_validation = diff_contract(
            expected_contracts["report"],
            payload,
            run_id=run_id,
            sequence_start=len(drift_events),
            detected_at=finished_at,
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
        day: "fetch" for day, models in raw.pricing_fetch_failures.items() if models
    }
    for day, prices in raw.pricing_by_day.items():
        parsed_prices: dict[str, PricingRow] = {}
        for model, payload in prices.items():
            pricing_validation = diff_contract(
                expected_contracts["pricing"],
                payload,
                run_id=run_id,
                sequence_start=len(drift_events),
                detected_at=finished_at,
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

    statuses = [
        _status(
            raw,
            day=day,
            domain="models",
            status="complete",
            expected_count=1,
            succeeded_count=1,
            run_id=run_id,
        )
        for day in sorted(daily_models)
    ]
    statuses.extend(
        _status(
            raw,
            day=day,
            domain="report",
            status="failed" if day in report_failures else "complete",
            expected_count=1,
            succeeded_count=0 if day in report_failures else 1,
            run_id=run_id,
            failure_code=report_failures.get(day),
        )
        for day in sorted(raw.report_days)
    )
    for day in sorted(raw.pricing_expected_models):
        expected_models = raw.pricing_expected_models[day]
        covered_models = set(raw.pricing_existing_models.get(day, frozenset()))
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
        statuses.append(
            _status(
                raw,
                day=day,
                domain="pricing",
                status=status,
                expected_count=len(expected_models),
                succeeded_count=succeeded_count,
                run_id=run_id,
                failure_code=failure_code,
            )
        )
    graph = parse_graph(raw.graph)
    return CollectionBundle(
        run_id=run_id,
        source_id=source_id,
        started_at=started_at,
        finished_at=finished_at,
        host=host,
        daily_models=daily_models,
        report_rows=report_rows,
        graph=graph,
        pricing_by_day=pricing_by_day,
        ingest_status=tuple(statuses),
        reconciliation=reconcile_all(),
        contract_drift=tuple(drift_events),
        fetch_summary={
            "rows_in": sum(len(payload.entries) for payload in daily_models.values())
            + len(report_rows)
            + len(graph.contributions)
        },
        system_metadata=system_metadata or capture_system_metadata(),
    )
