# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""ingest.py — Raw tokscale payload validation and collection bundle assembly."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime

from usagebassoon.contracts import (
    ContractValidation,
    ContractValidationError,
    PayloadContract,
    PayloadKind,
    validate_payloads,
)
from usagebassoon.json_types import JsonArray, JsonObject
from usagebassoon.normalizer import CollectionBundle, ProcessingTarget
from usagebassoon.parsers.daily import parse_daily
from usagebassoon.parsers.graph import parse_graph
from usagebassoon.parsers.pricing import parse_pricing
from usagebassoon.parsers.report import parse_report
from usagebassoon.reconcile import reconcile_all
from usagebassoon.system_metadata import SystemMetadata, capture_system_metadata


@dataclass(frozen=True, slots=True)
class RawCollection:
    """Raw JSON-decoded tokscale outputs for one collection run.

    Attributes:
        daily_models: Date-filtered models output keyed by requested UTC day.
        report: Output from tokscale report without summarization.
        graph: Output from tokscale graph.
        pricing_by_day: Pricing output keyed first by usage day, then request model.
        processed_targets: Targets whose data was fetched successfully.
        failed_targets: Targets omitted because a request failed.
    """

    daily_models: Mapping[date, JsonObject]
    report: JsonArray
    graph: JsonObject
    pricing_by_day: Mapping[date, Mapping[str, JsonObject]]
    processed_targets: frozenset[ProcessingTarget]
    failed_targets: frozenset[ProcessingTarget]


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
    validation: ContractValidation = validate_payloads(
        {
            "models": tuple(raw.daily_models.values()),
            "report": (raw.report,),
            "graph": (raw.graph,),
            "pricing": tuple(
                payload
                for pricing in raw.pricing_by_day.values()
                for payload in pricing.values()
            ),
        },
        run_id=run_id,
        contracts=contracts,
        detected_at=finished_at,
    )
    if validation.fatal:
        raise ContractValidationError(validation)
    daily_models = {
        day: parse_daily(payload, day=day) for day, payload in raw.daily_models.items()
    }
    pricing_by_day = {
        day: {
            pricing.model_id: pricing
            for pricing in (parse_pricing(payload) for payload in raw_pricing.values())
        }
        for day, raw_pricing in raw.pricing_by_day.items()
    }
    report_rows = parse_report(raw.report)
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
        processed_targets=raw.processed_targets,
        failed_targets=raw.failed_targets,
        reconciliation=reconcile_all(),
        contract_drift=validation.events,
        fetch_summary={
            "rows_in": sum(len(payload.entries) for payload in daily_models.values())
            + len(report_rows)
            + len(graph.contributions)
        },
        system_metadata=system_metadata or capture_system_metadata(),
    )
