# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""ingest.py — Raw tokscale payload validation and parsing for one collection run."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from usagebassoon.contracts import (
    ContractValidation,
    ContractValidationError,
    PayloadContract,
    PayloadKind,
    validate_payloads,
)
from usagebassoon.json_types import JsonArray, JsonObject
from usagebassoon.normalizer import CollectionBundle
from usagebassoon.parsers.graph import parse_graph
from usagebassoon.parsers.models import parse_models
from usagebassoon.parsers.pricing import parse_pricing
from usagebassoon.parsers.report import parse_report
from usagebassoon.reconcile import reconcile_all


@dataclass(frozen=True, slots=True)
class RawCollection:
    """Raw JSON-decoded tokscale outputs for one collection run.

    Attributes:
        models: Output from the grouped tokscale models command.
        report: Output from tokscale report without summarization.
        graph: Output from tokscale graph.
        pricing: Pricing output keyed by requested model identifier.
    """

    models: JsonObject
    report: JsonArray
    graph: JsonObject
    pricing: Mapping[str, JsonObject]


def build_collection_bundle(
    raw: RawCollection,
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    host: str | None,
    contracts: Mapping[PayloadKind, PayloadContract] | None = None,
) -> CollectionBundle:
    """Validate raw payload contracts, then parse one collection bundle.

    Args:
        raw: Decoded tokscale command outputs.
        run_id: Owning collection run id.
        started_at: Collection start timestamp.
        finished_at: Collection completion timestamp.
        host: Hostname or container identifier when available.
        contracts: Explicit contracts for tests or custom deployments.

    Returns:
        Parsed collection bundle carrying non-fatal drift events.

    Raises:
        ContractValidationError: If a required contract field is absent or
            has an incompatible type.
        ValueError: If a payload does not meet its parser's top-level shape.
    """
    validation: ContractValidation = validate_payloads(
        {
            "models": (raw.models,),
            "report": (raw.report,),
            "graph": (raw.graph,),
            "pricing": tuple(raw.pricing.values()),
        },
        run_id=run_id,
        contracts=contracts,
        detected_at=finished_at,
    )
    if validation.fatal:
        raise ContractValidationError(validation)

    models = parse_models(raw.models)
    report_rows = parse_report(raw.report)
    graph = parse_graph(raw.graph)
    pricing_rows = [parse_pricing(payload) for payload in raw.pricing.values()]
    pricing_by_model = {row.model_id: row for row in pricing_rows}
    return CollectionBundle(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        host=host,
        models=models,
        report_rows=report_rows,
        graph=graph,
        pricing_by_model=pricing_by_model,
        reconciliation=reconcile_all(models, report_rows, graph),
        contract_drift=validation.events,
        fetch_summary={
            "rows_in": len(models.entries)
            + len(report_rows)
            + sum(len(contribution.clients) for contribution in graph.contributions)
        },
    )
