# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""normalizer.py — Validated pydantic payloads -> canonical columnar tables.

Derived columns are computed here, before any backend sees the data, so
dialect differences in computed-column DDL never leak into data:
total_tokens (BigQuery has no GENERATED columns), session_label, and the
per-row pricing stamps. Both backends receive identical payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa

from usagebassoon.contracts import ContractDrift
from usagebassoon.parsers.graph import GraphPayload
from usagebassoon.parsers.models import ModelsPayload, ModelStatsRow
from usagebassoon.parsers.pricing import PricingRow
from usagebassoon.parsers.report import SessionRow, make_session_label
from usagebassoon.reconcile import ReconciliationResult

type ColumnarData = dict[str, list[object | None]]


@dataclass(frozen=True, slots=True)
class CollectionBundle:
    """All data produced by one successful collector invocation.

    Attributes:
        run_id: Identifier for this collection run.
        started_at: When the collector began invoking tokscale.
        finished_at: When parsing finished; merged-data timestamp.
        host: Hostname or container id, if known.
        models: Validated models payload (metrics authority).
        report_rows: Validated report rows (metadata authority).
        graph: Validated graph payload (daily dimension + telemetry).
        pricing_by_model: Resolved rate cards keyed by model id.
        reconciliation: Cross-payload consistency results.
        contract_drift: Schema contract deviations observed this run.
        fetch_summary: Rows-in counts per payload for audit.
        drift_fatal: True when a required field was missing or mistyped.
    """

    run_id: str
    started_at: datetime
    finished_at: datetime
    host: str | None
    models: ModelsPayload
    report_rows: list[SessionRow]
    graph: GraphPayload
    pricing_by_model: dict[str, PricingRow]
    reconciliation: ReconciliationResult
    contract_drift: tuple[ContractDrift, ...] = ()
    fetch_summary: dict[str, int] | None = None
    drift_fatal: bool = False


@dataclass(frozen=True, slots=True)
class NormalizedBundle:
    """Canonical Arrow tables keyed by table name, run-stamped."""

    run_id: str
    tables: dict[str, pa.Table]


def _col_major[T](recs: list[dict[str, T]]) -> ColumnarData:
    """Flip a uniformly-keyed record list into a column dict.

    Args:
        recs: Non-empty list of row dicts.

    Returns:
        Column-major mapping suitable for pa.Table.from_pydict.
    """
    columns: ColumnarData = {key: [] for key in recs[0]}
    for record in recs:
        for key, value in record.items():
            columns[key].append(value)
    return columns


def _session_rows(rows: list[SessionRow], at: datetime) -> ColumnarData:
    """Build the session dimension (with derived labels) for one run.

    Args:
        rows: Validated report rows (metadata authority).
        at: This run's collection timestamp.

    Returns:
        Column-major dict.
    """
    recs = [
        {
            "client": r.client,
            "session_id": r.session_id,
            "workspace": r.workspace,
            "workspace_label": r.workspace_label,
            "created_at": r.created_at,
            "last_active": r.last_active,
            "duration_minutes": r.duration_minutes,
            "message_count": r.message_count,
            "cost_usd": r.cost_usd,
            "models_used": sorted(r.models_used),
            "session_label": make_session_label(r),
            "first_seen_at": at,
            "last_seen_at": r.last_active or at,
            "last_updated_at": at,
        }
        for r in rows
    ]
    return _col_major(recs) if recs else {}


def _stats_rows(
    entries: list[ModelStatsRow],
    pricing_by_model: dict[str, PricingRow],
    session_last_seen: dict[tuple[str, str], datetime],
    at: datetime,
) -> ColumnarData:
    """Build per-(client,session,model) rows with pricing stamps.

    total_tokens is computed here (was a GENERATED column in v3): BigQuery
    has no generated columns and both backends must receive identical data.

    Args:
        entries: Validated cumulative entries.
        pricing_by_model: Rate cards captured this run.
        session_last_seen: Latest report activity timestamp per session.
        at: Collection timestamp.

    Returns:
        Column-major dict including embedded point-in-time pricing.
    """
    recs = []
    for r in entries:
        p = pricing_by_model.get(r.model)
        recs.append(
            {
                "client": r.client,
                "session_id": r.session_id,
                "model": r.model,
                "provider": r.provider,
                "input_tokens": r.input_tokens,
                "output_tokens": r.output_tokens,
                "cache_read": r.cache_read,
                "cache_write": r.cache_write,
                "reasoning": r.reasoning,
                # reasoning is ADDITIVE (fixture-verified), never nested.
                "total_tokens": (
                    r.input_tokens
                    + r.output_tokens
                    + r.cache_read
                    + r.cache_write
                    + r.reasoning
                ),
                "message_count": r.message_count,
                "cost_usd": r.cost_usd,
                "ms_per_1k_tokens": r.ms_per_1k_tokens,
                "perf_duration_ms": r.perf_duration_ms,
                "perf_token_coverage": r.perf_token_coverage,
                "price_input_per_token": p.pricing.input_cost_per_token if p else None,
                "price_output_per_token": p.pricing.output_cost_per_token
                if p
                else None,
                "price_cache_read_per_token": p.pricing.cache_read_input_token_cost
                if p
                else None,
                "price_cache_write_per_token": p.pricing.cache_write_input_token_cost
                if p
                else None,
                "price_matched_key": p.matched_key if p else None,
                "price_match_kind": p.resolution.kind if p else None,
                "price_alias_applied": p.resolution.alias_applied if p else None,
                "price_source": p.source if p else None,
                "price_captured_at": at if p else None,
                "first_seen_at": at,
                "last_seen_at": session_last_seen.get((r.client, r.session_id), at),
                "last_updated_at": at,
            }
        )
    return _col_major(recs) if recs else {}


def _daily_rows(graph: GraphPayload, at: datetime) -> ColumnarData:
    """Build day x client x model rows from graph contributions.

    Args:
        graph: Validated graph payload.
        at: Collection timestamp.

    Returns:
        Column-major dict.
    """
    recs = []
    for c in graph.contributions:
        for cl in c.clients:
            t = cl.tokens
            recs.append(
                {
                    "day": c.date,
                    "client": cl.client,
                    "model": cl.model_id,
                    "provider": cl.provider_id,
                    "input_tokens": t.input,
                    "output_tokens": t.output,
                    "cache_read": t.cache_read,
                    "cache_write": t.cache_write,
                    "reasoning": t.reasoning,
                    "message_count": cl.messages,
                    "cost_usd": cl.cost,
                    "last_updated_at": at,
                }
            )
    return _col_major(recs) if recs else {}


def _activity_rows(graph: GraphPayload, at: datetime) -> ColumnarData:
    """Build day-level intensity and active-time rows.

    Args:
        graph: Validated graph payload.
        at: Collection timestamp.

    Returns:
        Column-major dict.
    """
    recs = [
        {
            "day": c.date,
            "intensity": c.intensity,
            "active_time_ms": c.active_time_ms,
            "last_updated_at": at,
        }
        for c in graph.contributions
    ]
    return _col_major(recs) if recs else {}


def _append_only(
    bundle: CollectionBundle,
    at: datetime,
) -> dict[str, ColumnarData]:
    """Column-build pricing snapshots, run metrics, runs, issues, drift.

    Args:
        bundle: The validated collection bundle.
        at: Collection timestamp.

    Returns:
        Append-only tables keyed by name (empty dicts omitted by caller).
    """
    out: dict[str, ColumnarData] = {}
    if bundle.pricing_by_model:
        recs = [
            {
                "captured_at": at,
                "model": p.model_id,
                "source": p.source,
                "matched_key": p.matched_key,
                "match_kind": p.resolution.kind,
                "price_input_per_token": p.pricing.input_cost_per_token,
                "price_output_per_token": p.pricing.output_cost_per_token,
                "price_cache_read_per_token": p.pricing.cache_read_input_token_cost,
                "price_cache_write_per_token": p.pricing.cache_write_input_token_cost,
            }
            for model_id in sorted(bundle.pricing_by_model)
            for p in [bundle.pricing_by_model[model_id]]
        ]
        out["pricing_snapshots"] = _col_major(recs)
    tm, summary = bundle.graph.time_metrics, bundle.graph.summary
    out["run_metrics"] = {
        "run_id": [bundle.run_id],
        "captured_at": [bundle.graph.meta.generated_at],
        "total_tokens": [summary.total_tokens],
        "total_cost": [summary.total_cost],
        "active_days": [summary.active_days],
        "total_active_time_ms": [tm.total_active_time_ms],
        "longest_continuous_ms": [tm.longest_continuous_ms],
        "max_concurrent_sessions": [tm.max_concurrent_sessions],
        "graph_session_count": [tm.session_count],
    }
    if bundle.drift_fatal:
        st = "failed"
    elif bundle.contract_drift:
        st = "schema_drift"
    else:
        st = "ok" if bundle.reconciliation.ok else "partial"
    out["ingest_runs"] = {
        "run_id": [bundle.run_id],
        "started_at": [bundle.started_at],
        "finished_at": [bundle.finished_at],
        "host": [bundle.host],
        "tokscale_ver": [bundle.graph.meta.version],
        "status": [st],
        "rows_in": [
            (bundle.fetch_summary or {}).get("rows_in")
            or len(bundle.models.entries)
            + len(bundle.report_rows)
            + sum(len(c.clients) for c in bundle.graph.contributions)
        ],
        "rows_inserted": [0],
        "rows_updated": [0],
        "drift_events": [len(bundle.contract_drift)],
    }
    if bundle.reconciliation.issues:
        recs = [
            {
                "run_id": bundle.run_id,
                "check_name": i.check,
                "issue_key": i.key or "",
                "message": i.message,
            }
            for i in bundle.reconciliation.issues
        ]
        out["reconciliation_issues"] = _col_major(recs)
    if bundle.contract_drift:
        recs = [
            {
                "drift_id": d.drift_id,
                "run_id": d.run_id,
                "detected_at": d.detected_at,
                "payload_kind": d.payload_kind,
                "drift_kind": d.drift_kind,
                "path": d.path,
                "detail": d.detail,
                "tokscale_ver": d.tokscale_ver,
                "resolved": False,
            }
            for d in bundle.contract_drift
        ]
        out["schema_drift"] = _col_major(recs)
    return out


def normalize(bundle: CollectionBundle) -> NormalizedBundle:
    """Convert a validated bundle into canonical Arrow tables.

    Args:
        bundle: Parsed payloads, rates, reconciliation, drift results.

    Returns:
        Tables keyed by name.
    """
    at = bundle.finished_at
    tables: dict[str, pa.Table] = {}
    session_last_seen = {
        (row.client, row.session_id): row.last_active or at
        for row in bundle.report_rows
    }
    for name, cols in (
        ("sessions", _session_rows(bundle.report_rows, at)),
        (
            "session_model_stats",
            _stats_rows(
                list(bundle.models.entries),
                bundle.pricing_by_model,
                session_last_seen,
                at,
            ),
        ),
        ("daily_stats", _daily_rows(bundle.graph, at)),
        ("daily_activity", _activity_rows(bundle.graph, at)),
    ):
        if cols:
            tables[name] = pa.Table.from_pydict(cols)
    for name, cols in _append_only(bundle, at).items():
        if cols:
            tables[name] = pa.Table.from_pydict(cols)
    return NormalizedBundle(bundle.run_id, tables)
