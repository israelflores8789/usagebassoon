# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""normalizer.py — Validated tokscale payloads to canonical Arrow tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import pyarrow as pa

from usagebassoon.contracts import ContractDrift
from usagebassoon.parsers.daily import DailyModelsPayload
from usagebassoon.parsers.graph import GraphPayload
from usagebassoon.parsers.pricing import PricingRow
from usagebassoon.parsers.report import SessionRow, make_session_label
from usagebassoon.reconcile import ReconciliationResult
from usagebassoon.system_metadata import SystemMetadata

type ColumnarData = dict[str, list[object | None]]
type ProcessingTarget = tuple[date, str]

_TIMESTAMP = pa.timestamp("us", tz="UTC")

CANONICAL_TABLE_SCHEMAS: dict[str, pa.Schema] = {
    "sessions": pa.schema(
        [
            pa.field("source_id", pa.string()),
            pa.field("client", pa.string()),
            pa.field("session_id", pa.string()),
            pa.field("workspace", pa.string()),
            pa.field("workspace_label", pa.string()),
            pa.field("created_at", _TIMESTAMP),
            pa.field("last_active", _TIMESTAMP),
            pa.field("duration_minutes", pa.int64()),
            pa.field("message_count", pa.int64()),
            pa.field("tokscale_cost_usd", pa.float64()),
            pa.field("models_used", pa.list_(pa.string())),
            pa.field("session_label", pa.string()),
            pa.field("first_seen_at", _TIMESTAMP),
            pa.field("last_seen_at", _TIMESTAMP),
            pa.field("updated_at", _TIMESTAMP),
        ]
    ),
    "daily_stats": pa.schema(
        [
            pa.field("source_id", pa.string()),
            pa.field("day", pa.date32()),
            pa.field("client", pa.string()),
            pa.field("session_id", pa.string()),
            pa.field("model", pa.string()),
            pa.field("provider", pa.string()),
            pa.field("input_tokens", pa.int64()),
            pa.field("output_tokens", pa.int64()),
            pa.field("cache_read", pa.int64()),
            pa.field("cache_write", pa.int64()),
            pa.field("reasoning", pa.int64()),
            pa.field("total_tokens", pa.int64()),
            pa.field("message_count", pa.int64()),
            pa.field("tokscale_cost_usd", pa.float64()),
            pa.field("updated_at", _TIMESTAMP),
        ]
    ),
    "daily_activity": pa.schema(
        [
            pa.field("source_id", pa.string()),
            pa.field("day", pa.date32()),
            pa.field("intensity", pa.int64()),
            pa.field("active_time_ms", pa.int64()),
            pa.field("updated_at", _TIMESTAMP),
        ]
    ),
    "price_versions": pa.schema(
        [
            pa.field("source_id", pa.string()),
            pa.field("day", pa.date32()),
            pa.field("model", pa.string()),
            pa.field("source", pa.string()),
            pa.field("matched_key", pa.string()),
            pa.field("match_kind", pa.string()),
            pa.field("price_input_per_token", pa.float64()),
            pa.field("price_output_per_token", pa.float64()),
            pa.field("price_cache_read_per_token", pa.float64()),
            pa.field("price_cache_write_per_token", pa.float64()),
            pa.field("observed_at", _TIMESTAMP),
            pa.field("updated_at", _TIMESTAMP),
        ]
    ),
    "daily_processed_state": pa.schema(
        [
            pa.field("source_id", pa.string()),
            pa.field("day", pa.date32()),
            pa.field("target", pa.string()),
            pa.field("processed_at", _TIMESTAMP),
        ]
    ),
    "run_metrics": pa.schema(
        [
            pa.field("run_id", pa.string()),
            pa.field("source_id", pa.string()),
            pa.field("captured_at", _TIMESTAMP),
            pa.field("total_tokens", pa.int64()),
            pa.field("tokscale_total_cost_usd", pa.float64()),
            pa.field("active_days", pa.int64()),
            pa.field("total_active_time_ms", pa.int64()),
            pa.field("longest_continuous_ms", pa.int64()),
            pa.field("max_concurrent_sessions", pa.int64()),
            pa.field("graph_session_count", pa.int64()),
        ]
    ),
    "ingest_runs": pa.schema(
        [
            pa.field("run_id", pa.string()),
            pa.field("source_id", pa.string()),
            pa.field("started_at", _TIMESTAMP),
            pa.field("finished_at", _TIMESTAMP),
            pa.field("host", pa.string()),
            pa.field("os_name", pa.string()),
            pa.field("os_version", pa.string()),
            pa.field("architecture", pa.string()),
            pa.field("cpu_model", pa.string()),
            pa.field("cpu_count", pa.int64()),
            pa.field("memory_bytes", pa.int64()),
            pa.field("shell", pa.string()),
            pa.field("tokscale_ver", pa.string()),
            pa.field("status", pa.string()),
            pa.field("rows_in", pa.int64()),
            pa.field("rows_inserted", pa.int64()),
            pa.field("rows_updated", pa.int64()),
            pa.field("drift_events", pa.int64()),
        ]
    ),
    "reconciliation_issues": pa.schema(
        [
            pa.field("run_id", pa.string()),
            pa.field("source_id", pa.string()),
            pa.field("check_name", pa.string()),
            pa.field("issue_key", pa.string()),
            pa.field("message", pa.string()),
        ]
    ),
    "schema_drift": pa.schema(
        [
            pa.field("drift_id", pa.string()),
            pa.field("run_id", pa.string()),
            pa.field("source_id", pa.string()),
            pa.field("detected_at", _TIMESTAMP),
            pa.field("payload_kind", pa.string()),
            pa.field("drift_kind", pa.string()),
            pa.field("path", pa.string()),
            pa.field("detail", pa.string()),
            pa.field("tokscale_ver", pa.string()),
            pa.field("resolved", pa.bool_()),
        ]
    ),
}


@dataclass(frozen=True, slots=True)
class CollectionBundle:
    """All data produced by one successful collector invocation.

    Attributes:
        run_id: Identifier for this collection run.
        source_id: Stable namespace of the collector that observed this data.
        started_at: When the collector began invoking tokscale.
        finished_at: When parsing finished and facts were observed.
        host: Hostname or container id, if known.
        daily_models: Daily session and model statistics keyed by requested day.
        report_rows: Stable session metadata from tokscale report.
        graph: Activity dates and graph telemetry.
        pricing_by_day: Rate cards keyed by their associated usage day.
        processed_targets: Targets whose data is safe to mark processed.
        failed_targets: Targets left unmarked for the next collection retry.
        reconciliation: Non-fatal collection consistency observations.
        contract_drift: Schema contract deviations observed this run.
        fetch_summary: Rows-in counts for the ingest audit.
        system_metadata: Best-effort collector host metadata.
    """

    run_id: str
    source_id: str
    started_at: datetime
    finished_at: datetime
    host: str | None
    daily_models: dict[date, DailyModelsPayload]
    report_rows: list[SessionRow]
    graph: GraphPayload
    pricing_by_day: dict[date, dict[str, PricingRow]]
    processed_targets: frozenset[ProcessingTarget]
    failed_targets: frozenset[ProcessingTarget]
    reconciliation: ReconciliationResult
    contract_drift: tuple[ContractDrift, ...] = ()
    fetch_summary: dict[str, int] | None = None
    drift_fatal: bool = False
    system_metadata: SystemMetadata | None = None


@dataclass(frozen=True, slots=True)
class NormalizedBundle:
    """Canonical Arrow tables keyed by table name, run-stamped."""

    run_id: str
    tables: dict[str, pa.Table]


def _col_major[T](records: list[dict[str, T]]) -> ColumnarData:
    """Flip a uniformly-keyed record list into a column mapping."""
    columns: ColumnarData = {key: [] for key in records[0]}
    for record in records:
        for key, value in record.items():
            columns[key].append(value)
    return columns


def _table(name: str, columns: ColumnarData) -> pa.Table:
    """Create one normalized table with its canonical Arrow schema."""
    try:
        schema = CANONICAL_TABLE_SCHEMAS[name]
    except KeyError as error:
        raise ValueError(f"missing canonical Arrow schema for {name!r}") from error
    return pa.Table.from_pydict(columns, schema=schema)


def _session_rows(rows: list[SessionRow], at: datetime, source_id: str) -> ColumnarData:
    """Build the stable session dimension rows for one collection run."""
    records = [
        {
            "source_id": source_id,
            "client": row.client,
            "session_id": row.session_id,
            "workspace": row.workspace,
            "workspace_label": row.workspace_label,
            "created_at": row.created_at,
            "last_active": row.last_active,
            "duration_minutes": row.duration_minutes,
            "message_count": row.message_count,
            "tokscale_cost_usd": row.tokscale_cost_usd,
            "models_used": sorted(row.models_used),
            "session_label": make_session_label(row),
            "first_seen_at": at,
            "last_seen_at": row.last_active or at,
            "updated_at": at,
        }
        for row in rows
    ]
    return _col_major(records) if records else {}


def _daily_stats_rows(
    daily_models: dict[date, DailyModelsPayload], at: datetime, source_id: str
) -> ColumnarData:
    """Build persisted day, session, and model usage facts."""
    records: list[dict[str, object]] = []
    for day in sorted(daily_models):
        for daily_row in daily_models[day].entries:
            row = daily_row.stats
            records.append(
                {
                    "source_id": source_id,
                    "day": daily_row.day,
                    "client": row.client,
                    "session_id": row.session_id,
                    "model": row.model,
                    "provider": row.provider,
                    "input_tokens": row.input_tokens,
                    "output_tokens": row.output_tokens,
                    "cache_read": row.cache_read,
                    "cache_write": row.cache_write,
                    "reasoning": row.reasoning,
                    "total_tokens": (
                        row.input_tokens
                        + row.output_tokens
                        + row.cache_read
                        + row.cache_write
                        + row.reasoning
                    ),
                    "message_count": row.message_count,
                    "tokscale_cost_usd": row.tokscale_cost_usd,
                    "updated_at": at,
                }
            )
    return _col_major(records) if records else {}


def _activity_rows(graph: GraphPayload, at: datetime, source_id: str) -> ColumnarData:
    """Build graph-sourced daily activity rows."""
    records = [
        {
            "source_id": source_id,
            "day": contribution.date,
            "intensity": contribution.intensity,
            "active_time_ms": contribution.active_time_ms,
            "updated_at": at,
        }
        for contribution in graph.contributions
    ]
    return _col_major(records) if records else {}


def _price_version_rows(
    pricing_by_day: dict[date, dict[str, PricingRow]], at: datetime, source_id: str
) -> ColumnarData:
    """Build point-in-time rates associated with each processed usage day."""
    records: list[dict[str, object]] = []
    for day in sorted(pricing_by_day):
        for model in sorted(pricing_by_day[day]):
            pricing = pricing_by_day[day][model]
            records.append(
                {
                    "source_id": source_id,
                    "day": day,
                    "model": pricing.model_id,
                    "source": pricing.source,
                    "matched_key": pricing.matched_key,
                    "match_kind": pricing.resolution.kind,
                    "price_input_per_token": pricing.pricing.input_cost_per_token,
                    "price_output_per_token": pricing.pricing.output_cost_per_token,
                    "price_cache_read_per_token": (
                        pricing.pricing.cache_read_input_token_cost
                    ),
                    "price_cache_write_per_token": (
                        pricing.pricing.cache_write_input_token_cost
                        if pricing.pricing.cache_write_input_token_cost is not None
                        else 0.0
                    ),
                    "observed_at": at,
                    "updated_at": at,
                }
            )
    return _col_major(records) if records else {}


def _processed_state_rows(
    targets: frozenset[ProcessingTarget], at: datetime, source_id: str
) -> ColumnarData:
    """Build state rows only for targets whose data is in this batch."""
    records = [
        {"source_id": source_id, "day": day, "target": target, "processed_at": at}
        for day, target in sorted(targets)
    ]
    return _col_major(records) if records else {}


def _append_only(bundle: CollectionBundle, at: datetime) -> dict[str, ColumnarData]:
    """Build audit, telemetry, drift, and reconciliation history tables."""
    graph_summary = bundle.graph.summary
    time_metrics = bundle.graph.time_metrics
    rows_in = (bundle.fetch_summary or {}).get("rows_in") or sum(
        len(payload.entries) for payload in bundle.daily_models.values()
    ) + len(bundle.report_rows) + len(bundle.graph.contributions)
    if bundle.drift_fatal:
        status = "failed"
    elif bundle.contract_drift:
        status = "schema_drift"
    elif bundle.failed_targets or not bundle.reconciliation.ok:
        status = "partial"
    else:
        status = "ok"
    tables: dict[str, ColumnarData] = {
        "run_metrics": {
            "run_id": [bundle.run_id],
            "source_id": [bundle.source_id],
            "captured_at": [bundle.graph.meta.generated_at],
            "total_tokens": [graph_summary.total_tokens],
            "tokscale_total_cost_usd": [graph_summary.total_cost],
            "active_days": [graph_summary.active_days],
            "total_active_time_ms": [time_metrics.total_active_time_ms],
            "longest_continuous_ms": [time_metrics.longest_continuous_ms],
            "max_concurrent_sessions": [time_metrics.max_concurrent_sessions],
            "graph_session_count": [time_metrics.session_count],
        },
        "ingest_runs": {
            "run_id": [bundle.run_id],
            "source_id": [bundle.source_id],
            "started_at": [bundle.started_at],
            "finished_at": [bundle.finished_at],
            "host": [bundle.host],
            "os_name": [
                bundle.system_metadata.os_name if bundle.system_metadata else None
            ],
            "os_version": [
                bundle.system_metadata.os_version if bundle.system_metadata else None
            ],
            "architecture": [
                bundle.system_metadata.architecture if bundle.system_metadata else None
            ],
            "cpu_model": [
                bundle.system_metadata.cpu_model if bundle.system_metadata else None
            ],
            "cpu_count": [
                bundle.system_metadata.cpu_count if bundle.system_metadata else None
            ],
            "memory_bytes": [
                bundle.system_metadata.memory_bytes if bundle.system_metadata else None
            ],
            "shell": [bundle.system_metadata.shell if bundle.system_metadata else None],
            "tokscale_ver": [bundle.graph.meta.version],
            "status": [status],
            "rows_in": [rows_in],
            "rows_inserted": [0],
            "rows_updated": [0],
            "drift_events": [len(bundle.contract_drift)],
        },
    }
    if bundle.reconciliation.issues:
        tables["reconciliation_issues"] = _col_major(
            [
                {
                    "run_id": bundle.run_id,
                    "source_id": bundle.source_id,
                    "check_name": issue.check,
                    "issue_key": issue.key or "",
                    "message": issue.message,
                }
                for issue in bundle.reconciliation.issues
            ]
        )
    if bundle.contract_drift:
        tables["schema_drift"] = _col_major(
            [
                {
                    "drift_id": drift.drift_id,
                    "run_id": drift.run_id,
                    "source_id": bundle.source_id,
                    "detected_at": drift.detected_at,
                    "payload_kind": drift.payload_kind,
                    "drift_kind": drift.drift_kind,
                    "path": drift.path,
                    "detail": drift.detail,
                    "tokscale_ver": drift.tokscale_ver,
                    "resolved": False,
                }
                for drift in bundle.contract_drift
            ]
        )
    return tables


def normalize(bundle: CollectionBundle) -> NormalizedBundle:
    """Convert a validated bundle into canonical Arrow tables."""
    at = bundle.finished_at
    columns_by_table = (
        ("sessions", _session_rows(bundle.report_rows, at, bundle.source_id)),
        ("daily_stats", _daily_stats_rows(bundle.daily_models, at, bundle.source_id)),
        ("daily_activity", _activity_rows(bundle.graph, at, bundle.source_id)),
        (
            "price_versions",
            _price_version_rows(bundle.pricing_by_day, at, bundle.source_id),
        ),
        (
            "daily_processed_state",
            _processed_state_rows(bundle.processed_targets, at, bundle.source_id),
        ),
    )
    tables = {
        name: _table(name, columns) for name, columns in columns_by_table if columns
    }
    tables.update(
        {
            name: _table(name, columns)
            for name, columns in _append_only(bundle, at).items()
            if columns
        }
    )
    return NormalizedBundle(bundle.run_id, tables)
