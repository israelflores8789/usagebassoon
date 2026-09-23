# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""normalizer.py — Validated tokscale payloads to canonical Arrow tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

import pyarrow as pa

from usagebassoon.parsers.report import SessionRow, make_session_label

if TYPE_CHECKING:
    from usagebassoon.ingest import CollectionBundle, IngestStatus
    from usagebassoon.parsers.daily import DailyModelsPayload
    from usagebassoon.parsers.graph import GraphPayload
    from usagebassoon.parsers.pricing import PricingRow

type ColumnarData = dict[str, list[object | None]]

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
    "ingest_status": pa.schema(
        [
            pa.field("source_id", pa.string()),
            pa.field("day", pa.date32()),
            pa.field("domain", pa.string()),
            pa.field("status", pa.string()),
            pa.field("expected_count", pa.int64()),
            pa.field("succeeded_count", pa.int64()),
            pa.field("last_attempted_run", pa.string()),
            pa.field("last_succeeded_run", pa.string()),
            pa.field("failure_code", pa.string()),
            pa.field("updated_at", _TIMESTAMP),
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


def _ingest_status_rows(
    statuses: tuple[IngestStatus, ...], at: datetime, source_id: str
) -> ColumnarData:
    """Build retry-ledger rows for domain outcomes in this collection."""
    records = [
        {
            "source_id": source_id,
            "day": status.day,
            "domain": status.domain,
            "status": status.status,
            "expected_count": status.expected_count,
            "succeeded_count": status.succeeded_count,
            "last_attempted_run": status.last_attempted_run,
            "last_succeeded_run": status.last_succeeded_run,
            "failure_code": status.failure_code,
            "updated_at": at,
        }
        for status in sorted(statuses, key=_ingest_status_key)
    ]
    return _col_major(records) if records else {}


def _ingest_status_key(status: IngestStatus) -> tuple[date, str]:
    """Order one retry-ledger row deterministically by day and domain."""
    return (status.day, status.domain)


def _append_only(bundle: CollectionBundle, at: datetime) -> dict[str, ColumnarData]:
    """Build audit, drift, and reconciliation history tables."""
    rows_in = (bundle.fetch_summary or {}).get("rows_in") or sum(
        len(payload.entries) for payload in bundle.daily_models.values()
    ) + len(bundle.report_rows) + len(bundle.graph.contributions)
    if bundle.drift_fatal:
        status = "failed"
    elif bundle.contract_drift:
        status = "schema_drift"
    elif (
        any(status.status != "complete" for status in bundle.ingest_status)
        or not bundle.reconciliation.ok
    ):
        status = "partial"
    else:
        status = "ok"
    tables: dict[str, ColumnarData] = {
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
            "ingest_status",
            _ingest_status_rows(bundle.ingest_status, at, bundle.source_id),
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
