# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""normalizer.py — Validated tokscale payloads to canonical Arrow tables."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pyarrow as pa

from usagebassoon.drift import SchemaDriftIdentity, drift_identity
from usagebassoon.parsers.report import SessionRow, make_session_label
from usagebassoon.storage_model import CANONICAL_TABLE_SCHEMAS
from usagebassoon.system_metadata import capture_system_metadata

if TYPE_CHECKING:
    from usagebassoon.ingest import CollectionBundle
    from usagebassoon.parsers.daily import DailyModelsPayload
    from usagebassoon.parsers.pricing import PricingRow

type ColumnarData = dict[str, list[object | None]]


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
    columns["event_id"] = [str(uuid4()) for _ in next(iter(columns.values()))]
    return pa.Table.from_pydict(columns, schema=schema)


def _session_rows(
    rows: list[SessionRow], at: datetime, source_id: str, *, freshness_at: datetime
) -> ColumnarData:
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
            "collected_at": freshness_at,
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
                    "perf_duration_ms": row.perf_duration_ms,
                    "perf_timed_tokens": row.perf_timed_tokens,
                    "perf_sample_count": row.perf_sample_count,
                    "perf_token_coverage": row.perf_token_coverage,
                    "tokscale_ms_per_1k_tokens": row.tokscale_ms_per_1k_tokens,
                    "collected_at": at,
                }
            )
    return _col_major(records) if records else {}


def _price_version_rows(
    pricing_by_day: dict[date, dict[str, PricingRow]],
    source_id: str,
    *,
    freshness_at: datetime,
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
                    "collected_at": freshness_at,
                }
            )
    return _col_major(records) if records else {}


def _diagnostic_rows(bundle: CollectionBundle, at: datetime) -> dict[str, ColumnarData]:
    """Build collection outcomes and append-only diagnostic observation events."""
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
        "collection_ledger": {
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
            "day": [bundle.started_at.date()],
            "domain": ["collection"],
            "expected_count": [None],
            "succeeded_count": [None],
            "failure_code": [None],
            "collected_at": [bundle.started_at],
        },
    }
    ledger = tables["collection_ledger"]
    for target in bundle.ingest_status:
        for field, values in ledger.items():
            value = values[0]
            if field in {
                "day",
                "domain",
                "status",
                "expected_count",
                "succeeded_count",
                "failure_code",
            }:
                value = getattr(target, field)
            if (
                field == "status"
                and target.domain == "models"
                and target.status == "complete"
                and target.day >= bundle.started_at.date()
            ):
                # A successful open-day observation cannot finalize the day.
                value = "provisional"
            values.append(value)
    if bundle.reconciliation.issues or bundle.reconciliation.resolved:
        issue_rows: list[dict[str, object]] = [
            {
                "run_id": bundle.run_id,
                "source_id": bundle.source_id,
                "check_name": issue.check,
                "issue_key": issue.key,
                "message": issue.message,
                "created_at": at,
                "collected_at": at,
                "resolved": False,
                "observation_count": 1,
            }
            for issue in bundle.reconciliation.issues
        ] + [
            {
                "run_id": bundle.run_id,
                "source_id": bundle.source_id,
                "check_name": check,
                "issue_key": key,
                "message": None,
                "created_at": at,
                "collected_at": at,
                "resolved": True,
                "observation_count": 0,
            }
            for check, key in bundle.reconciliation.resolved
        ]
        tables["reconciliation_issues"] = _col_major(issue_rows)
    event_rows: dict[SchemaDriftIdentity, dict[str, object]] = {}
    tokscale_ver = bundle.graph.meta.version
    for drift in bundle.contract_drift:
        identity = (drift.domain, tokscale_ver, drift.drift_key)
        row = event_rows.get(identity)
        if row is None:
            event_rows[identity] = {
                "run_id": bundle.run_id,
                "source_id": bundle.source_id,
                "domain": drift.domain,
                "tokscale_ver": tokscale_ver,
                "drift_key": drift.drift_key,
                "drift_kind": drift.drift_kind,
                "path": drift.path,
                "detail": drift.detail,
                "contract_tokscale_ver": drift.contract_tokscale_ver,
                "created_at": at,
                "collected_at": at,
                "resolved": False,
                "observation_count": 1,
            }
        else:
            row["detail"] = drift.detail
            row["contract_tokscale_ver"] = drift.contract_tokscale_ver
            row["observation_count"] = cast(int, row["observation_count"]) + 1
    for state in bundle.resolved_schema_drift:
        identity = drift_identity(state)
        event_rows[identity] = {
            "run_id": bundle.run_id,
            "source_id": bundle.source_id,
            "domain": state.domain,
            "tokscale_ver": state.tokscale_ver,
            "drift_key": state.drift_key,
            "drift_kind": state.drift_kind,
            "path": state.path,
            "detail": state.detail,
            "contract_tokscale_ver": state.contract_tokscale_ver,
            "created_at": state.created_at,
            "collected_at": at,
            "resolved": True,
            "observation_count": 0,
        }
    if event_rows:
        tables["schema_drift_events"] = _col_major(list(event_rows.values()))
    return tables


def normalize(bundle: CollectionBundle) -> NormalizedBundle:
    """Convert a validated bundle into canonical Arrow tables.

    Uses collection start to order current-state updates.
    """
    at = bundle.finished_at
    freshness_at = bundle.started_at
    columns_by_table = (
        (
            "sessions",
            _session_rows(
                bundle.report_rows, at, bundle.source_id, freshness_at=freshness_at
            ),
        ),
        (
            "daily_stats",
            _daily_stats_rows(bundle.daily_models, freshness_at, bundle.source_id),
        ),
        (
            "price_versions",
            _price_version_rows(
                bundle.pricing_by_day, bundle.source_id, freshness_at=freshness_at
            ),
        ),
    )
    tables = {
        name: _table(name, columns) for name, columns in columns_by_table if columns
    }
    tables.update(
        {
            name: _table(name, columns)
            for name, columns in _diagnostic_rows(bundle, at).items()
            if columns
        }
    )
    return NormalizedBundle(bundle.run_id, tables)


def failed_collection(
    *,
    run_id: str,
    source_id: str,
    started_at: datetime,
    finished_at: datetime,
    host: str,
    error: Exception,
) -> NormalizedBundle:
    """Record a failed client acquisition without inventing token observations."""
    row = {
        **asdict(capture_system_metadata()),
        "event_id": str(uuid4()),
        "run_id": run_id,
        "source_id": source_id,
        "day": started_at.date(),
        "domain": "collection",
        "collected_at": started_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "host": host,
        "status": "failed",
        "failure_code": type(error).__name__,
    }
    ledger = pa.Table.from_pylist(
        [row], schema=CANONICAL_TABLE_SCHEMAS["collection_ledger"]
    )
    return NormalizedBundle(run_id, {"collection_ledger": ledger})
