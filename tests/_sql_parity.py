# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_sql_parity.py — Shared SQL dialect-parity test helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import resources
from typing import cast

import pyarrow as pa
import sqlglot
from sqlglot import exp

from usagebassoon.backends.base import StorageBackend
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS
from usagebassoon.schema_assets import PARITY_SCHEMA_ASSETS

DIALECTS = ("duckdb", "bigquery")
_CAPTURED_AT = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
_SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def asset_sql(dialect: str, filename: str) -> str:
    """Read one packaged SQL asset for a supported dialect."""
    if dialect not in DIALECTS:
        raise ValueError(f"unsupported SQL dialect {dialect!r}")
    if filename not in PARITY_SCHEMA_ASSETS:
        raise ValueError(f"unsupported SQL asset {filename!r}")
    return resources.files(f"usagebassoon.sql.{dialect}").joinpath(filename).read_text()


def statements(dialect: str, filename: str) -> list[exp.Expr]:
    """Parse executable statements from one packaged SQL asset."""
    return [
        statement
        for statement in sqlglot.parse(asset_sql(dialect, filename), read=dialect)
        if statement is not None
    ]


def view_names(dialect: str = "duckdb") -> tuple[str, ...]:
    """Return the shipped view names in their declared order."""
    names: list[str] = []
    for statement in statements(dialect, "views.sql"):
        if not isinstance(statement, exp.Create) or statement.kind != "VIEW":
            raise ValueError("views.sql must contain only CREATE VIEW statements")
        names.append(statement.this.name)
    return tuple(names)


def seed_synthetic_data(backend: StorageBackend) -> None:
    """Append purpose-built rows that exercise every shipped SQL view."""
    sessions = pa.Table.from_pylist(
        [
            {
                "source_id": _SOURCE_ID,
                "client": "codex",
                "session_id": "session-alpha",
                "workspace": "alpha",
                "workspace_label": "Alpha",
                "created_at": _CAPTURED_AT,
                "last_active": _CAPTURED_AT,
                "duration_minutes": 1,
                "message_count": 1,
                "tokscale_cost_usd": 1.5,
                "models_used": ["model-alpha"],
                "session_label": "alpha",
                "first_seen_at": _CAPTURED_AT,
                "last_seen_at": _CAPTURED_AT,
                "updated_at": _CAPTURED_AT,
            },
            {
                "source_id": _SOURCE_ID,
                "client": "codex",
                "session_id": "session-beta",
                "workspace": "beta",
                "workspace_label": "Beta",
                "created_at": _CAPTURED_AT,
                "last_active": _CAPTURED_AT,
                "duration_minutes": 1,
                "message_count": 1,
                "tokscale_cost_usd": 0.7,
                "models_used": ["model-beta"],
                "session_label": "beta",
                "first_seen_at": _CAPTURED_AT,
                "last_seen_at": _CAPTURED_AT,
                "updated_at": _CAPTURED_AT,
            },
            {
                "source_id": _SOURCE_ID,
                "client": "codex",
                "session_id": "session-gamma",
                "workspace": "gamma",
                "workspace_label": "Gamma",
                "created_at": _CAPTURED_AT,
                "last_active": _CAPTURED_AT,
                "duration_minutes": 1,
                "message_count": 1,
                "tokscale_cost_usd": 1.5,
                "models_used": ["model-gamma"],
                "session_label": "gamma",
                "first_seen_at": _CAPTURED_AT,
                "last_seen_at": _CAPTURED_AT,
                "updated_at": _CAPTURED_AT,
            },
            {
                "source_id": _SOURCE_ID,
                "client": "codex",
                "session_id": "session-empty",
                "workspace": "empty",
                "workspace_label": "Empty",
                "created_at": _CAPTURED_AT,
                "last_active": _CAPTURED_AT,
                "duration_minutes": 1,
                "message_count": 0,
                "tokscale_cost_usd": 0.0,
                "models_used": [],
                "session_label": "empty",
                "first_seen_at": _CAPTURED_AT,
                "last_seen_at": _CAPTURED_AT,
                "updated_at": _CAPTURED_AT,
            },
        ],
        schema=CANONICAL_TABLE_SCHEMAS["sessions"],
    )
    daily_stats = pa.Table.from_pylist(
        [
            {
                "source_id": _SOURCE_ID,
                "day": date(2026, 9, 20),
                "client": "codex",
                "session_id": "session-alpha",
                "model": "model-alpha",
                "provider": "provider",
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_read": 0,
                "cache_write": 0,
                "reasoning": 0,
                "total_tokens": 15,
                "message_count": 1,
                "tokscale_cost_usd": 1.5,
                "perf_duration_ms": 30,
                "perf_timed_tokens": 15,
                "perf_sample_count": 1,
                "perf_token_coverage": 1.0,
                "tokscale_ms_per_1k_tokens": 2_000.0,
                "updated_at": _CAPTURED_AT,
            },
            {
                "source_id": _SOURCE_ID,
                "day": date(2026, 9, 20),
                "client": "codex",
                "session_id": "session-beta",
                "model": "model-beta",
                "provider": "provider",
                "input_tokens": 0,
                "output_tokens": 7,
                "cache_read": 0,
                "cache_write": 0,
                "reasoning": 0,
                "total_tokens": 7,
                "message_count": 1,
                "tokscale_cost_usd": 0.7,
                "perf_duration_ms": 14,
                "perf_timed_tokens": 7,
                "perf_sample_count": 1,
                "perf_token_coverage": 1.0,
                "tokscale_ms_per_1k_tokens": 2_000.0,
                "updated_at": _CAPTURED_AT,
            },
            {
                "source_id": _SOURCE_ID,
                "day": date(2026, 9, 20),
                "client": "codex",
                "session_id": "session-gamma",
                "model": "model-gamma",
                "provider": "provider",
                "input_tokens": 3,
                "output_tokens": 0,
                "cache_read": 0,
                "cache_write": 0,
                "reasoning": 0,
                "total_tokens": 3,
                "message_count": 1,
                "tokscale_cost_usd": 1.5,
                "perf_duration_ms": 100,
                "perf_timed_tokens": 0,
                "perf_sample_count": 0,
                "perf_token_coverage": 0.0,
                "tokscale_ms_per_1k_tokens": 0.0,
                "updated_at": _CAPTURED_AT,
            },
        ],
        schema=CANONICAL_TABLE_SCHEMAS["daily_stats"],
    )
    price_versions = pa.Table.from_pylist(
        [
            {
                "source_id": _SOURCE_ID,
                "day": date(2026, 9, 20),
                "model": "model-alpha",
                "source": "synthetic",
                "matched_key": "model-alpha",
                "match_kind": "exact",
                "price_input_per_token": 0.1,
                "price_output_per_token": 0.1,
                "price_cache_read_per_token": 0.0,
                "price_cache_write_per_token": 0.0,
                "observed_at": _CAPTURED_AT,
                "updated_at": _CAPTURED_AT,
            },
            {
                "source_id": _SOURCE_ID,
                "day": date(2026, 9, 20),
                "model": "model-beta",
                "source": "synthetic",
                "matched_key": "model-beta",
                "match_kind": "exact",
                "price_input_per_token": 0.1,
                "price_output_per_token": None,
                "price_cache_read_per_token": 0.0,
                "price_cache_write_per_token": 0.0,
                "observed_at": _CAPTURED_AT,
                "updated_at": _CAPTURED_AT,
            },
            {
                "source_id": _SOURCE_ID,
                "day": date(2026, 9, 20),
                "model": "model-gamma",
                "source": "synthetic",
                "matched_key": "model-gamma",
                "match_kind": "exact",
                "price_input_per_token": 0.5,
                "price_output_per_token": None,
                "price_cache_read_per_token": 0.0,
                "price_cache_write_per_token": 0.0,
                "observed_at": _CAPTURED_AT,
                "updated_at": _CAPTURED_AT,
            },
        ],
        schema=CANONICAL_TABLE_SCHEMAS["price_versions"],
    )
    tags = pa.table(
        {
            "scope": ["client", "workspace", "session", "session"],
            "source_id": [_SOURCE_ID] * 4,
            "client": ["codex", "", "codex", "codex"],
            "workspace": ["", "alpha", "", ""],
            "session_id": ["", "", "session-alpha", "session-empty"],
            "tag": ["client-tag", "workspace-tag", "session-tag", "empty-tag"],
            "created_at": [_CAPTURED_AT] * 4,
            "updated_at": [_CAPTURED_AT] * 4,
        }
    )
    notes = pa.table(
        {
            "source_id": [_SOURCE_ID],
            "client": ["codex"],
            "session_id": ["session-alpha"],
            "note": ["synthetic note"],
            "created_at": [_CAPTURED_AT],
            "updated_at": [_CAPTURED_AT],
        }
    )
    for name, table in (
        ("sessions", sessions),
        ("daily_stats", daily_stats),
        ("price_versions", price_versions),
        ("tags", tags),
        ("notes", notes),
    ):
        backend.append(name, table)


def normalized_records(
    table: pa.Table,
    *,
    preserve_order: bool = False,
) -> list[dict[str, object]]:
    """Return comparison-safe records with stable optional ordering."""
    records = [
        cast(dict[str, object], _normalize_value(record))
        for record in table.to_pylist()
    ]
    if preserve_order:
        return records
    return sorted(records, key=_record_sort_key)


def assert_view_results_match(
    left: StorageBackend,
    right: StorageBackend,
    names: Iterable[str] | None = None,
    *,
    left_label: str = "DuckDB",
    right_label: str = "BigQuery",
) -> None:
    """Compare every requested view's Arrow schema and normalized rows."""
    for name in names or view_names():
        left_result = left.query(f"SELECT * FROM {name}")
        right_result = right.query(f"SELECT * FROM {name}")
        assert left_result.column_names == right_result.column_names, (
            f"{name}: result columns differ: "
            f"{left_label}={left_result.column_names!r}, "
            f"{right_label}={right_result.column_names!r}"
        )
        preserve_order = name == "report_summary_models"
        left_records = normalized_records(
            left_result,
            preserve_order=preserve_order,
        )
        right_records = normalized_records(
            right_result,
            preserve_order=preserve_order,
        )
        _assert_records_match(
            name,
            left_records,
            right_records,
            left_label=left_label,
            right_label=right_label,
        )
    report_models = left.query("SELECT * FROM report_summary_models")
    assert report_models.column("model").to_pylist() == [
        "model-alpha",
        "model-gamma",
        "model-beta",
    ]


def _normalize_value(value: object) -> object:
    """Convert Arrow scalar values to comparison-safe JSON values."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC).isoformat()
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 12)
    if isinstance(value, Decimal):
        return round(float(value), 12)
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize_value(item) for key, item in value.items()}
    return cast(str | int | bool | None, value)


def _assert_records_match(
    view: str,
    left: list[dict[str, object]],
    right: list[dict[str, object]],
    *,
    left_label: str,
    right_label: str,
) -> None:
    """Raise an assertion identifying the first cross-engine result mismatch."""
    if len(left) != len(right):
        raise AssertionError(
            f"{view}: row count differs: {left_label}={len(left)}, "
            f"{right_label}={len(right)}; "
            f"{left_label} rows={left!r}; {right_label} rows={right!r}"
        )
    for index, (left_row, right_row) in enumerate(zip(left, right, strict=True)):
        if left_row == right_row:
            continue
        differences = {
            field: (left_row.get(field), right_row.get(field))
            for field in left_row.keys() | right_row.keys()
            if left_row.get(field) != right_row.get(field)
        }
        raise AssertionError(
            f"{view}: row {index} differs by field: {differences!r}; "
            f"{left_label} row={left_row!r}; {right_label} row={right_row!r}"
        )


def _record_sort_key(record: dict[str, object]) -> str:
    """Serialize one normalized record into a deterministic sort key."""
    return json.dumps(record, sort_keys=True)
