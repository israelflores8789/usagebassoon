# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""merge.py — Persistence orchestration.

From normalized Arrow tables to a StorageBackend.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from usagebassoon.backends.base import StorageBackend, UpsertResult
from usagebassoon.normalizer import NormalizedBundle

CURRENT_STATE_TABLES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "sessions": (
        ("client", "session_id"),
        (
            "workspace",
            "workspace_label",
            "created_at",
            "last_active",
            "duration_minutes",
            "message_count",
            "cost_usd",
            "models_used",
            "session_label",
            "last_seen_at",
        ),
    ),
    "session_model_stats": (
        ("client", "session_id", "model"),
        (
            "provider",
            "input_tokens",
            "output_tokens",
            "cache_read",
            "cache_write",
            "reasoning",
            "total_tokens",
            "message_count",
            "cost_usd",
            "ms_per_1k_tokens",
            "perf_duration_ms",
            "perf_token_coverage",
            "price_input_per_token",
            "price_output_per_token",
            "price_cache_read_per_token",
            "price_cache_write_per_token",
            "price_matched_key",
            "price_match_kind",
            "price_alias_applied",
            "price_source",
        ),
    ),
    "daily_stats": (
        ("day", "client", "model"),
        (
            "provider",
            "input_tokens",
            "output_tokens",
            "cache_read",
            "cache_write",
            "reasoning",
            "message_count",
            "cost_usd",
        ),
    ),
    "daily_activity": (("day",), ("intensity", "active_time_ms")),
}

APPEND_ONLY_TABLES = frozenset(
    {
        "ingest_runs",
        "pricing_snapshots",
        "run_metrics",
        "reconciliation_issues",
        "schema_drift",
    }
)


@dataclass(frozen=True, slots=True)
class PersistSummary:
    """Outcome of persisting one normalized collection run.

    Attributes:
        inserted: New current-state rows.
        updated: Materially changed current-state rows.
        per_table: Per-current-table upsert outcomes.
    """

    inserted: int
    updated: int
    per_table: dict[str, UpsertResult]


def _with_ingest_counts(
    ingest_runs: pa.Table,
    inserted: int,
    updated: int,
) -> pa.Table:
    """Set final current-state counts on the DDL-defined ingest audit row.

    Args:
        ingest_runs: Single-row normalized ingest-runs table.
        inserted: Total newly inserted current-state rows.
        updated: Total materially updated current-state rows.

    Returns:
        The run table with final inserted and updated counts.

    Raises:
        ValueError: If normalizer did not supply the DDL audit columns.
    """
    result = ingest_runs
    for name, value in (("rows_inserted", inserted), ("rows_updated", updated)):
        index = result.schema.get_field_index(name)
        if index < 0:
            raise ValueError(f"ingest_runs is missing required column {name!r}")
        result = result.set_column(index, name, pa.array([value], type=pa.int64()))
    return result


def persist_run(backend: StorageBackend, bundle: NormalizedBundle) -> PersistSummary:
    """Persist one normalized collection in a single backend transaction.

    Current-state tables are upserted, append-only audit/history tables are
    appended, and user-owned curation tables are deliberately not accepted.

    Args:
        backend: Destination storage backend.
        bundle: Canonical Arrow tables produced by normalizer.

    Returns:
        Counts of inserted and updated current-state rows.

    Raises:
        ValueError: If normalizer supplies an unknown or incomplete table.
    """
    supplied = frozenset(bundle.tables)
    permitted = frozenset(CURRENT_STATE_TABLES) | APPEND_ONLY_TABLES
    if unknown := supplied - permitted:
        raise ValueError(f"normalizer produced unsupported tables: {sorted(unknown)}")
    if "ingest_runs" not in bundle.tables:
        raise ValueError("normalizer must produce an ingest_runs table")

    per_table: dict[str, UpsertResult] = {}
    with backend.transaction():
        for table, (natural_keys, change_fields) in CURRENT_STATE_TABLES.items():
            data = bundle.tables.get(table)
            if data is not None:
                per_table[table] = backend.upsert(
                    table,
                    data,
                    natural_keys,
                    change_fields,
                )
        for table in APPEND_ONLY_TABLES - {"ingest_runs"}:
            data = bundle.tables.get(table)
            if data is not None:
                backend.append(table, data)
        inserted = sum(result.inserted for result in per_table.values())
        updated = sum(result.updated for result in per_table.values())
        backend.append(
            "ingest_runs",
            _with_ingest_counts(bundle.tables["ingest_runs"], inserted, updated),
        )
    return PersistSummary(inserted=inserted, updated=updated, per_table=per_table)
