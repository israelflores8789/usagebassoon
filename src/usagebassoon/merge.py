# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""merge.py — Persistence orchestration.

From normalized Arrow tables to a StorageBackend.
"""

from __future__ import annotations

from dataclasses import dataclass

from usagebassoon.backends.base import (
    CurrentStateWrite,
    PersistenceBatch,
    StorageBackend,
    UpsertResult,
)
from usagebassoon.normalizer import NormalizedBundle

CURRENT_STATE_TABLES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "sessions": (
        ("source_id", "client", "session_id"),
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
        ("source_id", "client", "session_id", "model"),
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
        ("source_id", "day", "client", "model"),
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
    "daily_activity": (("source_id", "day"), ("intensity", "active_time_ms")),
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

    current_state = tuple(
        CurrentStateWrite(table, data, natural_keys, change_fields)
        for table, (natural_keys, change_fields) in CURRENT_STATE_TABLES.items()
        if (data := bundle.tables.get(table)) is not None
    )
    append_only = {
        table: data
        for table in APPEND_ONLY_TABLES - {"ingest_runs"}
        if (data := bundle.tables.get(table)) is not None
    }
    outcome = backend.persist_batch(
        PersistenceBatch(
            run_id=bundle.run_id,
            current_state=current_state,
            append_only=append_only,
            ingest_runs=bundle.tables["ingest_runs"],
        )
    )
    return PersistSummary(
        inserted=outcome.inserted,
        updated=outcome.updated,
        per_table=dict(outcome.per_table),
    )
