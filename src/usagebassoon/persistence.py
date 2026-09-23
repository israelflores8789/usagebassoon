# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""persistence.py — Transactional persistence of normalized collection batches."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import date

from usagebassoon.backends.base import (
    CurrentStateWrite,
    PersistenceBatch,
    StorageBackend,
    UpsertResult,
    close_backend,
)
from usagebassoon.config import UsageBassoonConfig, open_backend
from usagebassoon.ingest import IngestStatus, IngestTarget
from usagebassoon.logger import LOGGER_NAME
from usagebassoon.normalizer import NormalizedBundle

_MAX_TRANSACTION_RETRY_SECONDS = 30.0
_LOG = logging.getLogger(LOGGER_NAME)

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
            "tokscale_cost_usd",
            "models_used",
            "session_label",
            "last_seen_at",
        ),
    ),
    "daily_stats": (
        ("source_id", "day", "client", "session_id", "model"),
        (
            "provider",
            "input_tokens",
            "output_tokens",
            "cache_read",
            "cache_write",
            "reasoning",
            "total_tokens",
            "message_count",
            "tokscale_cost_usd",
        ),
    ),
    "daily_activity": (("source_id", "day"), ("intensity", "active_time_ms")),
    "price_versions": (
        ("source_id", "day", "model"),
        (
            "source",
            "matched_key",
            "match_kind",
            "price_input_per_token",
            "price_output_per_token",
            "price_cache_read_per_token",
            "price_cache_write_per_token",
            "observed_at",
        ),
    ),
    "ingest_status": (
        ("source_id", "day", "domain"),
        (
            "status",
            "expected_count",
            "succeeded_count",
            "last_attempted_run",
            "last_succeeded_run",
            "failure_code",
        ),
    ),
}

APPEND_ONLY_TABLES = frozenset(
    {
        "ingest_runs",
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


def _source_literal(source_id: str) -> str:
    """Return a safely quoted source id for fixed internal state queries."""
    return "'" + source_id.replace("'", "''") + "'"


def load_ingest_status(
    config: UsageBassoonConfig,
) -> tuple[
    dict[IngestTarget, IngestStatus], dict[date, set[str]], dict[date, set[str]]
]:
    """Load retry status plus persisted daily model and price coverage."""
    backend = open_backend(config)
    source = _source_literal(config.source_id)
    try:
        backend.apply_ddl()
        status_rows = backend.query(
            "SELECT day, domain, status, expected_count, succeeded_count, "
            "last_attempted_run, last_succeeded_run, failure_code "
            f"FROM ingest_status WHERE source_id = {source}"
        ).to_pylist()
        statuses: dict[IngestTarget, IngestStatus] = {}
        for row in status_rows:
            day = row["day"]
            domain = row["domain"]
            status = row["status"]
            attempted = row["last_attempted_run"]
            succeeded = row["last_succeeded_run"]
            failure_code = row["failure_code"]
            expected_count = row["expected_count"]
            succeeded_count = row["succeeded_count"]
            if (
                not isinstance(day, date)
                or not isinstance(domain, str)
                or not isinstance(status, str)
                or not isinstance(attempted, str)
                or (succeeded is not None and not isinstance(succeeded, str))
                or (failure_code is not None and not isinstance(failure_code, str))
                or (expected_count is not None and not isinstance(expected_count, int))
                or (
                    succeeded_count is not None and not isinstance(succeeded_count, int)
                )
            ):
                raise RuntimeError("ingest_status contains an invalid row")
            statuses[(day, domain)] = IngestStatus(
                day=day,
                domain=domain,
                status=status,
                expected_count=expected_count,
                succeeded_count=succeeded_count,
                last_attempted_run=attempted,
                last_succeeded_run=succeeded,
                failure_code=failure_code,
            )
        models_by_day: dict[date, set[str]] = {}
        for row in backend.query(
            f"SELECT DISTINCT day, model FROM daily_stats WHERE source_id = {source}"
        ).to_pylist():
            day, model = row["day"], row["model"]
            if not isinstance(day, date) or not isinstance(model, str):
                raise RuntimeError("daily_stats contains an invalid daily model key")
            models_by_day.setdefault(day, set()).add(model)
        prices_by_day: dict[date, set[str]] = {}
        for row in backend.query(
            f"SELECT DISTINCT day, model FROM price_versions WHERE source_id = {source}"
        ).to_pylist():
            day, model = row["day"], row["model"]
            if not isinstance(day, date) or not isinstance(model, str):
                raise RuntimeError("price_versions contains an invalid daily model key")
            prices_by_day.setdefault(day, set()).add(model)
        return statuses, models_by_day, prices_by_day
    finally:
        close_backend(backend, context="loading ingest status", logger=_LOG)


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


def persist_with_retries(
    config: UsageBassoonConfig,
    bundle: NormalizedBundle,
    logger: logging.Logger,
) -> PersistSummary:
    """Initialize storage and persist a batch with bounded backend retries."""
    schema_backend: StorageBackend | None = None
    try:
        schema_backend = open_backend(config)
        schema_backend.apply_ddl()
    except Exception:
        logger.exception(
            "collection run %s could not initialize the schema", bundle.run_id
        )
        raise
    finally:
        if schema_backend is not None:
            close_backend(
                schema_backend,
                context=f"schema initialization for {bundle.run_id}",
                logger=logger,
            )
    attempts = config.collection.max_retries + 1
    for attempt in range(1, attempts + 1):
        backend: StorageBackend | None = None
        try:
            backend = open_backend(config)
            return persist_run(backend, bundle)
        except Exception as error:
            try:
                retryable = backend is not None and backend.is_retryable_error(error)
            except Exception:
                logger.exception(
                    "could not classify collection run %s failure for retry",
                    bundle.run_id,
                )
                retryable = False
            if not retryable or attempt == attempts:
                logger.exception(
                    "collection run %s failed%s",
                    bundle.run_id,
                    f" after {attempts} attempts" if retryable else " without retry",
                )
                raise
            maximum_delay = min(
                _MAX_TRANSACTION_RETRY_SECONDS,
                config.collection.retry_initial_seconds * (2 ** (attempt - 1)),
            )
            delay = random.uniform(0, maximum_delay)
            logger.exception(
                "collection run %s had a retryable transaction conflict on attempt "
                "%s of %s; retrying in %.1fs",
                bundle.run_id,
                attempt,
                attempts,
                delay,
            )
            time.sleep(delay)
        finally:
            if backend is not None:
                close_backend(
                    backend,
                    context=f"persistence attempt {attempt}",
                    logger=logger,
                )
    raise RuntimeError("collection persistence exhausted without an exception")
