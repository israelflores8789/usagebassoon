# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""persistence.py — Transactional persistence of normalized collection batches."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import date, datetime

from usagebassoon.backends.base import (
    CurrentStateWrite,
    PersistenceBatch,
    SourceLeaseToken,
    StorageBackend,
    UpsertResult,
    close_backend,
)
from usagebassoon.config import UsageBassoonConfig, open_backend
from usagebassoon.drift import SchemaDriftState
from usagebassoon.ingest import IngestStatus, IngestTarget
from usagebassoon.logger import LOGGER_NAME
from usagebassoon.normalizer import NormalizedBundle
from usagebassoon.reconcile import ReconciliationIdentity

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
            "perf_duration_ms",
            "perf_timed_tokens",
            "perf_sample_count",
            "perf_token_coverage",
            "tokscale_ms_per_1k_tokens",
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
    "reconciliation_issues": (
        ("source_id", "check_name", "issue_key"),
        ("message", "updated_at", "updated_run_id", "resolved"),
    ),
    "schema_drift_events": (
        ("source_id", "domain", "tokscale_ver", "drift_key"),
        (
            "drift_kind",
            "path",
            "detail",
            "contract_tokscale_ver",
            "updated_at",
            "updated_run_id",
            "resolved",
        ),
    ),
}

APPEND_ONLY_TABLES = frozenset(
    {
        "ingest_runs",
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
    dict[IngestTarget, IngestStatus],
    dict[date, set[str]],
    dict[date, set[str]],
    frozenset[ReconciliationIdentity],
    tuple[SchemaDriftState, ...],
]:
    """Load retry status, persisted coverage, and unresolved diagnostics."""
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
        issue_rows = backend.query(
            "SELECT check_name, issue_key FROM reconciliation_issues "
            f"WHERE source_id = {source} AND resolved = FALSE"
        ).to_pylist()
        issue_identities: set[ReconciliationIdentity] = set()
        for row in issue_rows:
            check_name, issue_key = row["check_name"], row["issue_key"]
            if not isinstance(check_name, str) or not isinstance(issue_key, str):
                raise RuntimeError("reconciliation_issues contains an invalid identity")
            issue_identities.add((check_name, issue_key))
        drift_rows = backend.query(
            "SELECT domain, tokscale_ver, drift_key, drift_kind, path, detail, "
            "contract_tokscale_ver, created_at, detected_run_id, observation_count "
            "FROM schema_drift_events "
            f"WHERE source_id = {source} AND COALESCE(resolved, FALSE) = FALSE"
        ).to_pylist()
        schema_drift: list[SchemaDriftState] = []
        for row in drift_rows:
            domain = row["domain"]
            tokscale_ver = row["tokscale_ver"]
            drift_key = row["drift_key"]
            drift_kind = row["drift_kind"]
            path = row["path"]
            detail = row["detail"]
            contract_tokscale_ver = row["contract_tokscale_ver"]
            created_at = row["created_at"]
            detected_run_id = row["detected_run_id"]
            observation_count = row["observation_count"]
            if (
                not isinstance(domain, str)
                or not isinstance(tokscale_ver, str)
                or not isinstance(drift_key, str)
                or not isinstance(drift_kind, str)
                or not isinstance(path, str)
                or not isinstance(detail, str)
                or not isinstance(contract_tokscale_ver, str)
                or not isinstance(created_at, datetime)
                or not isinstance(detected_run_id, str)
                or not isinstance(observation_count, int)
            ):
                raise RuntimeError("schema_drift_events contains an invalid row")
            schema_drift.append(
                SchemaDriftState(
                    domain=domain,
                    tokscale_ver=tokscale_ver,
                    drift_key=drift_key,
                    drift_kind=drift_kind,
                    path=path,
                    detail=detail,
                    contract_tokscale_ver=contract_tokscale_ver,
                    created_at=created_at,
                    detected_run_id=detected_run_id,
                    observation_count=observation_count,
                )
            )
        return (
            statuses,
            models_by_day,
            prices_by_day,
            frozenset(issue_identities),
            tuple(schema_drift),
        )
    finally:
        close_backend(backend, context="loading ingest status", logger=_LOG)


def persist_run(
    backend: StorageBackend,
    bundle: NormalizedBundle,
    *,
    lease: SourceLeaseToken | None = None,
) -> PersistSummary:
    """Persist one normalized collection in a single backend transaction.

    Current-state tables are upserted, append-only audit/history tables are
    appended, and user-owned curation tables are deliberately not accepted.

    Args:
        backend: Destination storage backend.
        bundle: Canonical Arrow tables produced by normalizer.
        lease: Source lease already held by the collection orchestrator.

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
            lease=lease,
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
    *,
    lease: SourceLeaseToken | None = None,
) -> PersistSummary:
    """Initialize storage and persist a batch with bounded backend retries."""
    if lease is None:
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
            if lease is None:
                return persist_run(backend, bundle)
            return persist_run(backend, bundle, lease=lease)
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
