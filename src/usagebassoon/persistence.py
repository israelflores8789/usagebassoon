# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""persistence.py — Backend-specific publication of normalized collection batches."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import date, datetime

from usagebassoon.backends.base import (
    CurrentStateWrite,
    PersistenceBatch,
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
from usagebassoon.storage_model import DEBUG_TABLES, STATE_KEYS

_MAX_TRANSACTION_RETRY_SECONDS = 30.0
_LOG = logging.getLogger(LOGGER_NAME)

CURRENT_STATE_TABLES = {
    table: (keys, ("collected_at", "event_id"))
    for table, keys in STATE_KEYS.items()
    if table not in {"tags", "notes"}
}
APPEND_ONLY_TABLES = DEBUG_TABLES | {"collection_ledger"}


@dataclass(frozen=True, slots=True)
class PersistSummary:
    """Outcome of persisting one normalized collection run.

    Attributes:
        inserted: New state rows locally, appended observations in BigQuery.
        updated: Locally replaced state rows; zero for BigQuery publication.
        per_table: Backend write counts, excluding the collection ledger.
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
        planning = backend.query(
            f"SELECT * FROM collection_preflight WHERE source_id = {source}"
        ).to_pylist()
        status_rows = [row for row in planning if row["record_kind"] == "status"]
        statuses: dict[IngestTarget, IngestStatus] = {}
        for row in status_rows:
            day = row["day"]
            domain = row["domain"]
            status = row["status"]
            attempted = row["run_id"]
            failure_code = row["failure_code"]
            expected_count = row["expected_count"]
            succeeded_count = row["succeeded_count"]
            if (
                not isinstance(day, date)
                or not isinstance(domain, str)
                or not isinstance(status, str)
                or not isinstance(attempted, str)
                or (failure_code is not None and not isinstance(failure_code, str))
                or (expected_count is not None and not isinstance(expected_count, int))
                or (
                    succeeded_count is not None and not isinstance(succeeded_count, int)
                )
            ):
                raise RuntimeError("collection_status contains an invalid row")
            statuses[(day, domain)] = IngestStatus(
                day=day,
                domain=domain,
                status=status,
                expected_count=expected_count,
                succeeded_count=succeeded_count,
                run_id=attempted,
                failure_code=failure_code,
            )
        models_by_day: dict[date, set[str]] = {}
        for row in planning:
            if row["record_kind"] != "models":
                continue
            day, model = row["day"], row["model"]
            if not isinstance(day, date) or not isinstance(model, str):
                raise RuntimeError("daily_stats contains an invalid daily model key")
            models_by_day.setdefault(day, set()).add(model)
        prices_by_day: dict[date, set[str]] = {}
        for row in planning:
            if row["record_kind"] != "prices":
                continue
            day, model = row["day"], row["model"]
            if not isinstance(day, date) or not isinstance(model, str):
                raise RuntimeError("price_versions contains an invalid daily model key")
            prices_by_day.setdefault(day, set()).add(model)
        issue_rows = [row for row in planning if row["record_kind"] == "issues"]
        issue_identities: set[ReconciliationIdentity] = set()
        for row in issue_rows:
            check_name, issue_key = row["check_name"], row["issue_key"]
            if not isinstance(check_name, str) or not isinstance(issue_key, str):
                raise RuntimeError("reconciliation_issues contains an invalid identity")
            issue_identities.add((check_name, issue_key))
        drift_rows = [row for row in planning if row["record_kind"] == "drift"]
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


def persist_run(backend: StorageBackend, bundle: NormalizedBundle) -> PersistSummary:
    """Publish one run using the backend's native persistence strategy."""
    permitted = frozenset(CURRENT_STATE_TABLES) | APPEND_ONLY_TABLES
    if unknown := frozenset(bundle.tables) - permitted:
        raise ValueError(f"normalizer produced unsupported tables: {sorted(unknown)}")
    if "collection_ledger" not in bundle.tables:
        raise ValueError("normalizer must produce a collection_ledger table")
    outcome = backend.persist_batch(
        PersistenceBatch(
            run_id=bundle.run_id,
            current_state=tuple(
                CurrentStateWrite(table, data, keys, fields)
                for table, (keys, fields) in CURRENT_STATE_TABLES.items()
                if (data := bundle.tables.get(table)) is not None
            ),
            append_only={
                table: data
                for table in DEBUG_TABLES
                if (data := bundle.tables.get(table)) is not None
            },
            collection_ledger=bundle.tables["collection_ledger"],
        )
    )
    return PersistSummary(outcome.inserted, outcome.updated, dict(outcome.per_table))


def persist_with_retries(
    config: UsageBassoonConfig,
    bundle: NormalizedBundle,
    logger: logging.Logger,
) -> PersistSummary:
    """Publish a stable observation batch with bounded backend retries."""
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
            logger.warning(
                "another UsageBassoon instance may be writing to the database; "
                "collection run %s had a retryable backend error on attempt "
                "%s of %s; retrying in %.1fs",
                bundle.run_id,
                attempt,
                attempts,
                delay,
                exc_info=True,
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
