# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""orchestrator.py — Sequence collection, ingest, persistence, and archival work."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from socket import gethostname
from uuid import uuid4

from usagebassoon.archiver import SnapshotArchiver
from usagebassoon.backends.base import StorageBackend, close_backend
from usagebassoon.collector import (
    RawCollection,
    _fetch_daily_models,
    _fetch_pricing,
    _fetch_reports,
    _json_command,
    _object,
    _prefix,
)
from usagebassoon.config import UsageBassoonConfig, open_backend
from usagebassoon.ingest import (
    build_collection_bundle,
    build_ingest_evidence,
    plan_graph,
    plan_models,
)
from usagebassoon.logger import LOGGER_NAME
from usagebassoon.logger import configure as configure_logging
from usagebassoon.normalizer import normalize
from usagebassoon.persistence import (
    PersistSummary,
    load_ingest_status,
    persist_with_retries,
)

_MODELS_DOMAIN = "models"
_PRICING_DOMAIN = "pricing"
_GRAPH_MAX_STDOUT_BYTES = 16 * 1024 * 1024
_LOG = logging.getLogger(LOGGER_NAME)


def _snapshot_after_collect(
    config: UsageBassoonConfig,
    run_id: str,
    logger: logging.Logger,
) -> None:
    """Attempt a due optional archive without invalidating persisted usage data."""
    settings = config.snapshots
    if settings is None or settings.interval is None:
        return
    backend: StorageBackend | None = None
    try:
        backend = open_backend(config)
        SnapshotArchiver.from_config(config).write(backend, run_id=run_id)
    except Exception:
        logger.exception("snapshot after collection run %s failed", run_id)
    finally:
        if backend is not None:
            close_backend(
                backend,
                context=f"snapshot after collection run {run_id}",
                logger=logger,
            )


def collect(config: UsageBassoonConfig) -> tuple[str, PersistSummary]:
    """Run one complete loss-tolerant tokscale collection and persistence cycle."""
    try:
        logger = configure_logging(config.logging)
    except Exception:
        logger = _LOG
        logger.exception("could not configure collection logging")
    started_at = datetime.now(UTC)
    run_id = str(uuid4())
    try:
        prefix = _prefix(config)
        graph_raw = _object(
            _json_command(
                config,
                prefix,
                "graph",
                max_stdout_bytes=_GRAPH_MAX_STDOUT_BYTES,
            ),
            "graph",
        )
        graph_plan = plan_graph(graph_raw)
        (
            statuses,
            persisted_models,
            persisted_prices,
            prior_reconciliation_issues,
            prior_schema_drift,
        ) = load_ingest_status(config)
        completed = {
            target for target, status in statuses.items() if status.status == "complete"
        }
        today = datetime.now(UTC).date()
        candidate_days = graph_plan.candidate_days
        daily_days = tuple(
            day
            for day in candidate_days
            if day == today or (day, _MODELS_DOMAIN) not in completed
        )
        requested_price_days = tuple(
            day
            for day in candidate_days
            if day == today
            or day in daily_days
            or (day, _PRICING_DOMAIN) not in completed
        )
        daily_models = _fetch_daily_models(config, prefix, daily_days)
        models_plan = plan_models(daily_models)
        pricing_expected_models: dict[date, frozenset[str]] = {}
        pricing_requests: dict[date, set[str]] = {}
        for day in requested_price_days:
            if day in models_plan.models_by_day:
                models = set(models_plan.models_by_day[day])
            elif (day, _MODELS_DOMAIN) in completed:
                models = persisted_models.get(day, set())
            else:
                raise RuntimeError(
                    f"pricing for {day.isoformat()} has no completed models status"
                )
            pricing_expected_models[day] = frozenset(models)
            pricing_requests[day] = (
                models if day == today else models - persisted_prices.get(day, set())
            )
        pricing_by_day, pricing_failures = _fetch_pricing(
            config, prefix, pricing_requests, logger
        )
        report_by_day, report_fetch_failures = _fetch_reports(
            config, prefix, candidate_days, logger=logger
        )
        raw = RawCollection(
            graph=graph_raw,
            daily_models=daily_models,
            report_by_day=report_by_day,
            pricing_by_day=pricing_by_day,
        )
        evidence = build_ingest_evidence(
            graph_plan=graph_plan,
            models_plan=models_plan,
            pricing_days=frozenset(requested_price_days),
            persisted_models=persisted_models,
            persisted_prices=persisted_prices,
            report_fetch_failures=report_fetch_failures,
            pricing_fetch_failures=pricing_failures,
            prior_statuses=statuses,
            prior_reconciliation_issues=prior_reconciliation_issues,
            prior_schema_drift=prior_schema_drift,
        )
        bundle = build_collection_bundle(
            raw,
            evidence=evidence,
            run_id=run_id,
            source_id=config.source_id,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            host=gethostname(),
        )
        summary = persist_with_retries(config, normalize(bundle), logger)
        _snapshot_after_collect(config, run_id, logger)
    except Exception:
        logger.exception("collection cycle failed before completion")
        raise
    return run_id, summary
