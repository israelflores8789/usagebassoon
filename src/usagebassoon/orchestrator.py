# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""orchestrator.py — Sequence collection, ingest, persistence, and archival work."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from socket import gethostname
from uuid import uuid4

from usagebassoon.collection_lock import CollectionBusy, collection_lock
from usagebassoon.collector import (
    RawCollection,
    _fetch_daily_models,
    _fetch_pricing,
    _fetch_reports,
    _json_command,
    _object,
    _prefix,
)
from usagebassoon.config import UsageBassoonConfig
from usagebassoon.ingest import (
    build_collection_bundle,
    build_ingest_evidence,
    plan_graph,
    plan_models,
)
from usagebassoon.logger import LOGGER_NAME
from usagebassoon.logger import configure as configure_logging
from usagebassoon.normalizer import failed_collection, normalize
from usagebassoon.persistence import (
    PersistSummary,
    load_ingest_status,
    persist_with_retries,
)

_MODELS_DOMAIN = "models"
_PRICING_DOMAIN = "pricing"
_GRAPH_MAX_STDOUT_BYTES = 16 * 1024 * 1024
_HISTORICAL_OVERLAP_DAYS = 3
_LOG = logging.getLogger(LOGGER_NAME)


def _collect_locked(
    config: UsageBassoonConfig,
    run_id: str,
    started_at: datetime,
    logger: logging.Logger,
    refresh_range: tuple[date, date] | None = None,
) -> PersistSummary:
    """Collect and persist while the local environment lock remains held."""
    (
        statuses,
        persisted_models,
        persisted_prices,
        prior_reconciliation_issues,
        prior_schema_drift,
    ) = load_ingest_status(config)
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
        completed = {
            target for target, status in statuses.items() if status.status == "complete"
        }
        today = started_at.date()
        candidate_days = tuple(
            sorted(
                set(graph_plan.candidate_days)
                | {
                    day
                    for (day, domain), status in statuses.items()
                    if domain == _MODELS_DOMAIN and status.status == "provisional"
                }
                | {
                    day
                    for day in persisted_models
                    if day >= today - timedelta(days=_HISTORICAL_OVERLAP_DAYS)
                }
            )
        )
        if refresh_range is not None:
            since, until = refresh_range
            candidate_days = tuple(
                since + timedelta(days=offset)
                for offset in range((until - since).days + 1)
            )
            graph_plan = replace(
                graph_plan,
                graph=graph_plan.graph.model_copy(
                    update={
                        "contributions": [
                            item
                            for item in graph_plan.graph.contributions
                            if since <= item.date <= until
                        ]
                    }
                ),
            )
        graph_plan = replace(graph_plan, candidate_days=candidate_days)
        daily_days = tuple(
            day
            for day in candidate_days
            if refresh_range is not None
            or day >= today - timedelta(days=_HISTORICAL_OVERLAP_DAYS)
            or (day, _MODELS_DOMAIN) not in completed
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
    except Exception as error:
        failed = failed_collection(
            run_id=run_id,
            source_id=config.source_id,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            host=gethostname(),
            error=error,
        )
        try:
            persist_with_retries(config, failed, logger)
        except Exception:
            logger.exception("could not publish failed collection %s", run_id)
        raise
    return persist_with_retries(config, normalize(bundle), logger)


def collect(
    config: UsageBassoonConfig,
    *,
    refresh: bool = False,
    since: date | None = None,
    until: date | None = None,
) -> tuple[str, PersistSummary]:
    """Collect usage, optionally refreshing an inclusive range for this source.

    Args:
        config: Collector source and storage backend configuration.
        refresh: Bypass historical completion within the selected range.
        since: Refresh start; defaults to 29 days before the end.
        until: Refresh end; defaults to the collection's current UTC day.

    Returns:
        Run identity and backend publication counts. Recollection preserves
        absent keys and existing historical prices, while filling price gaps.

    Raises:
        ValueError: If bounds are reversed, future, or used without refresh.
    """
    started_at = datetime.now(UTC)
    refresh_range = None
    if refresh:
        end = until if until is not None else started_at.date()
        start = since if since is not None else end - timedelta(days=29)
        if start > end:
            raise ValueError("--since must be on or before --until")
        if end > started_at.date():
            raise ValueError("--until must not be after the current UTC day")
        refresh_range = (start, end)
    elif since is not None or until is not None:
        raise ValueError("date bounds require refresh=True")
    try:
        logger = configure_logging(config.logging)
    except Exception:
        logger = _LOG
        logger.exception("could not configure collection logging")
    run_id = str(uuid4())
    try:
        with collection_lock(config):
            summary = _collect_locked(config, run_id, started_at, logger, refresh_range)
    except CollectionBusy:
        logger.info("collection skipped because source %s is busy", config.source_id)
        raise
    except Exception:
        logger.exception("collection cycle failed before completion")
        raise
    return run_id, summary
