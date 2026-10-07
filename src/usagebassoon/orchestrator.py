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
        report_inventory,
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
                    if domain == _MODELS_DOMAIN and status.status != "complete"
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
        models_failures: dict[date, str] = {}
        daily_models = _fetch_daily_models(
            config, prefix, daily_days, failures=models_failures
        )
        models_plan = plan_models(daily_models, failures=models_failures)
        active_models = {
            model for models in models_plan.models_by_day.values() for model in models
        }
        known_prices = {
            model for models in persisted_prices.values() for model in models
        }
        unpriced_models = {
            model for models in persisted_models.values() for model in models
        } - known_prices
        observation_day = datetime.now(UTC).date()
        needed_models = active_models | unpriced_models
        pricing_requests = {
            observation_day: needed_models
            - persisted_prices.get(observation_day, set())
        }
        pricing_by_day, pricing_failures = _fetch_pricing(
            config, prefix, pricing_requests, logger, existing=persisted_prices
        )
        active_sessions = {
            (row.stats.client, row.stats.session_id)
            for payload in models_plan.daily_models.values()
            for row in payload.entries
        }
        missing_sessions = (
            active_sessions - report_inventory.sessions.keys()
        ) | report_inventory.missing
        discovered = any(
            domain == "report_inventory" and status.status == "complete"
            for (_day, domain), status in statuses.items()
        )
        unknown_creation = (
            {
                identity
                for identity in active_sessions
                if identity in report_inventory.sessions
                and report_inventory.sessions[identity] is None
            }
            if refresh_range is not None
            else set()
        )
        full_report = not discovered or bool(missing_sessions | unknown_creation)
        report_days = tuple(
            sorted(
                {
                    created.date()
                    for identity, created in report_inventory.sessions.items()
                    if refresh_range is not None
                    and identity in active_sessions
                    and created is not None
                }
                | {
                    day
                    for (day, domain), status in statuses.items()
                    if domain == "report" and status.status != "complete"
                }
            )
        )
        report_by_day, report_fetch_failures = _fetch_reports(
            config, prefix, report_days, logger=logger, all_history=full_report
        )
        expected_prices = {
            day: frozenset(set(prices) | set(pricing_failures.get(day, ())))
            for day, prices in pricing_by_day.items()
        }
        for day, failed_models in pricing_failures.items():
            expected_prices[day] = expected_prices.get(day, frozenset()) | failed_models
        already_observed = needed_models & persisted_prices.get(observation_day, set())
        if already_observed:
            expected_prices[observation_day] = (
                expected_prices.get(observation_day, frozenset()) | already_observed
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
            pricing_days=frozenset(expected_prices),
            persisted_models=persisted_models,
            persisted_prices=persisted_prices,
            report_fetch_failures=report_fetch_failures,
            pricing_fetch_failures=pricing_failures,
            prior_statuses=statuses,
            prior_reconciliation_issues=prior_reconciliation_issues,
            prior_schema_drift=prior_schema_drift,
            report_days=frozenset({date.min} if full_report else report_days),
            report_inventory=full_report,
            expected_sessions=frozenset(
                missing_sessions
                | unknown_creation
                | {
                    identity
                    for identity, created in report_inventory.sessions.items()
                    if created is not None
                    and created.date() in report_days
                    and identity in active_sessions
                }
            ),
        )
        evidence = replace(evidence, pricing_expected_models=expected_prices)
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
    summary = persist_with_retries(config, normalize(bundle), logger)
    incomplete = tuple(
        status
        for status in bundle.ingest_status
        if status.status not in {"complete", "provisional"}
    )
    if incomplete:
        logger.warning(
            "collection run %s persisted with %s incomplete targets",
            run_id,
            len(incomplete),
        )
    return replace(summary, incomplete_targets=incomplete)


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
        logger = configure_logging(config)
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
