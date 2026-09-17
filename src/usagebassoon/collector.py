# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""collector.py — Tokscale subprocess collection for one ingest cycle."""

from __future__ import annotations

import json
import logging
import os
import random
import shlex
import shutil
import subprocess
import time
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from socket import gethostname
from typing import cast
from uuid import uuid4

from usagebassoon.config import UsageBassoonConfig, open_backend
from usagebassoon.ingest import RawCollection, build_collection_bundle
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.logger import configure as configure_logging
from usagebassoon.merge import PersistSummary, persist_run
from usagebassoon.normalizer import NormalizedBundle, ProcessingTarget, normalize
from usagebassoon.parsers.daily import parse_daily
from usagebassoon.parsers.graph import parse_graph
from usagebassoon.snapshots import SnapshotStore

_DAILY_STATS_TARGET = "daily_stats"
_PRICE_VERSIONS_TARGET = "price_versions"
_MAX_TRANSACTION_RETRY_SECONDS = 30.0


def _snapshot_after_collect(
    config: UsageBassoonConfig,
    run_id: str,
    logger: logging.Logger,
) -> None:
    """Attempt a due optional snapshot without invalidating persisted usage data."""
    settings = config.snapshots
    if settings is None or settings.interval is None:
        return
    uri = settings.gcs_uri or f"file://{Path('~/.usagebassoon/snapshots').expanduser()}"
    backend = open_backend(config)
    try:
        SnapshotStore(
            uri,
            max_snapshots=settings.max_snapshots,
            interval=settings.interval,
        ).write(backend, run_id=run_id)
    except Exception:
        logger.exception("snapshot after collection run %s failed", run_id)
    finally:
        backend.close()


def _prefix(config: UsageBassoonConfig) -> list[str]:
    """Resolve the tokscale executable command prefix.

    Args:
        config: Active UsageBassoon configuration.

    Returns:
        Command tokens ending in the tokscale executable or package spec.
    """
    override = config.tokscale_bin or os.environ.get("TOKSCALE_BIN")
    if override:
        return shlex.split(override)
    if shutil.which("tokscale"):
        return ["tokscale"]
    return ["bunx", "tokscale@latest"]


def _json_command(prefix: Sequence[str], *arguments: str) -> JsonValue:
    """Run one tokscale JSON command and decode its standard output."""
    completed = subprocess.run(
        [*prefix, *arguments],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        detail = completed.stderr.strip() or "tokscale exited without diagnostics"
        raise RuntimeError(f"tokscale {' '.join(arguments)} failed: {detail}")
    try:
        return cast(JsonValue, json.loads(completed.stdout))
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"tokscale {' '.join(arguments)} did not emit valid JSON"
        ) from error


def _object(payload: JsonValue, command: str) -> JsonObject:
    """Require an object-shaped tokscale command payload."""
    if not isinstance(payload, dict):
        raise RuntimeError(f"tokscale {command} must emit a JSON object")
    return payload


def _array(payload: JsonValue, command: str) -> JsonArray:
    """Require an array-shaped tokscale command payload."""
    if not isinstance(payload, list):
        raise RuntimeError(f"tokscale {command} must emit a JSON array")
    return payload


def _source_literal(source_id: str) -> str:
    """Return a safely quoted source id for fixed internal state queries."""
    return "'" + source_id.replace("'", "''") + "'"


def _daily_state(
    config: UsageBassoonConfig,
) -> tuple[frozenset[ProcessingTarget], dict[date, set[str]]]:
    """Load processed targets and existing daily models for one source.

    Args:
        config: Active source and storage configuration.

    Returns:
        Persisted completion markers and model ids by usage day.
    """
    backend = open_backend(config)
    source = _source_literal(config.source_id)
    try:
        backend.apply_ddl()
        state_rows = backend.query(
            f"SELECT day, target FROM daily_processed_state WHERE source_id = {source}"
        ).to_pylist()
        processed: set[ProcessingTarget] = set()
        for row in state_rows:
            day = row["day"]
            target = row["target"]
            if not isinstance(day, date) or not isinstance(target, str):
                raise RuntimeError("daily_processed_state contains an invalid row")
            processed.add((day, target))
        model_rows = backend.query(
            f"SELECT DISTINCT day, model FROM daily_stats WHERE source_id = {source}"
        ).to_pylist()
        models_by_day: dict[date, set[str]] = {}
        for row in model_rows:
            day = row["day"]
            model = row["model"]
            if not isinstance(day, date) or not isinstance(model, str):
                raise RuntimeError("daily_stats contains an invalid daily model key")
            models_by_day.setdefault(day, set()).add(model)
        return frozenset(processed), models_by_day
    finally:
        backend.close()


def _persist_with_retries(
    config: UsageBassoonConfig,
    bundle: NormalizedBundle,
    logger: logging.Logger,
) -> PersistSummary:
    """Persist one normalized cycle with bounded conflict-aware retries."""
    schema_backend = open_backend(config)
    try:
        schema_backend.apply_ddl()
    finally:
        schema_backend.close()
    attempts = config.collection.max_retries + 1
    for attempt in range(1, attempts + 1):
        backend = None
        try:
            backend = open_backend(config)
            return persist_run(backend, bundle)
        except Exception as error:
            retryable = backend is not None and backend.is_retryable_error(error)
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
                backend.close()
    raise RuntimeError("collection persistence exhausted without an exception")


def _fetch_daily_models(
    prefix: Sequence[str], days: Sequence[date], logger: logging.Logger
) -> tuple[dict[date, JsonObject], set[ProcessingTarget]]:
    """Fetch daily models payloads without marking failed days complete."""
    payloads: dict[date, JsonObject] = {}
    failed: set[ProcessingTarget] = set()
    for day in days:
        try:
            payloads[day] = _object(
                _json_command(
                    prefix,
                    "models",
                    "--json",
                    "--group-by",
                    "client,session,model",
                    "--since",
                    day.isoformat(),
                    "--until",
                    day.isoformat(),
                ),
                f"models --since {day.isoformat()} --until {day.isoformat()}",
            )
        except RuntimeError:
            failed.add((day, _DAILY_STATS_TARGET))
            logger.exception("daily models collection failed for %s", day.isoformat())
    return payloads, failed


def _fetch_pricing(
    prefix: Sequence[str],
    models_by_day: dict[date, set[str]],
    logger: logging.Logger,
) -> tuple[dict[date, dict[str, JsonObject]], set[ProcessingTarget]]:
    """Fetch all rates needed for successfully available daily usage facts."""
    pricing_by_day: dict[date, dict[str, JsonObject]] = {}
    failed: set[ProcessingTarget] = set()
    for day, models in sorted(models_by_day.items()):
        prices: dict[str, JsonObject] = {}
        try:
            for model in sorted(models):
                prices[model] = _object(
                    _json_command(prefix, "pricing", model, "--json"),
                    f"pricing {model}",
                )
        except RuntimeError:
            failed.add((day, _PRICE_VERSIONS_TARGET))
            logger.exception("pricing collection failed for %s", day.isoformat())
        else:
            pricing_by_day[day] = prices
    return pricing_by_day, failed


def collect(config: UsageBassoonConfig) -> tuple[str, PersistSummary]:
    """Collect, validate, normalize, and persist one daily tokscale state.

    Graph supplies candidate days and daily activity. Date-filtered models
    supplies token facts. Completed historical targets are skipped while a
    candidate for the current UTC day is always refreshed.
    """
    logger = configure_logging(config.logging)
    started_at = datetime.now(UTC)
    try:
        prefix = _prefix(config)
        graph_raw = _object(_json_command(prefix, "graph"), "graph")
        graph = parse_graph(graph_raw)
        processed, persisted_models = _daily_state(config)
        today = datetime.now(UTC).date()
        candidate_days = tuple(
            sorted({contribution.date for contribution in graph.contributions})
        )
        daily_days = tuple(
            day
            for day in candidate_days
            if day == today or (day, _DAILY_STATS_TARGET) not in processed
        )
        requested_price_days = tuple(
            day
            for day in candidate_days
            if day == today or (day, _PRICE_VERSIONS_TARGET) not in processed
        )
        daily_models, failed_targets = _fetch_daily_models(prefix, daily_days, logger)
        pricing_models: dict[date, set[str]] = {}
        for day in requested_price_days:
            if day in daily_models:
                daily_payload = parse_daily(daily_models[day], day=day)
                pricing_models[day] = {row.stats.model for row in daily_payload.entries}
            elif (day, _DAILY_STATS_TARGET) in processed:
                pricing_models[day] = persisted_models.get(day, set())
            else:
                failed_targets.add((day, _PRICE_VERSIONS_TARGET))
        pricing_by_day, pricing_failed = _fetch_pricing(prefix, pricing_models, logger)
        failed_targets.update(pricing_failed)
        processed_targets: set[ProcessingTarget] = {
            (day, _DAILY_STATS_TARGET) for day in daily_models
        }
        processed_targets.update(
            (day, _PRICE_VERSIONS_TARGET) for day in pricing_by_day
        )
        raw = RawCollection(
            daily_models=daily_models,
            report=_array(
                _json_command(prefix, "report", "--json", "--no-summarize"), "report"
            ),
            graph=graph_raw,
            pricing_by_day=pricing_by_day,
            processed_targets=frozenset(processed_targets),
            failed_targets=frozenset(failed_targets),
        )
        run_id = str(uuid4())
        bundle = build_collection_bundle(
            raw,
            run_id=run_id,
            source_id=config.source_id,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            host=gethostname(),
        )
        summary = _persist_with_retries(config, normalize(bundle), logger)
        _snapshot_after_collect(config, run_id, logger)
    except Exception:
        logger.exception("collection cycle failed before completion")
        raise
    return run_id, summary
