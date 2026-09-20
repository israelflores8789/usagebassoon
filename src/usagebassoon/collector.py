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
import signal
import subprocess
import time
from collections.abc import Sequence
from datetime import UTC, date, datetime
from queue import Empty, Queue
from socket import gethostname
from threading import Thread
from typing import BinaryIO, cast
from uuid import uuid4

from usagebassoon.backends.base import StorageBackend, close_backend
from usagebassoon.config import UsageBassoonConfig, open_backend
from usagebassoon.display import sanitize_display
from usagebassoon.ingest import RawCollection, build_collection_bundle
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.logger import LOGGER_NAME
from usagebassoon.logger import configure as configure_logging
from usagebassoon.merge import PersistSummary, persist_run
from usagebassoon.normalizer import (
    IngestStatus,
    IngestTarget,
    NormalizedBundle,
    normalize,
)
from usagebassoon.parsers.daily import parse_daily
from usagebassoon.parsers.graph import parse_graph
from usagebassoon.snapshots import SnapshotStore

_MODELS_DOMAIN = "models"
_PRICING_DOMAIN = "pricing"
_MAX_TRANSACTION_RETRY_SECONDS = 30.0
_COLLECTION_DEADLINE_MARGIN_SECONDS = 5.0
_GRAPH_MAX_STDOUT_BYTES = 16 * 1024 * 1024
_CHILD_ENVIRONMENT_NAMES = frozenset(
    {
        "HOME",
        "LANG",
        "LANGUAGE",
        "PATH",
        "TMPDIR",
        "TOKSCALE_EXTRA_DIRS",
        "TOKSCALE_NATIVE_TIMEOUT_MS",
        "USER",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
    }
)
_LOG = logging.getLogger(LOGGER_NAME)


def _snapshot_after_collect(
    config: UsageBassoonConfig,
    run_id: str,
    logger: logging.Logger,
) -> None:
    """Attempt a due optional snapshot without invalidating persisted usage data."""
    settings = config.snapshots
    if settings is None or settings.interval is None:
        return
    backend: StorageBackend | None = None
    try:
        backend = open_backend(config)
        SnapshotStore.from_config(config).write(backend, run_id=run_id)
    except Exception:
        logger.exception("snapshot after collection run %s failed", run_id)
    finally:
        if backend is not None:
            close_backend(
                backend,
                context=f"snapshot after collection run {run_id}",
                logger=logger,
            )


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


def _child_environment(config: UsageBassoonConfig) -> dict[str, str]:
    """Build the minimal environment intentionally exposed to tokscale."""
    names = set(config.tokscale_env) | _CHILD_ENVIRONMENT_NAMES
    names.update(name for name in os.environ if name.startswith("LC_"))
    return {
        name: value for name in names if (value := os.environ.get(name)) is not None
    }


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    """Forcefully terminate a tokscale process group after a hard failure."""
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except ProcessLookupError:
        return
    except OSError:
        try:
            process.kill()
        except ProcessLookupError:
            return
        except OSError:
            _LOG.exception("could not terminate tokscale process %s", process.pid)


def _read_pipe(
    name: str,
    pipe: BinaryIO,
    queue: Queue[tuple[str, bytes | None]],
) -> None:
    """Read one process pipe in bounded chunks until it reaches EOF."""
    try:
        while chunk := pipe.read(64 * 1024):
            queue.put((name, chunk))
    except Exception:
        _LOG.exception("could not read tokscale %s", name)
    finally:
        try:
            queue.put((name, None))
        except Exception:
            _LOG.exception("could not signal tokscale %s completion", name)


def _capture_process(
    process: subprocess.Popen[bytes],
    *,
    timeout_seconds: float,
    max_stdout_bytes: int,
    max_stderr_bytes: int,
) -> tuple[bytes, bytes]:
    """Capture bounded process output and kill the process on hard limits."""
    if process.stdout is None or process.stderr is None:
        raise RuntimeError("tokscale subprocess pipes were not configured")
    events: Queue[tuple[str, bytes | None]] = Queue(maxsize=2)
    readers = [
        Thread(target=_read_pipe, args=("stdout", process.stdout, events), daemon=True),
        Thread(target=_read_pipe, args=("stderr", process.stderr, events), daemon=True),
    ]
    for reader in readers:
        reader.start()
    outputs = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {"stdout": max_stdout_bytes, "stderr": max_stderr_bytes}
    complete: set[str] = set()
    deadline = time.monotonic() + timeout_seconds
    exceeded: str | None = None
    timed_out = False
    terminated = False
    while len(complete) != 2:
        remaining = deadline - time.monotonic()
        if remaining <= 0 and not terminated:
            timed_out = True
            _terminate_process(process)
            terminated = True
            remaining = 1.0
        try:
            name, chunk = events.get(timeout=max(0.01, min(remaining, 0.1)))
        except Empty:
            continue
        if chunk is None:
            complete.add(name)
            continue
        output = outputs[name]
        available = limits[name] - len(output)
        if available > 0:
            output.extend(chunk[:available])
        if len(chunk) > available and exceeded is None:
            exceeded = name
            if not terminated:
                _terminate_process(process)
                terminated = True
    for reader in readers:
        reader.join(timeout=1.0)
    if process.poll() is None:
        _terminate_process(process)
    process.wait()
    if timed_out:
        raise RuntimeError(
            f"tokscale exceeded {timeout_seconds:.0f} seconds and was killed"
        )
    if exceeded is not None:
        raise RuntimeError(
            f"tokscale {exceeded} exceeded its {limits[exceeded]} byte limit "
            "and was killed"
        )
    return bytes(outputs["stdout"]), bytes(outputs["stderr"])


def _command_timeout(
    config: UsageBassoonConfig,
    deadline: float | None,
) -> float:
    """Return a per-command timeout bounded by the active collection deadline."""
    if deadline is None:
        return config.tokscale_timeout_seconds
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("collection cadence elapsed before tokscale completed")
    return min(config.tokscale_timeout_seconds, remaining)


def _json_command(
    config: UsageBassoonConfig,
    prefix: Sequence[str],
    *arguments: str,
    deadline: float | None = None,
    max_stdout_bytes: int | None = None,
) -> JsonValue:
    """Run one bounded tokscale JSON command and decode its standard output."""
    command = [*prefix, *arguments]
    command_name = sanitize_display(" ".join(arguments))
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_child_environment(config),
            shell=False,
            start_new_session=True,
        )
    except OSError as error:
        _LOG.exception("could not start tokscale %s", command_name)
        raise RuntimeError(f"could not start tokscale {command_name}") from error
    stdout, stderr = _capture_process(
        process,
        timeout_seconds=_command_timeout(config, deadline),
        max_stdout_bytes=min(
            config.tokscale_max_stdout_bytes,
            max_stdout_bytes or config.tokscale_max_stdout_bytes,
        ),
        max_stderr_bytes=config.tokscale_max_stderr_bytes,
    )
    if process.returncode:
        detail = sanitize_display(stderr.decode("utf-8", errors="replace").strip())
        raise RuntimeError(
            f"tokscale {command_name} failed: "
            f"{detail or 'tokscale exited without diagnostics'}"
        )
    try:
        return cast(JsonValue, json.loads(stdout.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(
            f"tokscale {command_name} did not emit valid JSON"
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


def _load_ingest_status(
    config: UsageBassoonConfig,
) -> tuple[
    dict[IngestTarget, IngestStatus], dict[date, set[str]], dict[date, set[str]]
]:
    """Load retry status plus persisted daily model and price coverage.

    Args:
        config: Active source and storage configuration.

    Returns:
        Retry-ledger rows, persisted usage models, and persisted price models.
    """
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
        price_rows = backend.query(
            f"SELECT DISTINCT day, model FROM price_versions WHERE source_id = {source}"
        ).to_pylist()
        prices_by_day: dict[date, set[str]] = {}
        for row in price_rows:
            day = row["day"]
            model = row["model"]
            if not isinstance(day, date) or not isinstance(model, str):
                raise RuntimeError("price_versions contains an invalid daily model key")
            prices_by_day.setdefault(day, set()).add(model)
        return statuses, models_by_day, prices_by_day
    finally:
        close_backend(backend, context="loading ingest status", logger=_LOG)


def _persist_with_retries(
    config: UsageBassoonConfig,
    bundle: NormalizedBundle,
    logger: logging.Logger,
) -> PersistSummary:
    """Persist one normalized cycle with bounded conflict-aware retries."""
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


def _fetch_daily_models(
    config: UsageBassoonConfig,
    prefix: Sequence[str],
    days: Sequence[date],
    *,
    deadline: float | None,
) -> dict[date, JsonObject]:
    """Fetch every required daily models payload or raise on the first failure."""
    payloads: dict[date, JsonObject] = {}
    for day in days:
        payloads[day] = _object(
            _json_command(
                config,
                prefix,
                "models",
                "--json",
                "--group-by",
                "client,session,model",
                "--since",
                day.isoformat(),
                "--until",
                day.isoformat(),
                deadline=deadline,
            ),
            f"models --since {day.isoformat()} --until {day.isoformat()}",
        )
    return payloads


def _fetch_pricing(
    config: UsageBassoonConfig,
    prefix: Sequence[str],
    models_by_day: dict[date, set[str]],
    logger: logging.Logger,
    *,
    deadline: float | None,
) -> tuple[dict[date, dict[str, JsonObject]], dict[date, frozenset[str]]]:
    """Fetch optional model prices while retaining successful per-model results."""
    pricing_by_day: dict[date, dict[str, JsonObject]] = {}
    failures: dict[date, frozenset[str]] = {}
    for day, models in sorted(models_by_day.items()):
        prices: dict[str, JsonObject] = {}
        failed_models: set[str] = set()
        for model in sorted(models):
            try:
                prices[model] = _object(
                    _json_command(
                        config,
                        prefix,
                        "pricing",
                        model,
                        "--json",
                        deadline=deadline,
                    ),
                    f"pricing {model}",
                )
            except Exception:
                failed_models.add(model)
                logger.exception(
                    "pricing collection failed for %s on %s", day.isoformat(), model
                )
        pricing_by_day[day] = prices
        if failed_models:
            failures[day] = frozenset(failed_models)
    return pricing_by_day, failures


def _fetch_reports(
    config: UsageBassoonConfig,
    prefix: Sequence[str],
    days: Sequence[date],
    *,
    deadline: float | None,
    logger: logging.Logger | None = None,
) -> tuple[dict[date, JsonArray], frozenset[date]]:
    """Fetch optional daily report payloads while preserving valid empty arrays."""
    reports: dict[date, JsonArray] = {}
    failures: set[date] = set()
    active_logger = logger or _LOG
    for day in days:
        try:
            report = _array(
                _json_command(
                    config,
                    prefix,
                    "report",
                    "--json",
                    "--no-summarize",
                    "--since",
                    day.isoformat(),
                    "--until",
                    day.isoformat(),
                    deadline=deadline,
                ),
                f"report --since {day.isoformat()} --until {day.isoformat()}",
            )
        except Exception:
            failures.add(day)
            active_logger.exception("session report collection failed for %s", day)
        else:
            reports[day] = report
    return reports, frozenset(failures)


def collect(config: UsageBassoonConfig) -> tuple[str, PersistSummary]:
    """Collect, validate, normalize, and persist one daily tokscale state.

    Graph supplies candidate days and daily activity. Date-filtered models
    supplies token facts. Completed historical targets are skipped while a
    candidate for the current UTC day is always refreshed.
    """
    try:
        logger = configure_logging(config.logging)
    except Exception:
        logger = _LOG
        logger.exception("could not configure collection logging")
    started_at = datetime.now(UTC)
    run_id = str(uuid4())
    try:
        prefix = _prefix(config)
        deadline = None
        if config.collection.cadence is not None:
            from usagebassoon.snapshots import parse_interval

            cadence = parse_interval(config.collection.cadence)
            if cadence is None:
                raise RuntimeError("collection cadence must be configured")
            deadline = time.monotonic() + max(
                0.0,
                cadence.total_seconds() - _COLLECTION_DEADLINE_MARGIN_SECONDS,
            )
        graph_raw = _object(
            _json_command(
                config,
                prefix,
                "graph",
                deadline=deadline,
                max_stdout_bytes=_GRAPH_MAX_STDOUT_BYTES,
            ),
            "graph",
        )
        graph = parse_graph(graph_raw)
        statuses, persisted_models, persisted_prices = _load_ingest_status(config)
        completed = {
            target for target, status in statuses.items() if status.status == "complete"
        }
        today = datetime.now(UTC).date()
        candidate_days = tuple(
            sorted({contribution.date for contribution in graph.contributions})
        )
        daily_days = tuple(
            day
            for day in candidate_days
            if day == today or (day, _MODELS_DOMAIN) not in completed
        )
        requested_price_days = tuple(
            day
            for day in candidate_days
            if day == today or (day, _PRICING_DOMAIN) not in completed
        )
        daily_models = _fetch_daily_models(
            config,
            prefix,
            daily_days,
            deadline=deadline,
        )
        pricing_expected_models: dict[date, frozenset[str]] = {}
        pricing_requests: dict[date, set[str]] = {}
        for day in requested_price_days:
            if day in daily_models:
                daily_payload = parse_daily(daily_models[day], day=day)
                models = {row.stats.model for row in daily_payload.entries}
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
            config,
            prefix,
            pricing_requests,
            logger,
            deadline=deadline,
        )
        report_by_day, report_fetch_failures = _fetch_reports(
            config,
            prefix,
            candidate_days,
            deadline=deadline,
            logger=logger,
        )
        raw = RawCollection(
            daily_models=daily_models,
            report_by_day=report_by_day,
            report_days=frozenset(candidate_days),
            report_fetch_failures=report_fetch_failures,
            graph=graph_raw,
            pricing_by_day=pricing_by_day,
            pricing_expected_models=pricing_expected_models,
            pricing_existing_models={
                day: frozenset(persisted_prices.get(day, set()))
                for day in requested_price_days
            },
            pricing_fetch_failures=pricing_failures,
            prior_statuses=statuses,
        )
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
