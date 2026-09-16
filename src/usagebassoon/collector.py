# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""collector.py — Tokscale subprocess collection for one ingest cycle."""

from __future__ import annotations

import json
import logging
import os
import shlex
import shutil
import subprocess
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from socket import gethostname
from typing import cast
from uuid import uuid4

from usagebassoon.config import UsageBassoonConfig
from usagebassoon.ingest import RawCollection, build_collection_bundle
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.logger import configure as configure_logging
from usagebassoon.merge import PersistSummary, persist_run
from usagebassoon.normalizer import NormalizedBundle, normalize
from usagebassoon.parsers.models import parse_models


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
    """Run one tokscale JSON command and decode its standard output.

    Args:
        prefix: Resolved tokscale command prefix.
        arguments: Command-specific tokscale arguments.

    Returns:
        Decoded JSON payload.

    Raises:
        RuntimeError: If tokscale fails or does not emit valid JSON.
    """
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
    """Require an object-shaped tokscale command payload.

    Args:
        payload: Decoded tokscale JSON value.
        command: Display name of the producing command.

    Returns:
        JSON object payload.

    Raises:
        RuntimeError: If tokscale emitted an unexpected top-level shape.
    """
    if not isinstance(payload, dict):
        raise RuntimeError(f"tokscale {command} must emit a JSON object")
    return payload


def _array(payload: JsonValue, command: str) -> JsonArray:
    """Require an array-shaped tokscale command payload.

    Args:
        payload: Decoded tokscale JSON value.
        command: Display name of the producing command.

    Returns:
        JSON array payload.

    Raises:
        RuntimeError: If tokscale emitted an unexpected top-level shape.
    """
    if not isinstance(payload, list):
        raise RuntimeError(f"tokscale {command} must emit a JSON array")
    return payload


def _persist_with_retries(
    config: UsageBassoonConfig,
    bundle: NormalizedBundle,
    logger: logging.Logger,
) -> PersistSummary:
    """Persist one normalized cycle with bounded transient-failure retries.

    Args:
        config: Active collection retry settings and backend configuration.
        bundle: One immutable normalized run reused across every retry.
        logger: Local operational logger for failures safe to diagnose later.

    Returns:
        The successful atomic persistence outcome.

    Raises:
        Exception: The final backend exception after retry exhaustion.
    """
    from usagebassoon.config import open_backend

    attempts = config.collection.max_retries + 1
    for attempt in range(1, attempts + 1):
        backend = None
        try:
            backend = open_backend(config)
            backend.apply_ddl()
            return persist_run(backend, bundle)
        except Exception:
            if attempt == attempts:
                logger.exception(
                    "collection run %s failed after %s attempts",
                    bundle.run_id,
                    attempts,
                )
                raise
            delay = config.collection.retry_initial_seconds * (2 ** (attempt - 1))
            logger.exception(
                "collection run %s failed on attempt %s of %s; retrying in %.1fs",
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


def collect(config: UsageBassoonConfig) -> tuple[str, PersistSummary]:
    """Collect, validate, normalize, and persist one cumulative tokscale state.

    Args:
        config: Active configuration and source identity.

    Returns:
        Generated ingest run id and persistence outcome.
    """
    logger = configure_logging(config.logging)
    started_at = datetime.now(UTC)
    try:
        prefix = _prefix(config)
        models_raw = _object(
            _json_command(
                prefix,
                "models",
                "--json",
                "--group-by",
                "client,session,model",
                "--merge-worktrees",
            ),
            "models",
        )
        models = parse_models(models_raw)
        pricing: dict[str, JsonObject] = {
            model: _object(
                _json_command(prefix, "pricing", model, "--json"),
                "pricing",
            )
            for model in sorted({entry.model for entry in models.entries})
        }
        raw = RawCollection(
            models=models_raw,
            report=_array(
                _json_command(prefix, "report", "--json", "--no-summarize"),
                "report",
            ),
            graph=_object(_json_command(prefix, "graph"), "graph"),
            pricing=pricing,
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
    except Exception:
        logger.exception("collection cycle failed before completion")
        raise
    return run_id, summary
