# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""collector.py — Tokscale subprocess collection for one ingest cycle."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
from collections.abc import Sequence
from datetime import UTC, datetime
from socket import gethostname
from typing import cast
from uuid import uuid4

from usagebassoon.config import UsageBassoonConfig
from usagebassoon.ingest import RawCollection, build_collection_bundle
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.merge import PersistSummary, persist_run
from usagebassoon.normalizer import normalize
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


def collect(config: UsageBassoonConfig) -> tuple[str, PersistSummary]:
    """Collect, validate, normalize, and persist one cumulative tokscale state.

    Args:
        config: Active configuration and source identity.

    Returns:
        Generated ingest run id and persistence outcome.
    """
    from usagebassoon.config import open_backend

    started_at = datetime.now(UTC)
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
        model: _object(_json_command(prefix, "pricing", model, "--json"), "pricing")
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
    backend = open_backend(config)
    try:
        backend.apply_ddl()
        summary = persist_run(backend, normalize(bundle))
    finally:
        backend.close()
    return run_id, summary
