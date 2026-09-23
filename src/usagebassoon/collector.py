# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""collector.py — Tokscale subprocess collection for one ingest cycle."""

from __future__ import annotations

import json
import logging
import os
import shlex
import shutil
import signal
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from queue import Empty, Queue
from threading import Thread
from typing import BinaryIO, cast

from usagebassoon.config import UsageBassoonConfig
from usagebassoon.display import sanitize_display
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.logger import LOGGER_NAME

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


@dataclass(frozen=True, slots=True)
class RawCollection:
    """Raw JSON payloads collected from tokscale for one collection cycle.

    This type deliberately contains only source payloads. Planning evidence,
    contract results, and parsed data belong to ingest and orchestration.
    """

    graph: JsonObject
    daily_models: Mapping[date, JsonObject]
    report_by_day: Mapping[date, JsonArray]
    pricing_by_day: Mapping[date, Mapping[str, JsonObject]]


def resolve_tokscale_command(config: UsageBassoonConfig) -> tuple[str, ...]:
    """Resolve the complete tokscale executable command prefix.

    Args:
        config: Active UsageBassoon configuration.

    Returns:
        Command tokens ending in the tokscale executable or package spec.

    Raises:
        RuntimeError: If a configured command cannot be tokenized.
    """
    override = config.tokscale_bin or os.environ.get("TOKSCALE_BIN")
    if override:
        try:
            command = tuple(shlex.split(override))
        except ValueError as error:
            raise RuntimeError("TOKSCALE_BIN is not a valid command line") from error
        if not command:
            raise RuntimeError("TOKSCALE_BIN did not contain an executable")
        return command
    if shutil.which("tokscale"):
        return ("tokscale",)
    return ("bunx", "tokscale@latest")


def _prefix(config: UsageBassoonConfig) -> list[str]:
    """Return the tokscale command as a mutable argv for collection helpers."""
    return list(resolve_tokscale_command(config))


def preflight_tokscale(config: UsageBassoonConfig) -> tuple[tuple[str, ...], str]:
    """Verify the effective tokscale command and read its reported version.

    Args:
        config: Active UsageBassoon configuration.

    Returns:
        The command prefix used by the collector and the reported version.

    Raises:
        RuntimeError: If tokscale cannot be started, times out, or rejects the
            version probe.
    """
    command = resolve_tokscale_command(config)
    try:
        process = subprocess.Popen(
            [*command, "--version"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_child_environment(config),
            shell=False,
            start_new_session=os.name == "posix",
        )
        stdout, stderr = _capture_process(
            process,
            timeout_seconds=config.tokscale_timeout_seconds,
            max_stdout_bytes=config.tokscale_max_stdout_bytes,
            max_stderr_bytes=config.tokscale_max_stderr_bytes,
        )
    except FileNotFoundError as error:
        if command == ("bunx", "tokscale@latest"):
            raise RuntimeError(
                "tokscale was not found on PATH and the bunx fallback is "
                "unavailable; install tokscale or configure [tokscale].bin"
            ) from error
        raise RuntimeError(
            "could not start tokscale preflight; executable "
            f"{command[0]!r} was not found"
        ) from error
    except RuntimeError as error:
        raise RuntimeError(f"tokscale preflight failed: {error}") from error
    except OSError as error:
        raise RuntimeError(f"could not start tokscale preflight: {error}") from error
    if process.returncode != 0:
        detail = sanitize_display(stderr.decode("utf-8", errors="replace").strip())
        raise RuntimeError(
            "tokscale preflight failed: "
            f"{detail or 'tokscale exited without diagnostics'}"
        )
    output = stdout.decode("utf-8", errors="replace").strip()
    if not output:
        raise RuntimeError(
            "tokscale preflight failed: version probe returned no output"
        )
    version = output.splitlines()[0].strip()
    if version.lower().startswith("tokscale "):
        version = version.partition(" ")[2].strip()
    if not version:
        raise RuntimeError(
            "tokscale preflight failed: version probe returned no version"
        )
    return command, version


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
        raise RuntimeError("collection timeout elapsed before tokscale completed")
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
