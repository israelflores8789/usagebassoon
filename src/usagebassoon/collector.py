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
from typing import cast

from usagebassoon.config import TOKSCALE_CLEANUP_TIMEOUT_SECONDS, UsageBassoonConfig
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


def _capture_process(
    process: subprocess.Popen[bytes],
    *,
    timeout_seconds: float,
    max_stdout_bytes: int,
    max_stderr_bytes: int,
) -> tuple[bytes, bytes]:
    """Capture output within a command deadline and a fixed cleanup allowance.

    Nonblocking reads keep pipe ownership in this thread, including when a
    surviving descendant holds a writer open. Cleanup drains and reaps only
    within its shared deadline; incomplete or unreadable output always fails.
    """
    if process.stdout is None or process.stderr is None:
        raise RuntimeError("tokscale subprocess pipes were not configured")
    pipes = {"stdout": process.stdout, "stderr": process.stderr}
    outputs = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {"stdout": max_stdout_bytes, "stderr": max_stderr_bytes}
    complete: set[str] = set()
    deadline = time.monotonic() + timeout_seconds
    cleanup_deadline: float | None = None
    failure: RuntimeError | None = None
    try:
        for pipe in pipes.values():
            os.set_blocking(pipe.fileno(), False)
        while len(complete) != 2 or process.poll() is None:
            now = time.monotonic()
            if cleanup_deadline is not None:
                if now >= cleanup_deadline:
                    break
            elif now >= deadline and failure is None:
                failure = RuntimeError(
                    f"tokscale exceeded {timeout_seconds:.0f} seconds; "
                    "termination requested"
                )
            if failure is not None and cleanup_deadline is None:
                cleanup_deadline = now + TOKSCALE_CLEANUP_TIMEOUT_SECONDS
                _terminate_process(process)
            progressed = False
            for name, pipe in pipes.items():
                if name in complete:
                    continue
                try:
                    chunk = os.read(pipe.fileno(), 64 * 1024)
                except BlockingIOError:
                    continue
                except OSError as error:
                    _LOG.exception("could not read tokscale %s", name)
                    complete.add(name)
                    if failure is None:
                        failure = RuntimeError(f"could not read tokscale {name}")
                        failure.__cause__ = error
                    continue
                progressed = True
                if not chunk:
                    complete.add(name)
                    continue
                output = outputs[name]
                available = limits[name] - len(output)
                if available > 0:
                    output.extend(chunk[:available])
                if len(chunk) > available and failure is None:
                    failure = RuntimeError(
                        f"tokscale {name} exceeded its {limits[name]} byte limit; "
                        "termination requested"
                    )
            if not progressed:
                active_deadline = cleanup_deadline or deadline
                time.sleep(max(0.0, min(0.01, active_deadline - time.monotonic())))
    except BaseException:
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + TOKSCALE_CLEANUP_TIMEOUT_SECONDS
            _terminate_process(process)
        raise
    finally:
        if failure is not None and cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + TOKSCALE_CLEANUP_TIMEOUT_SECONDS
            _terminate_process(process)
        for name, pipe in pipes.items():
            try:
                pipe.close()
            except OSError:
                _LOG.exception("could not close tokscale %s", name)
        try:
            process.wait(
                timeout=max(0.0, (cleanup_deadline or deadline) - time.monotonic())
            )
        except subprocess.TimeoutExpired:
            _LOG.warning(
                "tokscale process %s did not exit before cleanup deadline",
                process.pid,
                exc_info=True,
            )
            if failure is None:
                failure = RuntimeError("tokscale did not exit before its deadline")
        except OSError:
            _LOG.exception("could not reap tokscale process %s", process.pid)
            if failure is None:
                failure = RuntimeError("could not reap tokscale process")
    if failure is not None:
        raise failure
    return bytes(outputs["stdout"]), bytes(outputs["stderr"])


def _json_command(
    config: UsageBassoonConfig,
    prefix: Sequence[str],
    *arguments: str,
    max_stdout_bytes: int | None = None,
) -> JsonValue:
    """Run one bounded tokscale JSON command and decode its standard output."""
    command = [*prefix, "--no-spinner", *arguments]
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
        timeout_seconds=config.tokscale_timeout_seconds,
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
            ),
            f"models --since {day.isoformat()} --until {day.isoformat()}",
        )
    return payloads


def _fetch_pricing(
    config: UsageBassoonConfig,
    prefix: Sequence[str],
    models_by_day: dict[date, set[str]],
    logger: logging.Logger,
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
                ),
                f"report --since {day.isoformat()} --until {day.isoformat()}",
            )
        except Exception:
            failures.add(day)
            active_logger.exception("session report collection failed for %s", day)
        else:
            reports[day] = report
    return reports, frozenset(failures)
