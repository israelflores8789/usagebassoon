# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""collector.py — Tokscale subprocess collection for one ingest cycle."""

from __future__ import annotations

import errno
import json
import logging
import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import cast

from platformdirs import user_config_path

from usagebassoon.buckets.local import _catalog_lock
from usagebassoon.config import (
    TOKSCALE_CLEANUP_TIMEOUT_SECONDS,
    UsageBassoonConfig,
    default_tokscale_directory,
)
from usagebassoon.deadlines import limited
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
        "APPDATA",
        "HOME",
        "LANG",
        "LANGUAGE",
        "PATH",
        "TMPDIR",
        "TOKSCALE_CONFIG_DIR",
        "TOKSCALE_EXTRA_DIRS",
        "TOKSCALE_NATIVE_TIMEOUT_MS",
        "USER",
        "USERPROFILE",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
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


@dataclass
class _CustomPricingState:
    """Local publication checkpoint; backend prices remain authoritative."""

    present: bool | None = None
    models: set[str] = field(default_factory=set)
    pending: set[str] = field(default_factory=set)
    observed_at: datetime = field(
        default_factory=lambda: datetime.min.replace(tzinfo=UTC)
    )


def _pricing_catalog(path: Path) -> JsonObject:
    """Read an advisory local catalog, recovering missing or invalid state."""
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(value, dict)
            or type(value.get("version")) is not int
            or value["version"] != 1
            or not isinstance(value.get("targets"), dict)
        ):
            raise ValueError("invalid local pricing catalog")
        return cast(JsonObject, value)
    except FileNotFoundError:
        return {"version": 1, "targets": {}}
    except (OSError, ValueError):
        _LOG.warning(
            "local pricing state is unavailable; prices will be reconciled",
            exc_info=True,
        )
        return {"version": 1, "targets": {}}


def _read_pricing_state(path: Path, target: str) -> _CustomPricingState:
    """Decode the selected destination's custom-pricing presence and retry set."""
    targets = cast(JsonObject, _pricing_catalog(path)["targets"])
    if target not in targets:
        return _CustomPricingState()
    try:
        value = targets[target]
        if not isinstance(value, dict):
            raise ValueError("invalid local pricing checkpoint")
        present = value.get("custom_pricing_present")
        if present is not None and not isinstance(present, bool):
            raise ValueError("invalid custom-pricing presence")
        models, pending = value.get("custom_models"), value.get("pending_models")
        if not isinstance(models, list) or not isinstance(pending, list):
            raise ValueError("invalid pricing model sets")
        if not all(isinstance(model, str) for model in [*models, *pending]):
            raise ValueError("invalid pricing model identities")
        stamp = value.get("observed_at")
        if not isinstance(stamp, str):
            raise ValueError("invalid pricing checkpoint timestamp")
        observed_at = datetime.fromisoformat(stamp)
        if observed_at.tzinfo is None:
            raise ValueError("pricing checkpoint timestamp must be timezone aware")
        return _CustomPricingState(
            present,
            set(cast(list[str], models)),
            set(cast(list[str], pending)),
            observed_at,
        )
    except ValueError:
        _LOG.warning(
            "local pricing checkpoint is invalid; prices will be reconciled",
            exc_info=True,
        )
        return _CustomPricingState()


def _write_pricing_state(path: Path, target: str, state: _CustomPricingState) -> None:
    """Atomically checkpoint control state without exposing it to tokscale."""
    with (
        limited(TOKSCALE_CLEANUP_TIMEOUT_SECONDS),
        _catalog_lock(path.with_name(".pricing-state.lock")),
    ):
        if _read_pricing_state(path, target).observed_at > state.observed_at:
            return
        catalog = _pricing_catalog(path)
        targets = cast(JsonObject, catalog["targets"])
        models: JsonArray = [model for model in sorted(state.models)]
        pending: JsonArray = [model for model in sorted(state.pending)]
        targets[target] = {
            "custom_pricing_present": state.present,
            "custom_models": models,
            "pending_models": pending,
            "observed_at": state.observed_at.isoformat(),
        }
        descriptor, name = tempfile.mkstemp(prefix=".state-", dir=path.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(catalog, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                _LOG.warning(
                    "could not clean up local pricing state staging", exc_info=True
                )


@dataclass
class TokscaleProfile:
    """Per-run configuration snapshot with durable, destination-scoped control."""

    environment: dict[str, str]
    state_path: Path
    target: str
    custom_present: bool
    state: _CustomPricingState
    refresh_models: set[str] = field(default_factory=set)
    ignored_models: set[str] = field(default_factory=set)

    def prepare_prices(self, known: set[str], recovered: set[str]) -> set[str]:
        """Record transition intent before any affected price can be published."""
        self.state.models.update(recovered)
        refresh = set(self.state.pending)
        if self.state.present != self.custom_present:
            refresh.update(known if self.custom_present else self.state.models)
        self.refresh_models = refresh
        self.state.pending.update(refresh)
        _write_pricing_state(self.state_path, self.target, self.state)
        return refresh

    def filter_prices(
        self,
        prices: dict[date, dict[str, JsonObject]],
        existing: Mapping[date, set[str]],
        needed: set[str],
    ) -> None:
        """Leave unaffected already-observed automatic prices unchanged."""
        for day, observations in prices.items():
            for model, price in list(observations.items()):
                if (
                    self.custom_present
                    and model in self.refresh_models
                    and model not in self.state.models
                    and model not in needed
                    and model in existing.get(day, set())
                    and price.get("source") != "Custom"
                ):
                    del observations[model]
                    self.ignored_models.add(model)
        self.state.pending.difference_update(self.ignored_models)
        _write_pricing_state(self.state_path, self.target, self.state)

    def acknowledge_prices(self, published: set[str], custom: set[str]) -> None:
        """Advance presence only after affected observations have been accepted."""
        self.state.pending.difference_update(published)
        self.state.models.update(custom)
        if not self.state.pending:
            self.state.present = self.custom_present
            if not self.custom_present:
                self.state.models.clear()
        _write_pricing_state(self.state_path, self.target, self.state)


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


def _tokscale_config_directory(environment: Mapping[str, str]) -> Path:
    """Resolve the user's tokscale profile using tokscale's platform rules."""
    if directory := environment.get("TOKSCALE_CONFIG_DIR"):
        return Path(directory).absolute()
    home = Path(environment.get("HOME") or Path.home())
    if sys.platform == "darwin":
        return home / ".config" / "tokscale"
    if sys.platform == "win32":
        return user_config_path("tokscale", appauthor=False, roaming=True)
    return Path(environment.get("XDG_CONFIG_HOME") or home / ".config") / "tokscale"


@contextmanager
def _tokscale_temporary_directory(
    *, prefix: str, directory: Path | None = None
) -> Generator[Path, None, None]:
    """Clean private staging directories without hiding acquisition failures."""
    temporary = tempfile.TemporaryDirectory(prefix=prefix, dir=directory)
    failed = False
    try:
        yield Path(temporary.name)
    except BaseException:
        failed = True
        raise
    finally:
        try:
            temporary.cleanup()
        except OSError:
            _LOG.exception("could not clean up a temporary tokscale directory")
            if not failed:
                raise


def _persistent_tokscale_cache(
    config: UsageBassoonConfig, source: Path, environment: Mapping[str, str]
) -> Path:
    """Seed a private source/profile cache once, retaining compacted history."""
    identity = "\0".join(
        (config.source_id, str(source.resolve()), environment.get("HOME", ""))
    )
    root = default_tokscale_directory()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    key = sha256(identity.encode()).hexdigest()
    directory = root / key
    directory.mkdir(mode=0o700, exist_ok=True)
    cache = directory / "cache"
    if cache.is_dir():
        return cache
    # Publish a complete seed; concurrent collectors may adopt the first seed.
    with _tokscale_temporary_directory(prefix=".seed-", directory=directory) as staging:
        seed = staging / "cache"
        origin = source / "cache"
        if origin.is_dir():
            shutil.copytree(origin, seed)
        else:
            seed.mkdir(mode=0o700)
        try:
            seed.rename(cache)
        except OSError as error:
            if error.errno not in {errno.EEXIST, errno.ENOTEMPTY} or not cache.is_dir():
                raise
    return cache


def _link_tokscale_cache(profile: Path, cache: Path) -> None:
    """Attach persistent caches without copying or removing their contents."""
    link = profile / "cache"
    if sys.platform == "win32":
        # Directory junctions do not require Windows symlink privileges.
        environment = dict(
            os.environ,
            USAGEBASSOON_CACHE_LINK=str(link),
            USAGEBASSOON_CACHE_TARGET=str(cache),
        )
        result = subprocess.run(
            'cmd /d /c mklink /J "%USAGEBASSOON_CACHE_LINK%" '
            '"%USAGEBASSOON_CACHE_TARGET%"',
            executable=os.environ.get("COMSPEC", "cmd.exe"),
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=TOKSCALE_CLEANUP_TIMEOUT_SECONDS,
            check=False,
        )
        if result.returncode:
            raise RuntimeError("could not attach the persistent tokscale cache")
    else:
        link.symlink_to(cache, target_is_directory=True)


@contextmanager
def tokscale_profile(
    config: UsageBassoonConfig,
    *,
    observed_at: datetime | None = None,
) -> Generator[TokscaleProfile, None, None]:
    """Copy configuration into a fresh wiki profile with persistent caches.

    Args:
        config: Source identity and configuration location for persistent state.
        observed_at: Collection start clock for ordering concurrent checkpoints.

    Yields:
        Restricted child environment and local custom-pricing control state.

    Raises:
        OSError: If configuration, persistent state, or profile cleanup fails.
        RuntimeError: If the persistent cache cannot be attached.
    """
    environment = _child_environment(config)
    source = _tokscale_config_directory(environment)
    with _tokscale_temporary_directory(prefix="usagebassoon-tokscale-") as profile:
        for filename in ("settings.json", "custom-pricing.json"):
            path = source / filename
            if path.exists():
                shutil.copyfile(path, profile / filename)
        cache = _persistent_tokscale_cache(config, source, environment)
        _link_tokscale_cache(profile, cache)
        environment["TOKSCALE_CONFIG_DIR"] = str(profile)
        target = sha256(
            repr(
                (
                    config.backend,
                    config.local_database,
                    config.motherduck,
                    config.bigquery,
                )
            ).encode()
        ).hexdigest()
        state_path = cache.parent / "state.json"
        state = _read_pricing_state(state_path, target)
        state.observed_at = observed_at or datetime.now(UTC)
        yield TokscaleProfile(
            environment,
            state_path,
            target,
            (profile / "custom-pricing.json").is_file(),
            state,
        )


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
    environment: Mapping[str, str] | None = None,
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
            env=dict(environment)
            if environment is not None
            else _child_environment(config),
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
    *,
    failures: dict[date, str] | None = None,
    environment: Mapping[str, str] | None = None,
) -> dict[date, JsonObject]:
    """Fetch daily models, retaining successful days when failures are tracked."""
    payloads: dict[date, JsonObject] = {}
    for day in days:
        try:
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
                    environment=environment,
                ),
                f"models --since {day.isoformat()} --until {day.isoformat()}",
            )
        except (OSError, RuntimeError, ValueError):
            if failures is None:
                raise
            failures[day] = "fetch"
            _LOG.exception("daily models collection failed for %s", day)
    return payloads


def _fetch_pricing(
    config: UsageBassoonConfig,
    prefix: Sequence[str],
    models_by_day: dict[date, set[str]],
    logger: logging.Logger,
    *,
    existing: Mapping[date, set[str]] | None = None,
    environment: Mapping[str, str] | None = None,
) -> tuple[dict[date, dict[str, JsonObject]], dict[date, frozenset[str]]]:
    """Fetch optional model prices while retaining successful per-model results."""
    pricing_by_day: dict[date, dict[str, JsonObject]] = {}
    failures: dict[date, frozenset[str]] = {}
    attempted: set[tuple[date, str]] = set()
    for _day, models in sorted(models_by_day.items()):
        for model in sorted(models):
            day = datetime.now(UTC).date()
            if (day, model) in attempted or (
                existing is not None and model in existing.get(day, set())
            ):
                continue
            attempted.add((day, model))
            try:
                price = _object(
                    _json_command(
                        config,
                        prefix,
                        "pricing",
                        model,
                        "--json",
                        environment=environment,
                    ),
                    f"pricing {model}",
                )
            except Exception:
                failures[day] = failures.get(day, frozenset()) | {model}
                logger.exception(
                    "pricing collection failed for %s on %s", day.isoformat(), model
                )
            else:
                observed_day = datetime.now(UTC).date()
                pricing_by_day.setdefault(observed_day, {})[model] = price
                attempted.add((observed_day, model))
    return pricing_by_day, failures


def _fetch_reports(
    config: UsageBassoonConfig,
    prefix: Sequence[str],
    days: Sequence[date],
    *,
    logger: logging.Logger | None = None,
    all_history: bool = False,
    environment: Mapping[str, str] | None = None,
) -> tuple[dict[date, JsonArray], frozenset[date]]:
    """Observe metadata once for a bounded creation-date range or all history."""
    reports: dict[date, JsonArray] = {}
    failures: set[date] = set()
    active_logger = logger or _LOG
    requested = [date.min] if all_history else sorted(set(days))
    groups = [requested] if requested else []
    for group in groups:
        start = group[0] - timedelta(days=1) if group[0] > date.min else date.min
        end = group[-1] + timedelta(days=1) if group[-1] < date.max else date.max
        try:
            report = _array(
                _json_command(
                    config,
                    prefix,
                    "report",
                    "--json",
                    "--no-summarize",
                    *(
                        ()
                        if all_history
                        else ("--since", start.isoformat(), "--until", end.isoformat())
                    ),
                    environment=environment,
                ),
                "report" if all_history else f"report --since {start} --until {end}",
            )
        except Exception:
            failures.update(group)
            active_logger.exception(
                "session report collection failed for %s through %s", start, end
            )
        else:
            reports.update({day: report for day in group})
    return reports, frozenset(failures)
