# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""scheduling.py — Native schedulers and the self-contained worker loop."""

from __future__ import annotations

import json
import os
import plistlib
import re
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Literal

from usagebassoon.collector import preflight_tokscale
from usagebassoon.config import (
    ConfigurationManager,
    UsageBassoonConfig,
    parse_interval,
    update_schedule_interval,
)
from usagebassoon.drift import DoctorCheck
from usagebassoon.logger import configure as configure_logging
from usagebassoon.orchestrator import collect as collect_run
from usagebassoon.version import __version__

Platform = Literal["linux", "darwin"]
SYSTEMD_SERVICE_NAME = "usagebassoon.service"
SYSTEMD_TIMER_NAME = "usagebassoon.timer"
LAUNCH_LABEL = "io.github.israelflores8789.usagebassoon.collect"
_SYSTEMD_RUNNING_STATES = frozenset({"running", "degraded", "starting", "initializing"})
_PATH_SUFFIX = "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"


class SchedulingError(RuntimeError):
    """Raised when a scheduler operation cannot be completed."""


@dataclass(frozen=True, slots=True)
class SchedulerAvailability:
    """Result of checking the native scheduler for the current platform."""

    platform: str
    provider: str
    available: bool
    detail: str


@dataclass(frozen=True, slots=True)
class ScheduleStatus:
    """Read-only state returned by one native scheduler provider."""

    platform: str
    provider: str
    installed: bool
    active: bool | None
    enabled: bool | None
    interval: str
    artifact: Path
    log_path: str
    command: tuple[str, ...]
    details: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        """Return JSON-compatible status data."""
        return {
            "platform": self.platform,
            "provider": self.provider,
            "installed": self.installed,
            "active": self.active,
            "enabled": self.enabled,
            "interval": self.interval,
            "artifact": str(self.artifact),
            "log_path": self.log_path,
            "command": list(self.command),
            "details": list(self.details),
        }


@dataclass(frozen=True, slots=True)
class WorkerStatus:
    """Read-only state for the detached container worker."""

    pid: int | None
    running: bool
    interval: str
    pid_path: Path
    log_path: Path

    def as_dict(self) -> dict[str, object]:
        """Return JSON-compatible worker status data."""
        return {
            "pid": self.pid,
            "running": self.running,
            "interval": self.interval,
            "pid_path": str(self.pid_path),
            "log_path": str(self.log_path),
        }


def current_platform() -> Platform:
    """Return the supported native platform for this process."""
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "darwin":
        return "darwin"
    raise SchedulingError(
        f"unsupported platform for `bassoon schedule`: {sys.platform!r}; "
        "Windows scheduling is not supported yet"
    )


def scheduler_availability() -> SchedulerAvailability:
    """Check whether the native scheduler is usable on this platform."""
    platform = current_platform()
    if platform == "darwin":
        if shutil.which("launchctl"):
            return SchedulerAvailability("darwin", "launchd", True, "launchctl found")
        return SchedulerAvailability(
            "darwin",
            "launchd",
            False,
            _missing_scheduler_message("launchd"),
        )
    if shutil.which("systemctl") is None:
        return SchedulerAvailability(
            "linux",
            "systemd",
            False,
            _missing_scheduler_message("systemd user scheduling"),
        )
    result = _run(
        ["systemctl", "--user", "is-system-running"],
        check=False,
    )
    state = result.stdout.strip()
    if state in _SYSTEMD_RUNNING_STATES:
        return SchedulerAvailability("linux", "systemd", True, f"user manager {state}")
    detail = result.stderr.strip() or state or "systemd user manager is unavailable"
    return SchedulerAvailability(
        "linux",
        "systemd",
        False,
        f"{_missing_scheduler_message('systemd user scheduling')} ({detail})",
    )


def _missing_scheduler_message(scheduler: str) -> str:
    """Return the actionable message for an unavailable native scheduler."""
    return (
        f"Warning: {scheduler} was not found or is unavailable. If UsageBassoon is "
        "running in a container, consider MotherDuck or BigQuery for data "
        "persistence and GCS for snapshots. Run `bassoon schedule worker "
        "--foreground` as the container's main process."
    )


def _require_scheduler() -> SchedulerAvailability:
    """Require the native scheduler and return its availability details."""
    availability = scheduler_availability()
    if not availability.available:
        raise SchedulingError(availability.detail)
    return availability


def _run(
    command: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run one scheduler command without invoking a shell."""
    try:
        result = subprocess.run(
            command,
            check=check,
            capture_output=True,
            text=True,
            shell=False,
        )
    except FileNotFoundError as error:
        raise SchedulingError(
            f"scheduler executable not found: {command[0]}"
        ) from error
    except OSError as error:
        raise SchedulingError(
            f"could not run {shlex.join(command)}: {error}"
        ) from error
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise SchedulingError(
            f"{shlex.join(command)} failed" + (f": {detail}" if detail else "")
        )
    return result


def _systemd_unit_dir() -> Path:
    """Return the current user's systemd unit directory."""
    return Path.home() / ".config" / "systemd" / "user"


def _launchd_plist() -> Path:
    """Return the current user's launchd plist path."""
    return Path.home() / "Library" / "LaunchAgents" / f"{LAUNCH_LABEL}.plist"


def _launchd_log() -> Path:
    """Return the current user's launchd log path."""
    return Path.home() / "Library" / "Logs" / "usagebassoon.collect.log"


def _systemd_log() -> str:
    """Return the configured systemd log hint path."""
    return "journalctl --user -u usagebassoon.service"


def _resolve_bassoon() -> str:
    """Resolve the installed CLI executable for native schedulers."""
    for name in ("bassoon", "usagebassoon"):
        if resolved := shutil.which(name):
            return resolved
    candidate = Path(sys.argv[0]).expanduser()
    if (
        candidate.is_absolute()
        and candidate.is_file()
        and os.access(candidate, os.X_OK)
    ):
        return str(candidate)
    raise SchedulingError(
        "could not resolve the installed `bassoon` executable; install "
        "usagebassoon before installing a schedule"
    )


def _collect_command(config: UsageBassoonConfig) -> tuple[str, ...]:
    """Build the absolute collection command for a native scheduler."""
    return (
        _resolve_bassoon(),
        "collect",
        "--config",
        str(config.path.expanduser().resolve()),
    )


def _duration_seconds(interval: str) -> float:
    """Parse one validated schedule interval into seconds."""
    if re.fullmatch(r"\d+(?:\.\d+)?[mh]", interval, re.IGNORECASE) is None:
        raise SchedulingError(
            "schedule.interval must be a positive duration in minutes or hours"
        )
    duration = parse_interval(interval)
    if duration is None:
        raise SchedulingError("schedule.interval must be positive")
    return duration.total_seconds()


def _systemd_time_span(interval: str) -> str:
    """Translate a validated schedule interval to a systemd time span."""
    seconds = _duration_seconds(interval)
    return f"{seconds:g}s"


def _systemd_path() -> str:
    """Build a scheduler PATH containing common user tool locations."""
    home = Path.home()
    return f"{home / '.local' / 'bin'}:{home / '.bun' / 'bin'}{_PATH_SUFFIX}"


def _systemd_service(config: UsageBassoonConfig, command: tuple[str, ...]) -> str:
    """Render the native systemd collection service."""
    return "\n".join(
        (
            "[Unit]",
            "Description=usagebassoon tokscale collection",
            "Documentation=https://github.com/israelflores8789/usagebassoon",
            "",
            "[Service]",
            "Type=oneshot",
            "SyslogIdentifier=usagebassoon",
            f"Environment=PATH={_systemd_path()}",
            f"ExecStart={shlex.join(list(command))}",
            "Nice=10",
            "NoNewPrivileges=true",
            "",
        )
    )


def _systemd_timer(interval: str) -> str:
    """Render the elapsed-interval systemd collection timer."""
    return "\n".join(
        (
            "[Unit]",
            "Description=usagebassoon collection schedule",
            "",
            "[Timer]",
            "OnBootSec=2min",
            f"OnUnitActiveSec={_systemd_time_span(interval)}",
            "RandomizedDelaySec=60",
            f"Unit={SYSTEMD_SERVICE_NAME}",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        )
    )


def _write_atomic(path: Path, content: bytes, mode: int = 0o644) -> None:
    """Atomically write one scheduler artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, stat.S_IMODE(mode))
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def install_native_schedule(
    config: UsageBassoonConfig,
    *,
    no_linger: bool = False,
) -> ScheduleStatus:
    """Preflight and install the native schedule for the current platform."""
    availability = _require_scheduler()
    command = _collect_command(config)
    preflight_tokscale(config)
    if availability.platform == "linux":
        return _install_systemd(config, command, no_linger=no_linger)
    return _install_launchd(config, command)


def _install_systemd(
    config: UsageBassoonConfig,
    command: tuple[str, ...],
    *,
    no_linger: bool,
) -> ScheduleStatus:
    """Install and enable the systemd user service and timer."""
    unit_dir = _systemd_unit_dir()
    _write_atomic(
        unit_dir / SYSTEMD_SERVICE_NAME,
        _systemd_service(config, command).encode(),
    )
    _write_atomic(
        unit_dir / SYSTEMD_TIMER_NAME,
        _systemd_timer(config.schedule.interval).encode(),
    )
    _run(["systemctl", "--user", "daemon-reload"])
    _run(["systemctl", "--user", "enable", "--now", SYSTEMD_TIMER_NAME])
    warnings: list[str] = []
    if not no_linger:
        loginctl = shutil.which("loginctl")
        if loginctl is None:
            warnings.append("loginctl was not found; user lingering was not enabled")
        else:
            user = os.environ.get("USER") or str(os.getuid())
            result = _run([loginctl, "enable-linger", user], check=False)
            if result.returncode != 0:
                detail = result.stderr.strip() or "permission was denied"
                warnings.append(
                    "could not enable systemd lingering; the schedule may stop "
                    f"after logout ({detail})"
                )
    return _systemd_status(config, command, tuple(warnings))


def _launchd_environment() -> dict[str, str]:
    """Build the minimal PATH environment for a launchd agent."""
    home = Path.home()
    return {
        "PATH": (
            f"{home / '.local' / 'bin'}:{home / '.bun' / 'bin'}"
            ":/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
        )
    }


def _launchd_plist_bytes(
    config: UsageBassoonConfig,
    command: tuple[str, ...],
) -> bytes:
    """Render a launchd plist for one collection schedule."""
    seconds = _duration_seconds(config.schedule.interval)
    if seconds < 1 or seconds > 2**31 - 1:
        raise SchedulingError(
            "launchd schedule interval is outside its supported range"
        )
    payload: dict[str, object] = {
        "Label": LAUNCH_LABEL,
        "ProgramArguments": list(command),
        "EnvironmentVariables": _launchd_environment(),
        "StartInterval": int(seconds),
        "RunAtLoad": False,
        "ProcessType": "Background",
        "LowPriorityIO": True,
        "Nice": 10,
        "StandardOutPath": str(_launchd_log()),
        "StandardErrorPath": str(_launchd_log()),
    }
    return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=False)


def _launch_domain() -> str:
    """Return the current user's launchd GUI domain."""
    return f"gui/{os.getuid()}"


def _launchctl(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run launchctl without invoking a shell."""
    return _run(["launchctl", *arguments], check=check)


def _install_launchd(
    config: UsageBassoonConfig,
    command: tuple[str, ...],
) -> ScheduleStatus:
    """Install and bootstrap a launchd LaunchAgent."""
    plist = _launchd_plist()
    _launchd_log().parent.mkdir(parents=True, exist_ok=True)
    _write_atomic(plist, _launchd_plist_bytes(config, command))
    _launchctl("bootout", _launch_domain(), str(plist), check=False)
    _launchctl("bootstrap", _launch_domain(), str(plist))
    return _launchd_status(config, command)


def start_native_schedule(config: UsageBassoonConfig) -> ScheduleStatus:
    """Start an installed native schedule after preflight."""
    availability = _require_scheduler()
    command = _collect_command(config)
    preflight_tokscale(config)
    if availability.platform == "linux":
        if not (_systemd_unit_dir() / SYSTEMD_TIMER_NAME).is_file():
            raise SchedulingError("schedule is not installed")
        _run(["systemctl", "--user", "enable", "--now", SYSTEMD_TIMER_NAME])
        return _systemd_status(config, command)
    plist = _launchd_plist()
    if not plist.is_file():
        raise SchedulingError("schedule is not installed")
    _launchctl("bootout", _launch_domain(), str(plist), check=False)
    _launchctl("bootstrap", _launch_domain(), str(plist))
    return _launchd_status(config, command)


def stop_native_schedule() -> None:
    """Stop the scheduler and its active collection process."""
    availability = _require_scheduler()
    if availability.platform == "linux":
        _run(["systemctl", "--user", "stop", SYSTEMD_TIMER_NAME], check=False)
        _run(["systemctl", "--user", "stop", SYSTEMD_SERVICE_NAME], check=False)
        return
    _launchctl("bootout", _launch_domain(), str(_launchd_plist()), check=False)


def remove_native_schedule() -> None:
    """Stop a native schedule and remove only its generated artifacts."""
    availability = _require_scheduler()
    if availability.platform == "linux":
        _run(
            ["systemctl", "--user", "disable", "--now", SYSTEMD_TIMER_NAME],
            check=False,
        )
        unit_dir = _systemd_unit_dir()
        (unit_dir / SYSTEMD_SERVICE_NAME).unlink(missing_ok=True)
        (unit_dir / SYSTEMD_TIMER_NAME).unlink(missing_ok=True)
        _run(["systemctl", "--user", "daemon-reload"])
        return
    plist = _launchd_plist()
    _launchctl("bootout", _launch_domain(), str(plist), check=False)
    plist.unlink(missing_ok=True)


def native_schedule_status(config: UsageBassoonConfig) -> ScheduleStatus:
    """Return native scheduler state without changing it."""
    availability = scheduler_availability()
    command: tuple[str, ...]
    try:
        command = _collect_command(config)
    except SchedulingError:
        command = ()
    if availability.platform == "linux":
        return _systemd_status(
            config,
            command,
            (availability.detail,) if not availability.available else (),
        )
    return _launchd_status(
        config,
        command,
        (availability.detail,) if not availability.available else (),
    )


def schedule_doctor_check(config: UsageBassoonConfig | None) -> DoctorCheck:
    """Return read-only scheduler diagnostics for ``bassoon doctor``.

    Args:
        config: Validated configuration, or ``None`` when configuration
            validation failed before the scheduler could be inspected.

    Returns:
        One scheduler-specific diagnostic check. Scheduler availability is
        advisory because native scheduling is optional for one-shot use.
    """
    try:
        availability = scheduler_availability()
    except SchedulingError as error:
        return DoctorCheck("scheduling", "warning", str(error))
    if config is None:
        return DoctorCheck(
            "scheduling",
            "warning",
            f"{availability.provider} is {availability.detail}; configuration "
            "could not be inspected",
        )
    try:
        status = native_schedule_status(config)
    except SchedulingError as error:
        return DoctorCheck("scheduling", "warning", str(error))
    if not availability.available:
        return DoctorCheck("scheduling", "warning", availability.detail)
    if not status.installed:
        return DoctorCheck(
            "scheduling",
            "ok",
            f"{status.provider} is available; no native schedule is installed",
            (f"configured interval: {status.interval}",),
        )
    if status.active is False or status.enabled is False:
        return DoctorCheck(
            "scheduling",
            "warning",
            f"{status.provider} schedule is installed but not running",
            (f"artifact: {status.artifact}", *status.details),
        )
    return DoctorCheck(
        "scheduling",
        "ok",
        f"{status.provider} schedule is installed and active",
        (f"interval: {status.interval}", f"artifact: {status.artifact}"),
    )


def _worker_pid_path(config: UsageBassoonConfig) -> Path:
    """Return the detached worker PID-file path."""
    return config.logging.directory.expanduser() / "worker.pid"


def _worker_log_path(config: UsageBassoonConfig) -> Path:
    """Return the detached worker stdout/stderr log path."""
    return config.logging.directory.expanduser() / "worker.log"


def _read_worker_pid(path: Path) -> int | None:
    """Read a valid PID from a worker state file."""
    try:
        value = int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, OSError, ValueError):
        return None
    return value if value > 0 else None


def _pid_is_running(pid: int) -> bool:
    """Check whether a process exists without inspecting or controlling it."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def worker_status(config: UsageBassoonConfig) -> WorkerStatus:
    """Return the detached worker state for one configuration."""
    pid_path = _worker_pid_path(config)
    pid = _read_worker_pid(pid_path)
    return WorkerStatus(
        pid=pid,
        running=pid is not None and _pid_is_running(pid),
        interval=config.schedule.interval,
        pid_path=pid_path,
        log_path=_worker_log_path(config),
    )


def _worker_configuration(
    config_path: Path | None,
    schedule_interval: str | None,
) -> UsageBassoonConfig:
    """Load worker configuration and persist an optional interval override."""
    manager = ConfigurationManager(config_path)
    configuration = manager.load(schedule_interval=schedule_interval)
    if schedule_interval is not None:
        update_schedule_interval(manager.path, configuration.schedule.interval)
        configuration = manager.load()
    return configuration


def start_worker(
    config_path: Path | None = None,
    *,
    schedule_interval: str | None = None,
) -> WorkerStatus:
    """Start the detached worker and return its PID and log locations.

    Args:
        config_path: Optional configuration file to load.
        schedule_interval: Optional interval to persist before starting.

    Returns:
        The detached worker state after launch.

    Raises:
        SchedulingError: If a worker is already running or cannot be started.
    """
    configuration = _worker_configuration(config_path, schedule_interval)
    existing = worker_status(configuration)
    if existing.running:
        raise SchedulingError(
            f"schedule worker is already running (pid {existing.pid}); "
            "use `bassoon schedule stop` first"
        )
    existing.pid_path.unlink(missing_ok=True)
    preflight_tokscale(configuration)
    command = [
        _resolve_bassoon(),
        "schedule",
        "worker",
        "--foreground",
        "--config",
        str(configuration.path.expanduser().resolve()),
    ]
    configuration.logging.directory.expanduser().mkdir(parents=True, exist_ok=True)
    log_handle = _worker_log_path(configuration).open("a", encoding="utf-8")
    try:
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                shell=False,
                close_fds=True,
                start_new_session=True,
            )
        except OSError as error:
            raise SchedulingError(
                f"could not start schedule worker: {error}"
            ) from error
    finally:
        log_handle.close()
    try:
        _write_atomic(
            existing.pid_path,
            f"{process.pid}\n".encode(),
            mode=0o600,
        )
    except OSError as error:
        with suppress(OSError):
            process.terminate()
        raise SchedulingError(f"could not write worker PID file: {error}") from error
    return worker_status(configuration)


def stop_worker(config: UsageBassoonConfig) -> WorkerStatus:
    """Request graceful shutdown of the detached worker.

    Args:
        config: Validated worker configuration.

    Returns:
        Worker state after the stop request completes.

    Raises:
        SchedulingError: If the worker does not exit within the grace period.
    """
    state = worker_status(config)
    if state.pid is None or not state.running:
        state.pid_path.unlink(missing_ok=True)
        return WorkerStatus(
            pid=state.pid,
            running=False,
            interval=state.interval,
            pid_path=state.pid_path,
            log_path=state.log_path,
        )
    try:
        os.kill(state.pid, signal.SIGTERM)
    except ProcessLookupError:
        state.pid_path.unlink(missing_ok=True)
        return WorkerStatus(
            pid=state.pid,
            running=False,
            interval=state.interval,
            pid_path=state.pid_path,
            log_path=state.log_path,
        )
    deadline = time.monotonic() + 10.0
    while _pid_is_running(state.pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    if _pid_is_running(state.pid):
        raise SchedulingError(
            f"schedule worker pid {state.pid} did not stop within 10 seconds"
        )
    state.pid_path.unlink(missing_ok=True)
    return WorkerStatus(
        pid=state.pid,
        running=False,
        interval=state.interval,
        pid_path=state.pid_path,
        log_path=state.log_path,
    )


def schedule_logs(config: UsageBassoonConfig | None = None) -> str:
    """Return worker or native scheduler output."""
    if config is not None:
        worker_log = _worker_log_path(config)
        if worker_log.exists():
            return worker_log.read_text(encoding="utf-8", errors="replace")
    platform = current_platform()
    if platform == "linux":
        result = _run(
            [
                "journalctl",
                "--user",
                "-u",
                SYSTEMD_SERVICE_NAME,
                "--since",
                "today",
                "--no-pager",
            ],
            check=False,
        )
        return result.stdout or result.stderr or _systemd_log()
    log_path = _launchd_log()
    if not log_path.exists():
        return f"launchd log: {log_path} (not created yet)"
    return log_path.read_text(encoding="utf-8", errors="replace")


def run_worker(
    config_path: Path | None = None,
    *,
    schedule_interval: str | None = None,
) -> None:
    """Run the foreground self-contained container scheduler.

    Args:
        config_path: Optional configuration file to load.
        schedule_interval: Optional interval to persist before starting.
    """
    manager = ConfigurationManager(config_path)
    configuration = _worker_configuration(config_path, schedule_interval)
    preflight_tokscale(configuration)
    logger = configure_logging(configuration.logging)
    stop_requested = Event()

    def request_stop(_signum: int, _frame: object) -> None:
        """Stop future cycles without interrupting an active collection."""
        stop_requested.set()

    previous_term = signal.signal(signal.SIGTERM, request_stop)
    previous_int = signal.signal(signal.SIGINT, request_stop)
    try:
        while not stop_requested.is_set():
            try:
                run_id, summary = collect_run(configuration)
            except Exception as error:
                logger.exception("scheduled collection cycle failed")
                print(
                    f"scheduled collection failed: {error}",
                    file=sys.stderr,
                    flush=True,
                )
            else:
                print(
                    f"scheduled run {run_id}: {summary.inserted} inserted, "
                    f"{summary.updated} updated",
                    flush=True,
                )
            if stop_requested.is_set():
                break
            updated = manager.load()
            if updated != configuration:
                preflight_tokscale(updated)
                configuration = updated
            interval = parse_interval(configuration.schedule.interval)
            if interval is None:
                raise SchedulingError("schedule.interval must be positive")
            stop_requested.wait(interval.total_seconds())
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
        print("schedule worker stopped", flush=True)


def _systemd_status(
    config: UsageBassoonConfig,
    command: tuple[str, ...],
    extra_details: tuple[str, ...] = (),
) -> ScheduleStatus:
    """Read systemd service and timer state."""
    unit_dir = _systemd_unit_dir()
    timer = unit_dir / SYSTEMD_TIMER_NAME
    service = unit_dir / SYSTEMD_SERVICE_NAME
    installed = timer.is_file() and service.is_file()
    details = list(extra_details)
    active: bool | None = None
    enabled: bool | None = None
    if shutil.which("systemctl"):
        enabled_result = _run(
            ["systemctl", "--user", "is-enabled", SYSTEMD_TIMER_NAME],
            check=False,
        )
        active_result = _run(
            ["systemctl", "--user", "is-active", SYSTEMD_TIMER_NAME],
            check=False,
        )
        enabled = enabled_result.returncode == 0
        active = active_result.returncode == 0
        timer_result = _run(
            [
                "systemctl",
                "--user",
                "show",
                SYSTEMD_TIMER_NAME,
                "--property=NextElapseUSecRealtime",
                "--property=LastTriggerUSecRealtime",
            ],
            check=False,
        )
        details.extend(
            line for line in timer_result.stdout.splitlines() if line.strip()
        )
    details.append(f"service artifact: {service}")
    return ScheduleStatus(
        platform="linux",
        provider="systemd",
        installed=installed,
        active=active,
        enabled=enabled,
        interval=config.schedule.interval,
        artifact=timer,
        log_path=_systemd_log(),
        command=command,
        details=tuple(details),
    )


def _launchd_status(
    config: UsageBassoonConfig,
    command: tuple[str, ...],
    extra_details: tuple[str, ...] = (),
) -> ScheduleStatus:
    """Read launchd LaunchAgent state."""
    plist = _launchd_plist()
    installed = plist.is_file()
    active: bool | None = None
    details = list(extra_details)
    if shutil.which("launchctl"):
        result = _launchctl(
            "print",
            f"{_launch_domain()}/{LAUNCH_LABEL}",
            check=False,
        )
        active = result.returncode == 0
        if result.stdout.strip():
            details.extend(result.stdout.splitlines())
        elif result.stderr.strip():
            details.append(result.stderr.strip())
    return ScheduleStatus(
        platform="darwin",
        provider="launchd",
        installed=installed,
        active=active,
        enabled=active,
        interval=config.schedule.interval,
        artifact=plist,
        log_path=str(_launchd_log()),
        command=command,
        details=tuple(details),
    )


def status_json(status: ScheduleStatus, worker: WorkerStatus | None = None) -> str:
    """Serialize native and optional worker status as stable JSON."""
    payload = status.as_dict()
    payload["usagebassoon_version"] = __version__
    if worker is not None:
        payload["worker"] = worker.as_dict()
    return json.dumps(payload, sort_keys=True)


def human_status(status: ScheduleStatus, worker: WorkerStatus | None = None) -> str:
    """Render native and optional worker status for terminal users."""
    lines = [
        f"usagebassoon: {__version__}",
        f"provider: {status.provider}",
        f"installed: {'yes' if status.installed else 'no'}",
        f"active: {status.active if status.active is not None else 'unknown'}",
        f"enabled: {status.enabled if status.enabled is not None else 'unknown'}",
        f"interval: {status.interval}",
        f"artifact: {status.artifact}",
        f"logs: {status.log_path}",
    ]
    if status.command:
        lines.append(f"command: {shlex.join(list(status.command))}")
    lines.extend(f"detail: {detail}" for detail in status.details)
    if worker is not None:
        worker_state = "running" if worker.running else "not running"
        pid = f" (pid {worker.pid})" if worker.pid is not None else ""
        lines.extend(
            (
                f"worker: {worker_state}{pid}",
                f"worker pid: {worker.pid_path}",
                f"worker logs: {worker.log_path}",
            )
        )
    return "\n".join(lines)


def worker_command_help() -> str:
    """Return the container-worker entrypoint guidance."""
    return "Run `bassoon schedule worker --foreground` as the container's main process."
