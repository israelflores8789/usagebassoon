# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_scheduling.py — Tests for native scheduler artifacts and preflight."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import NoReturn

import pytest
from typer.testing import CliRunner

from usagebassoon.cli.app import app
from usagebassoon.collector import preflight_tokscale, resolve_tokscale_command
from usagebassoon.config import ConfigurationManager, UsageBassoonConfig
from usagebassoon.scheduling import (
    SchedulerAvailability,
    ScheduleStatus,
    WorkerStatus,
    _systemd_time_span,
    run_worker,
    schedule_doctor_check,
)

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configuration(
    tmp_path: Path,
    *,
    tokscale_bin: str | None = None,
) -> UsageBassoonConfig:
    """Create a minimal typed configuration for scheduler tests."""
    path = tmp_path / "config.toml"
    content = f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\ndatabase = ":memory:"\n'
    if tokscale_bin is not None:
        content += f'\n[tokscale]\nbin = "{tokscale_bin}"\n'
    path.write_text(content)
    return ConfigurationManager(path).load()


def test_systemd_time_span_supports_arbitrary_valid_intervals() -> None:
    """Translate configured minute/hour intervals to systemd seconds."""
    assert _systemd_time_span("15m") == "900s"
    assert _systemd_time_span("2h") == "7200s"


def test_tokscale_preflight_honors_configured_package_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Probe configured npm/deno-style commands without assuming Bun."""
    configuration = _configuration(tmp_path, tokscale_bin="npx tokscale@latest")
    calls: list[list[str]] = []

    class FakeProcess:
        """Small bounded-pipe process double for the version probe."""

        pid = 12345
        returncode = 0
        stdout = BytesIO(b"tokscale 4.15.1\n")
        stderr = BytesIO(b"")

        def poll(self) -> int:
            return self.returncode

        def wait(self) -> int:
            return self.returncode

    def fake_popen(command: list[str], **kwargs: object) -> FakeProcess:
        calls.append(command)
        return FakeProcess()

    monkeypatch.setattr("usagebassoon.collector.subprocess.Popen", fake_popen)

    assert resolve_tokscale_command(configuration) == ("npx", "tokscale@latest")
    assert preflight_tokscale(configuration) == ("npx", "tokscale@latest")
    assert calls == [["npx", "tokscale@latest", "--version"]]


def test_tokscale_preflight_reports_missing_default_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explain how to fix an unavailable default tokscale resolution."""
    configuration = _configuration(tmp_path)

    def missing_popen(*_args: object, **_kwargs: object) -> None:
        raise FileNotFoundError("bunx")

    def no_executable(_name: str) -> str | None:
        return None

    monkeypatch.setattr("usagebassoon.collector.shutil.which", no_executable)
    monkeypatch.setattr("usagebassoon.collector.subprocess.Popen", missing_popen)

    with pytest.raises(RuntimeError, match="install tokscale"):
        preflight_tokscale(configuration)


def test_doctor_surfaces_missing_native_scheduler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recommend remote persistence when native scheduling is unavailable."""
    configuration = _configuration(tmp_path)
    monkeypatch.setattr(
        "usagebassoon.scheduling.scheduler_availability",
        lambda: SchedulerAvailability(
            "linux",
            "systemd",
            False,
            "systemd user scheduling is unavailable",
        ),
    )

    check = schedule_doctor_check(configuration)

    assert check.name == "scheduling"
    assert check.status == "warning"
    assert "systemd user scheduling is unavailable" in check.message


def test_missing_scheduler_warning_directs_users_to_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Point container users at the foreground scheduler explicitly."""
    configuration = _configuration(tmp_path)
    monkeypatch.setattr(
        "usagebassoon.scheduling.scheduler_availability",
        lambda: SchedulerAvailability(
            "linux",
            "systemd",
            False,
            "Warning: systemd was not found; run `bassoon schedule worker`",
        ),
    )

    check = schedule_doctor_check(configuration)

    assert "bassoon schedule worker" in check.message


def test_worker_interval_is_persisted_before_the_first_cycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Make the container worker's interval behavior match native install."""
    configuration = _configuration(tmp_path)

    def stop_worker(_config: UsageBassoonConfig) -> NoReturn:
        raise KeyboardInterrupt

    def fake_preflight(_config: UsageBassoonConfig) -> tuple[str, ...]:
        return ()

    def fake_logging(_logging: object) -> None:
        return None

    monkeypatch.setattr(
        "usagebassoon.scheduling.preflight_tokscale",
        fake_preflight,
    )
    monkeypatch.setattr(
        "usagebassoon.scheduling.configure_logging",
        fake_logging,
    )
    monkeypatch.setattr("usagebassoon.scheduling.collect_run", stop_worker)

    with pytest.raises(KeyboardInterrupt):
        run_worker(configuration.path, schedule_interval="30m")

    assert ConfigurationManager(configuration.path).load().schedule.interval == "30m"


def test_worker_rejects_invalid_interval_argument(tmp_path: Path) -> None:
    """Report unsupported day-granularity worker intervals before startup."""
    configuration = _configuration(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "schedule",
            "worker",
            "--config",
            str(configuration.path),
            "--interval",
            "1d",
        ],
    )

    assert result.exit_code == 1
    assert "minutes or hours" in result.output


def test_worker_cli_starts_detached_process_and_reports_pid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return promptly while reporting the detached worker's runtime details."""
    configuration = _configuration(tmp_path)
    started = WorkerStatus(
        pid=12345,
        running=True,
        interval="30m",
        pid_path=tmp_path / "worker.pid",
        log_path=tmp_path / "worker.log",
    )
    calls: list[str | None] = []

    def fake_start(
        _config: Path | None = None,
        *,
        schedule_interval: str | None = None,
    ) -> WorkerStatus:
        calls.append(schedule_interval)
        return started

    monkeypatch.setattr("usagebassoon.cli.schedule.start_worker", fake_start)

    result = CliRunner().invoke(
        app,
        [
            "schedule",
            "worker",
            "--config",
            str(configuration.path),
            "--interval",
            "30m",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == ["30m"]
    assert "background" in result.output
    assert "12345" in result.output
    assert str(started.log_path) in result.output


def test_schedule_install_persists_explicit_interval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist --interval before handing the schedule to its provider."""
    configuration = _configuration(tmp_path)
    status = ScheduleStatus(
        platform="linux",
        provider="systemd",
        installed=True,
        active=True,
        enabled=True,
        interval="30m",
        artifact=tmp_path / "usagebassoon.timer",
        log_path="journalctl --user -u usagebassoon.service",
        command=("bassoon", "collect"),
    )

    def fake_preflight(_config: UsageBassoonConfig) -> tuple[str, ...]:
        return ("tokscale",)

    def fake_install(
        _config: UsageBassoonConfig,
        *,
        no_linger: bool = False,
    ) -> ScheduleStatus:
        del no_linger
        return status

    monkeypatch.setattr(
        "usagebassoon.cli.schedule.scheduler_availability",
        lambda: SchedulerAvailability("linux", "systemd", True, "ready"),
    )
    monkeypatch.setattr(
        "usagebassoon.cli.schedule.preflight_tokscale",
        fake_preflight,
    )
    monkeypatch.setattr(
        "usagebassoon.cli.schedule.install_native_schedule",
        fake_install,
    )

    result = CliRunner().invoke(
        app,
        [
            "schedule",
            "install",
            "--config",
            str(configuration.path),
            "--interval",
            "30m",
        ],
    )

    assert result.exit_code == 0, result.output
    assert ConfigurationManager(configuration.path).load().schedule.interval == "30m"
