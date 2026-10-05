# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_scheduling.py — Tests for native scheduler artifacts and preflight."""

from __future__ import annotations

import json
import logging
from io import BytesIO
from pathlib import Path
from threading import Event
from typing import NoReturn, override

import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon.cli.app import app
from usagebassoon.collector import preflight_tokscale, resolve_tokscale_command
from usagebassoon.config import ConfigurationManager, LoggingConfig, UsageBassoonConfig
from usagebassoon.persistence import PersistSummary
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
    content = "\n".join(
        (
            f'source_id = "{SOURCE_ID}"',
            'backend.provider = "duckdb"',
            'backend.duckdb.database = ":memory:"',
            "[snapshots.local]",
            "enable = false",
            "",
        )
    )
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

        def __init__(self) -> None:
            self.stdout = BytesIO(b"tokscale 4.15.1\n")
            self.stderr = BytesIO(b"")

        def poll(self) -> int:
            return self.returncode

        def wait(self) -> int:
            return self.returncode

    def fake_popen(command: list[str], **_: object) -> FakeProcess:
        calls.append(command)
        return FakeProcess()

    monkeypatch.setattr("usagebassoon.collector.subprocess.Popen", fake_popen)

    assert resolve_tokscale_command(configuration) == ("npx", "tokscale@latest")
    assert preflight_tokscale(configuration) == (("npx", "tokscale@latest"), "4.15.1")
    assert calls == [["npx", "tokscale@latest", "--version"]]


def test_schedule_status_includes_usagebassoon_version(tmp_path: Path) -> None:
    """Include the installed version in both status presentation formats."""
    from usagebassoon.scheduling import human_status, status_json
    from usagebassoon.version import __version__

    status = ScheduleStatus(
        platform="linux",
        provider="systemd",
        installed=False,
        active=None,
        enabled=None,
        interval="30m",
        artifact=tmp_path / "usagebassoon.timer",
        log_path="journalctl",
        command=(),
    )

    assert f"usagebassoon: {__version__}" in human_status(status)
    assert json.loads(status_json(status))["usagebassoon_version"] == __version__


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
            "systemd user scheduling is unavailable; run bassoon schedule worker",
        ),
    )

    check = schedule_doctor_check(configuration)

    assert check.name == "scheduling"
    assert check.status == "warning"
    assert "systemd user scheduling is unavailable" in check.message
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

    def fake_logging(_logging: LoggingConfig) -> logging.Logger:
        return logging.getLogger("test_worker_interval")

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

    assert (
        ConfigurationManager(configuration.path).load().collection.schedule.interval
        == "30m"
    )


@pytest.mark.parametrize("change", ["edited", "replaced", "invalid", "deleted", "none"])
def test_worker_keeps_startup_configuration_until_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    change: str,
) -> None:
    """Keep collecting with startup settings after edits during the wait."""
    configuration = _configuration(tmp_path)
    changed_content = (
        'source_id = "22222222-2222-4222-8222-222222222222"\n'
        'backend.provider = "duckdb"\n'
        'backend.duckdb.database = "changed.duckdb"\n'
        '[collection.schedule]\ninterval = "1h"\n'
        '[tokscale]\nbin = "changed-tokscale"\n'
        "[logging]\nmax_bytes = 12345\n"
    )
    collected: list[UsageBassoonConfig] = []
    preflighted: list[UsageBassoonConfig] = []
    waits: list[float | None] = []
    logger = logging.getLogger("test_worker_configuration")

    class CycleEvent(Event):
        """Change the file during the first wait and stop after three cycles."""

        @override
        def wait(self, timeout: float | None = None) -> bool:
            waits.append(timeout)
            if len(waits) == 1:
                if change == "edited":
                    configuration.path.write_text(changed_content)
                elif change == "replaced":
                    replacement = tmp_path / "replacement.toml"
                    replacement.write_text(changed_content)
                    replacement.replace(configuration.path)
                elif change == "invalid":
                    configuration.path.write_text("invalid TOML = [")
                elif change == "deleted":
                    configuration.path.unlink()
            if len(waits) == 3:
                self.set()
            return self.is_set()

    def fake_collect(config: UsageBassoonConfig) -> tuple[str, PersistSummary]:
        collected.append(config)
        if len(collected) > 1 and change != "none":
            assert "restart the worker" in caplog.text
        return "run", PersistSummary(0, 0, {})

    def fake_preflight(config: UsageBassoonConfig) -> tuple[str, ...]:
        preflighted.append(config)
        return ()

    def fake_logging(_config: LoggingConfig) -> logging.Logger:
        return logger

    monkeypatch.setattr("usagebassoon.scheduling.Event", CycleEvent)
    monkeypatch.setattr("usagebassoon.scheduling.collect_run", fake_collect)
    monkeypatch.setattr("usagebassoon.scheduling.preflight_tokscale", fake_preflight)
    monkeypatch.setattr("usagebassoon.scheduling.configure_logging", fake_logging)

    with caplog.at_level(logging.INFO, logger=logger.name):
        run_worker(configuration.path)

    assert collected == [configuration] * 3
    assert all(config is collected[0] for config in collected)
    assert preflighted == [configuration]
    assert waits == [900.0] * 3
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    stderr = plain_cli_output(capsys.readouterr().err)
    if change == "none":
        assert errors == []
        assert stderr == ""
    else:
        assert len(errors) == 1
        assert "continuing with startup settings" in errors[0].getMessage()
        assert "restart the worker" in stderr
        assert SOURCE_ID not in stderr
        assert "22222222-2222-4222-8222-222222222222" not in stderr


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
            "--collect-interval",
            "1d",
        ],
    )

    assert result.exit_code == 1
    assert "minutes or hours" in plain_cli_output(result.output)


@pytest.mark.parametrize(
    ("collect_flag", "snapshot_flag"),
    [("--collect-interval", "--snapshot-interval"), ("-c", "-s")],
)
def test_worker_cli_starts_detached_process_and_reports_pid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collect_flag: str,
    snapshot_flag: str,
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
        snapshot_schedule_interval: str | None = None,
    ) -> WorkerStatus:
        assert snapshot_schedule_interval == "12h"
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
            collect_flag,
            "30m",
            snapshot_flag,
            "12h",
        ],
    )

    assert result.exit_code == 0, plain_cli_output(result.output)
    assert calls == ["30m"]
    assert "background" in plain_cli_output(result.output)
    assert "12345" in plain_cli_output(result.output)
    assert str(started.log_path) in plain_cli_output(result.output)


@pytest.mark.parametrize("collect_flag", ["--collect-interval", "-c"])
def test_schedule_install_persists_explicit_interval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collect_flag: str,
) -> None:
    """Persist --collect-interval before handing the schedule to its provider."""
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
            collect_flag,
            "30m",
        ],
    )

    assert result.exit_code == 0, plain_cli_output(result.output)
    assert (
        ConfigurationManager(configuration.path).load().collection.schedule.interval
        == "30m"
    )


def test_snapshot_worker_runs_while_collection_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Backup deadlines advance independently of a blocked collection call."""
    from dataclasses import replace

    import usagebassoon.scheduling as module
    from usagebassoon.config import (
        LocalSnapshotConfig,
        SnapshotsConfig,
    )

    configuration = replace(
        _configuration(tmp_path),
        snapshots=SnapshotsConfig(
            local=LocalSnapshotConfig(path=tmp_path / "archive", enable=True)
        ),
    )
    captured = Event()
    blocked = Event()
    signals: dict[int, object] = {}

    def configuration_for_worker(*_args: object) -> UsageBassoonConfig:
        return configuration

    def snapshot_interval(_config: UsageBassoonConfig) -> float:
        return 0.01

    monkeypatch.setattr(module, "_worker_configuration", configuration_for_worker)
    monkeypatch.setattr(module, "snapshot_schedule_seconds", snapshot_interval)

    def install_signal(number: int, handler: object) -> None:
        signals[number] = handler

    def snapshot(_config: UsageBassoonConfig) -> None:
        if blocked.wait(2):
            captured.set()

    def collect(_config: UsageBassoonConfig) -> tuple[str, PersistSummary]:
        blocked.set()
        assert captured.wait(2), "blocked collection prevented independent backup"
        handler = signals[module.signal.SIGTERM]
        assert callable(handler)
        handler(0, None)
        return "run", PersistSummary(0, 0, {})

    def broken_preflight(_config: UsageBassoonConfig) -> None:
        raise RuntimeError("tokscale unavailable")

    monkeypatch.setattr(module.signal, "signal", install_signal)
    monkeypatch.setattr(module, "preflight_tokscale", broken_preflight)
    monkeypatch.setattr(module, "run_snapshot_check", snapshot)
    monkeypatch.setattr(module, "collect_run", collect)
    run_worker(configuration.path)
    assert captured.is_set()


@pytest.mark.parametrize("platform", ["linux", "darwin"])
def test_native_snapshot_artifacts_have_independent_cadence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, platform: str
) -> None:
    """Separate scheduler artifacts never reuse the collection interval."""
    import plistlib
    import subprocess
    from dataclasses import replace

    import usagebassoon.scheduling as module
    from usagebassoon.config import (
        CollectionConfig,
        LocalSnapshotConfig,
        ScheduleConfig,
        SnapshotScheduleConfig,
        SnapshotsConfig,
    )

    config = replace(
        _configuration(tmp_path),
        collection=CollectionConfig(schedule=ScheduleConfig(interval="2h")),
        snapshots=SnapshotsConfig(
            local=LocalSnapshotConfig(path=tmp_path / "archive", enable=True),
            schedule=SnapshotScheduleConfig(interval="10m"),
        ),
    )
    monkeypatch.setattr(module, "_systemd_unit_dir", lambda: tmp_path)
    monkeypatch.setattr(module, "_snapshot_plist", lambda: tmp_path / "snapshot.plist")
    monkeypatch.setattr(module, "_resolve_bassoon", lambda: "/usr/bin/bassoon")
    calls: list[list[str]] = []

    def run(
        command: list[str], *, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        del check
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(module, "_run", run)
    module._install_snapshot_schedule(config, platform)
    if platform == "linux":
        timer = (tmp_path / module.SNAPSHOT_TIMER_NAME).read_text()
        assert "OnUnitActiveSec=600s" in timer and module.SNAPSHOT_SERVICE_NAME in timer
        assert "--automatic" in (tmp_path / module.SNAPSHOT_SERVICE_NAME).read_text()
    else:
        payload = plistlib.loads((tmp_path / "snapshot.plist").read_bytes())
        assert payload["StartInterval"] == 600
        assert "--automatic" in payload["ProgramArguments"]


@pytest.mark.parametrize("disabled_weekly", [False, True])
def test_snapshot_schedule_defaults_and_disable_flags(
    tmp_path: Path, disabled_weekly: bool
) -> None:
    """Use hourly weekly checks or twelve-hour cadence, and stop disabled archives."""
    from dataclasses import replace

    from usagebassoon.config import (
        LocalSnapshotConfig,
        SnapshotsConfig,
    )
    from usagebassoon.scheduling import snapshot_schedule_seconds

    config = replace(
        _configuration(tmp_path),
        snapshots=SnapshotsConfig(
            local=LocalSnapshotConfig(
                path=tmp_path / "archive", enable=True, disable_weekly=disabled_weekly
            ),
        ),
    )
    assert snapshot_schedule_seconds(config) == (43200.0 if disabled_weekly else 3600.0)
    disabled = replace(
        config, snapshots=SnapshotsConfig(local=LocalSnapshotConfig(enable=False))
    )
    assert snapshot_schedule_seconds(disabled) is None


@pytest.mark.parametrize("snapshot_flag", ["--snapshot-interval", "-s"])
def test_schedule_install_persists_independent_snapshot_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, snapshot_flag: str
) -> None:
    """Install uses the validated snapshot override and preserves collection cadence."""
    config = _configuration(tmp_path)
    observed: list[UsageBassoonConfig] = []

    def install(
        configuration: UsageBassoonConfig, *, no_linger: bool
    ) -> ScheduleStatus:
        del no_linger
        observed.append(configuration)
        return ScheduleStatus(
            platform="linux",
            provider="systemd",
            installed=True,
            active=True,
            enabled=True,
            interval=configuration.collection.schedule.interval,
            artifact=tmp_path / "timer",
            log_path="journal",
            command=(),
        )

    monkeypatch.setattr(
        "usagebassoon.cli.schedule.scheduler_availability",
        lambda: SchedulerAvailability("linux", "systemd", True, "ready"),
    )
    monkeypatch.setattr("usagebassoon.cli.schedule.install_native_schedule", install)
    result = CliRunner().invoke(
        app,
        [
            "schedule",
            "install",
            "--config",
            str(config.path),
            snapshot_flag,
            "2d",
        ],
    )
    assert result.exit_code == 0, plain_cli_output(result.output)
    assert observed[0].snapshots.schedule.interval == "2d"
    assert observed[0].collection.schedule.interval == "15m"
    assert ConfigurationManager(config.path).load() == observed[0]


def test_worker_persists_both_intervals(tmp_path: Path) -> None:
    """Validate and persist both independent schedules before worker startup."""
    from usagebassoon.scheduling import _worker_configuration

    config = _configuration(tmp_path)
    loaded = _worker_configuration(config.path, "30m", "2d")
    assert loaded.collection.schedule.interval == "30m"
    assert loaded.snapshots.schedule.interval == "2d"
    assert ConfigurationManager(config.path).load() == loaded


@pytest.mark.parametrize(
    ("role", "failure_kind"),
    [("collection", "error"), ("collection", "busy"), ("snapshot", "error")],
)
def test_worker_continues_after_an_operational_cycle_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    role: str,
    failure_kind: str,
) -> None:
    """Failed cycles remain observable and do not prevent the next scheduled run."""
    import usagebassoon.scheduling as module
    from usagebassoon.collection_lock import CollectionBusy

    config = _configuration(tmp_path)
    attempts: list[UsageBassoonConfig] = []
    waits: list[float | None] = []
    logger = logging.getLogger("test_worker_failures")
    failure = (
        CollectionBusy("another collector is active")
        if failure_kind == "busy"
        else OSError("cycle unavailable")
    )

    class StopAfterTwoCycles(Event):
        """Advance two cycles without sleeping or leaving a worker running."""

        @override
        def wait(self, timeout: float | None = None) -> bool:
            waits.append(timeout)
            if len(waits) == 2:
                self.set()
            return self.is_set()

    def cycle(configuration: UsageBassoonConfig) -> None:
        attempts.append(configuration)
        if len(attempts) == 1:
            raise failure

    def collect(configuration: UsageBassoonConfig) -> tuple[str, PersistSummary]:
        cycle(configuration)
        return "successful-run", PersistSummary(1, 0, {})

    def configure(_config: LoggingConfig) -> logging.Logger:
        return logger

    def preflight(_config: UsageBassoonConfig) -> tuple[str, str]:
        return "tokscale", "test"

    monkeypatch.setattr(module, "configure_logging", configure)
    with caplog.at_level(logging.INFO, logger=logger.name):
        if role == "snapshot":
            monkeypatch.setattr(module, "run_snapshot_check", cycle)
            module._snapshot_worker(config, StopAfterTwoCycles(), 60)
        else:
            monkeypatch.setattr(module, "Event", StopAfterTwoCycles)
            monkeypatch.setattr(module, "preflight_tokscale", preflight)
            monkeypatch.setattr(module, "collect_run", collect)
            run_worker(config.path)
    assert attempts == [config, config]
    assert waits == ([60, 60] if role == "snapshot" else [900, 900])
    output = capsys.readouterr()
    if role == "collection":
        assert plain_cli_output(output.out).count("scheduled run successful-run") == 1
        assert str(failure) in plain_cli_output(output.err)
    records = [record for record in caplog.records if record.exc_info]
    if failure_kind == "busy":
        assert not records
        assert "scheduled collection skipped" in caplog.text
    else:
        assert len(records) == 1
        assert records[0].exc_info is not None
        assert records[0].exc_info[1] is failure


@pytest.mark.parametrize("origin", ["child", "backend"])
@pytest.mark.parametrize("disabled", [False, True])
def test_worker_failure_outputs_protect_active_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    origin: str,
    disabled: bool,
) -> None:
    """Child and backend failures stay secret-safe and permit another cycle."""
    from dataclasses import replace
    from sys import executable

    import usagebassoon.scheduling as module
    from usagebassoon.collector import _json_command
    from usagebassoon.logger import LOG_DIRECTORY_ENV_VAR

    monkeypatch.setenv(LOG_DIRECTORY_ENV_VAR, str(tmp_path / "logs"))
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "unlabeled-backend-credential")
    monkeypatch.setenv("CUSTOM_INPUT", "unlabeled-child-credential")
    config = replace(
        _configuration(tmp_path),
        tokscale_env=("CUSTOM_INPUT",),
        logging=LoggingConfig(directory=tmp_path / "logs", disable=disabled),
    )
    log_path = tmp_path / "logs" / "usagebassoon.log"
    previous_log = log_path.read_bytes()
    attempts = 0
    failures: list[RuntimeError] = []

    class StopAfterTwoCycles(Event):
        """Exercise recovery without waiting for the scheduled interval."""

        @override
        def wait(self, timeout: float | None = None) -> bool:
            if attempts == 2:
                self.set()
            return self.is_set()

    def configuration(
        _path: Path | None, _collection: str | None, _snapshots: str | None
    ) -> UsageBassoonConfig:
        return config

    def preflight(_config: UsageBassoonConfig) -> tuple[str, str]:
        return "tokscale", "test"

    def collect(settings: UsageBassoonConfig) -> tuple[str, PersistSummary]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            try:
                if origin == "child":
                    _json_command(
                        settings,
                        [
                            executable,
                            "-c",
                            "import os, sys; "
                            "sys.stderr.write(os.environ['CUSTOM_INPUT']); sys.exit(1)",
                        ],
                        "graph",
                    )
                else:
                    raise RuntimeError(
                        "backend unavailable: unlabeled-backend-credential"
                    )
            except RuntimeError as error:
                failures.append(error)
                raise RuntimeError(f"collection publication failed: {error}") from error
        return "successful-run", PersistSummary(1, 0, {})

    monkeypatch.setattr(module, "Event", StopAfterTwoCycles)
    monkeypatch.setattr(module, "_worker_configuration", configuration)
    monkeypatch.setattr(module, "preflight_tokscale", preflight)
    monkeypatch.setattr(module, "collect_run", collect)
    run_worker(config.path)

    output = capsys.readouterr()
    assert attempts == 2
    assert "scheduled run successful-run" in plain_cli_output(output.out)
    assert len(failures) == 1
    secret = f"unlabeled-{origin}-credential"
    assert secret in str(failures[0])
    if disabled:
        assert plain_cli_output(output.err) == ""
        assert log_path.read_bytes() == previous_log
    else:
        log = (tmp_path / "logs" / "usagebassoon.log").read_text()
        assert secret not in log + output.err
        assert "scheduled collection failed" in plain_cli_output(output.err)
        assert "direct cause" in log
        assert "test_scheduling.py" in log
        if origin == "child":
            assert "collector.py" in log
        assert "collection publication failed" in log


@pytest.mark.parametrize("content", ["not-a-pid", "0", "-1"])
def test_invalid_worker_pid_state_blocks_worker_control(
    tmp_path: Path,
    content: str,
) -> None:
    """Unknown worker identity must not be treated as proof no worker exists."""
    from usagebassoon.scheduling import SchedulingError, _read_worker_pid

    path = tmp_path / "worker.pid"
    assert _read_worker_pid(path) is None
    path.write_text(content)
    with pytest.raises(SchedulingError, match="worker PID state"):
        _read_worker_pid(path)


def test_unreadable_worker_pid_state_is_not_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Permission failures prevent unsafe duplicate startup or misleading stop."""
    from usagebassoon.scheduling import SchedulingError, _read_worker_pid

    def denied(_path: Path, *, encoding: str) -> str:
        assert encoding == "utf-8"
        raise PermissionError("cannot inspect worker identity")

    monkeypatch.setattr(Path, "read_text", denied)
    with pytest.raises(SchedulingError) as caught:
        _read_worker_pid(tmp_path / "worker.pid")
    assert isinstance(caught.value.__cause__, PermissionError)


def test_healthy_collection_does_not_mask_an_inactive_snapshot_schedule(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backup scheduler state is independently actionable in doctor output."""
    from dataclasses import replace

    import usagebassoon.scheduling as module
    from usagebassoon.config import LocalSnapshotConfig, SnapshotsConfig

    config = replace(
        _configuration(tmp_path),
        snapshots=SnapshotsConfig(
            local=LocalSnapshotConfig(path=tmp_path / "archive", enable=True)
        ),
    )
    status = module.ScheduleStatus(
        "linux",
        "systemd",
        True,
        True,
        True,
        "15m",
        tmp_path / "timer",
        "log",
        (),
        snapshot_installed=True,
        snapshot_active=False,
    )

    def available() -> module.SchedulerAvailability:
        return module.SchedulerAvailability("linux", "systemd", True, "available")

    def state(_config: UsageBassoonConfig) -> module.ScheduleStatus:
        return status

    monkeypatch.setattr(module, "scheduler_availability", available)
    monkeypatch.setattr(module, "native_schedule_status", state)
    check = module.schedule_doctor_check(config)
    assert check.status == "warning" and "snapshot schedule" in check.message
