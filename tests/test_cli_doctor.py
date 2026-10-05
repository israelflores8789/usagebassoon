# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_doctor.py — Typer integration tests for health diagnostics."""

from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from tests._observations import observations
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.diagnostics import DoctorCheck

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create an initialized local warehouse for doctor command tests."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend.provider = "duckdb"\n'
        f'backend.duckdb.database = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    return config, backend


def test_doctor_sanitizes_config_location_unless_raw(tmp_path: Path) -> None:
    """Make doctor issue-ready by default while warning on raw diagnostics."""
    missing = tmp_path / "private" / "config.toml"
    runner = CliRunner()

    sanitized = runner.invoke(app, ["doctor", "--config", str(missing)])
    raw = runner.invoke(app, ["doctor", "--raw", "--config", str(missing)])

    assert sanitized.exit_code == 1
    assert str(missing) not in plain_cli_output(sanitized.output)
    assert "<config-path>" in plain_cli_output(sanitized.output)
    assert raw.exit_code == 1
    assert str(missing) in plain_cli_output(raw.output).replace("\n", "")
    assert "raw doctor output may contain" in plain_cli_output(raw.stderr)


def test_doctor_strict_fails_when_unresolved_drift_is_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Treat diagnostic warnings as failures only when strict mode is requested."""
    config, backend = _configured_store(tmp_path)
    observed = datetime.now(UTC)
    backend.append(
        "schema_drift_events",
        observations(
            pa.table(
                {
                    "source_id": [SOURCE_ID],
                    "domain": ["models"],
                    "tokscale_ver": ["4.15.1"],
                    "drift_key": ["unknown_field:entries[].extra"],
                    "drift_kind": ["unknown_field"],
                    "path": ["entries[].extra"],
                    "detail": ["unexpected field"],
                    "contract_tokscale_ver": ["4.15.1"],
                    "created_at": [observed],
                    "collected_at": [observed],
                    "run_id": ["run-1"],
                    "resolved": [False],
                    "observation_count": [1],
                }
            )
        ),
    )
    backend.close()

    def fake_preflight(_config: object) -> tuple[tuple[str, ...], str]:
        """Return a working Tokscale command for this drift test."""
        return ("tokscale",), "4.15.1"

    monkeypatch.setattr("usagebassoon.cli.doctor.preflight_tokscale", fake_preflight)
    runner = CliRunner()

    regular = runner.invoke(app, ["doctor", "--config", str(config)])
    strict = runner.invoke(app, ["doctor", "--strict", "--config", str(config)])

    assert regular.exit_code == 0
    assert "WARNING schema_drift" in plain_cli_output(regular.output)
    assert strict.exit_code == 1


def test_doctor_reports_configured_tokscale_command_and_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Probe the configured runner and print both versions before backend checks."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    with config.open("a") as stream:
        stream.write('\n[tokscale]\nbin = "npx tokscale@latest"\n')
    calls: list[list[str]] = []

    class FakeProcess:
        """Supply fresh pipes for each Tokscale version probe."""

        pid = 12345
        returncode = 0

        def __init__(self) -> None:
            self.stdout = BytesIO(b"tokscale 4.15.1\n")
            self.stderr = BytesIO(b"")

        def poll(self) -> int:
            return self.returncode

        def wait(self) -> int:
            return self.returncode

    def fake_popen(command: list[str], **_kwargs: object) -> FakeProcess:
        calls.append(command)
        return FakeProcess()

    monkeypatch.setattr("usagebassoon.collector.subprocess.Popen", fake_popen)

    def fake_schedule_check(_configuration: object) -> DoctorCheck:
        """Keep scheduler subprocesses outside the tokscale probe test."""
        return DoctorCheck("scheduling", "ok", "scheduler inspection skipped")

    monkeypatch.setattr(
        "usagebassoon.cli.doctor.schedule_doctor_check", fake_schedule_check
    )

    result = CliRunner().invoke(app, ["doctor", "--config", str(config)])

    assert result.exit_code == 0, plain_cli_output(result.output)
    assert calls == [["npx", "tokscale@latest", "--version"]]
    assert (
        plain_cli_output(result.output).index("OK usagebassoon: version")
        < plain_cli_output(result.output).index("OK tokscale: version 4.15.1")
        < plain_cli_output(result.output).index("OK configuration:")
    )
    assert "command: npx tokscale@latest" in plain_cli_output(result.output)


def test_doctor_reports_failed_tokscale_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail doctor when the selected Tokscale runner cannot start."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    with config.open("a") as stream:
        stream.write('\n[tokscale]\nbin = "missing-tokscale"\n')

    def missing_popen(*_args: object, **_kwargs: object) -> None:
        raise FileNotFoundError("missing-tokscale")

    monkeypatch.setattr("usagebassoon.collector.subprocess.Popen", missing_popen)

    result = CliRunner().invoke(app, ["doctor", "--config", str(config)])

    assert result.exit_code == 1
    assert "ERROR tokscale:" in plain_cli_output(result.output)
    assert "command: missing-tokscale" in plain_cli_output(result.output)
    assert "OK configuration:" in plain_cli_output(result.output)


def test_backup_health_does_not_let_weekly_capture_mask_scheduled_overdue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each automatic role has its own freshness check."""
    from datetime import UTC, datetime, timedelta

    from usagebassoon.config import (
        LocalSnapshotConfig,
        SnapshotScheduleConfig,
        SnapshotsConfig,
        UsageBassoonConfig,
    )
    from usagebassoon.diagnostics import snapshot_health
    from usagebassoon.snapshot.reader import SnapshotReader

    root = str(tmp_path / "archive")
    config = UsageBassoonConfig(
        tmp_path / "config.toml",
        "11111111-1111-4111-8111-111111111111",
        "duckdb",
        snapshots=SnapshotsConfig(
            local=LocalSnapshotConfig(path=Path(root), enable=True),
            schedule=SnapshotScheduleConfig(interval="1h"),
        ),
    )
    stamp = datetime.now(UTC)

    def listing(
        _reader: SnapshotReader, _selection: str = "latest"
    ) -> list[dict[str, object]]:
        return [
            {
                "uri": root + "/scheduled",
                "captured_at": (stamp - timedelta(hours=2)).isoformat(),
                "roles": ["scheduled"],
                "verified_at": stamp.isoformat(),
            },
            {
                "uri": root + "/weekly",
                "captured_at": stamp.isoformat(),
                "roles": ["weekly"],
                "weekly_slot": stamp.strftime("%G-W%V"),
                "verified_at": stamp.isoformat(),
            },
        ]

    monkeypatch.setattr(SnapshotReader, "listing", listing)
    check = snapshot_health(config)
    assert check.status == "warning"
    assert "scheduled recovery capture is overdue" in check.message
    assert "current weekly recovery slot is overdue" not in check.message
    assert any("1 of 4 target slots available" in detail for detail in check.details)


def test_advisory_maintenance_failure_is_logged(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Optional maintenance inspection stays non-fatal with traceback evidence."""
    import logging

    from usagebassoon.diagnostics import maintenance_health

    backend = DuckDBBackend(":memory:")

    def unavailable() -> tuple[bool, str] | None:
        raise OSError("maintenance RPC unavailable")

    monkeypatch.setattr(backend, "maintenance_status", unavailable)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "disabled", False)
    try:
        assert maintenance_health(backend).status == "warning"
        assert "doctor maintenance inspection failed" in caplog.text
        assert "maintenance RPC unavailable" in caplog.text
    finally:
        backend.close()


def test_compaction_read_failure_is_logged_and_other_checks_continue(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unknown retention health is an error check without aborting the whole doctor."""
    import logging

    from usagebassoon.diagnostics import run_doctor

    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()

    def unavailable() -> pa.Table | None:
        raise OSError("compaction progress unavailable")

    monkeypatch.setattr(backend, "compaction_backlog", unavailable)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "disabled", False)
    try:
        report = run_doctor(backend, backend_name="duckdb", database=":memory:")
        assert any(
            c.name == "compaction" and c.status == "error" for c in report.checks
        )
        assert any(c.name == "reconciliation" for c in report.checks)
        assert "doctor compaction inspection failed" in caplog.text
        assert "compaction progress unavailable" in caplog.text
    finally:
        backend.close()


def test_weekly_health_derives_target_slot_gaps_from_available_snapshots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Present coverage reports a missing intermediate week without a tally."""
    from datetime import timedelta

    from usagebassoon.config import (
        LocalSnapshotConfig,
        SnapshotsConfig,
        UsageBassoonConfig,
    )
    from usagebassoon.diagnostics import snapshot_health
    from usagebassoon.snapshot.reader import SnapshotReader

    now = datetime.now(UTC)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=now.weekday()
    )
    root = tmp_path / "archive"
    config = UsageBassoonConfig(
        tmp_path / "config.toml",
        SOURCE_ID,
        "duckdb",
        snapshots=SnapshotsConfig(local=LocalSnapshotConfig(path=root, enable=True)),
    )
    rows: list[dict[str, object]] = [
        {
            "uri": str(root / str(index)),
            "captured_at": (start - timedelta(weeks=index)).isoformat(),
            "weekly_slot": (start - timedelta(weeks=index)).strftime("%G-W%V"),
            "roles": ["weekly"],
        }
        for index in (0, 1, 3)
    ]

    def listing(
        _reader: SnapshotReader, _selection: str = "latest"
    ) -> list[dict[str, object]]:
        return rows

    monkeypatch.setattr(SnapshotReader, "listing", listing)
    report = snapshot_health(config)
    assert any("3 of 4 target slots available" in line for line in report.details)
    assert any(
        "Oldest available weekly recovery point" in line for line in report.details
    )
    missing = (start - timedelta(weeks=2)).strftime("%G-W%V")
    assert any(
        "Weekly coverage gaps" in line and missing in line for line in report.details
    )
