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

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create an initialized local warehouse for doctor command tests."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\n'
        f'local_database = "{database}"\n'
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
    assert str(missing) not in sanitized.output
    assert "<config-path>" in sanitized.output
    assert raw.exit_code == 1
    assert str(missing) in raw.output.replace("\n", "")
    assert "raw doctor output may contain" in raw.stderr


def test_doctor_strict_fails_when_unresolved_drift_is_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Treat diagnostic warnings as failures only when strict mode is requested."""
    config, backend = _configured_store(tmp_path)
    backend.append(
        "schema_drift_events",
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
                "created_at": [datetime(2026, 9, 15, tzinfo=UTC)],
                "updated_at": [datetime(2026, 9, 15, tzinfo=UTC)],
                "detected_run_id": ["run-1"],
                "updated_run_id": ["run-1"],
                "resolved": [False],
                "observation_count": [1],
            }
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
    assert "WARNING schema_drift" in regular.output
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

    result = CliRunner().invoke(app, ["doctor", "--config", str(config)])

    assert result.exit_code == 0, result.output
    assert calls == [["npx", "tokscale@latest", "--version"]]
    assert (
        result.output.index("OK usagebassoon: version")
        < result.output.index("OK tokscale: version 4.15.1")
        < result.output.index("OK configuration:")
    )
    assert "command: npx tokscale@latest" in result.output


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
    assert "ERROR tokscale:" in result.output
    assert "command: missing-tokscale" in result.output
    assert "OK configuration:" in result.output
