# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_doctor.py — Typer integration tests for health diagnostics."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create an initialized local warehouse for doctor command tests."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\ndatabase = "{database}"\n'
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


def test_doctor_strict_fails_when_unresolved_drift_is_present(tmp_path: Path) -> None:
    """Treat diagnostic warnings as failures only when strict mode is requested."""
    config, backend = _configured_store(tmp_path)
    backend.append(
        "schema_drift",
        pa.table(
            {
                "drift_id": ["drift-1"],
                "run_id": ["run-1"],
                "source_id": [SOURCE_ID],
                "detected_at": [datetime(2026, 9, 15, tzinfo=UTC)],
                "payload_kind": ["models"],
                "drift_kind": ["unknown_field"],
                "path": ["entries[].extra"],
                "detail": ["unexpected field"],
                "tokscale_ver": ["4.15.1"],
                "resolved": [False],
            }
        ),
    )
    backend.close()
    runner = CliRunner()

    regular = runner.invoke(app, ["doctor", "--config", str(config)])
    strict = runner.invoke(app, ["doctor", "--strict", "--config", str(config)])

    assert regular.exit_code == 0
    assert "WARNING schema_drift" in regular.output
    assert strict.exit_code == 1
