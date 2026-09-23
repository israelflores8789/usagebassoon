# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_init.py — Typer integration tests for initialization."""

from __future__ import annotations

import tomllib
from pathlib import Path
from uuid import UUID

import pytest
from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.config import (
    CONFIG_PATH_ENV_VAR,
    DEFAULT_DUCKDB_DATABASE,
    ConfigurationManager,
)
from usagebassoon.logger import LOG_DIRECTORY_ENV_VAR


def test_init_creates_source_config_and_local_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create a stable source namespace and the default DuckDB schema."""
    config_path = tmp_path / "config.toml"
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv(CONFIG_PATH_ENV_VAR, raising=False)

    result = CliRunner().invoke(app, ["init", "--config", str(config_path)])

    assert result.exit_code == 0
    original = config_path.read_bytes()
    assert set(tomllib.loads(original.decode())) == {"source_id", "backend"}
    configuration = ConfigurationManager(config_path).load()
    assert UUID(configuration.source_id)
    assert configuration.backend == "duckdb"
    assert configuration.database == DEFAULT_DUCKDB_DATABASE
    assert configuration.schedule.interval == "15m"
    assert configuration.logging.max_files == 10
    repeated = CliRunner().invoke(app, ["init", "--config", str(config_path)])
    assert repeated.exit_code == 0
    assert "Using existing configuration" in repeated.output
    assert config_path.read_bytes() == original
    backend = DuckDBBackend(configuration.database)
    try:
        assert backend.query("SELECT count(*) AS n FROM sessions").to_pylist() == [
            {"n": 0}
        ]
    finally:
        backend.close()


def test_init_preserves_an_existing_configuration(tmp_path: Path) -> None:
    """Reuse a configured source and database without overwriting either one."""
    config_path = tmp_path / "config.toml"
    database = tmp_path / "custom.duckdb"
    source_id = "11111111-1111-4111-8111-111111111111"
    config_path.write_text(
        f'source_id = "{source_id}"\nbackend = "duckdb"\ndatabase = "{database}"\n'
    )

    result = CliRunner().invoke(app, ["init", "--config", str(config_path)])

    assert result.exit_code == 0
    assert ConfigurationManager(config_path).load().source_id == source_id
    assert "Using existing configuration" in result.output
    backend = DuckDBBackend(database)
    try:
        assert backend.query("SELECT count(*) AS n FROM tags").to_pylist() == [{"n": 0}]
    finally:
        backend.close()


def test_init_formats_invalid_existing_configuration_as_a_cli_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report an invalid preserved config without exposing a traceback."""
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
    config_path = tmp_path / "config.toml"
    log_directory = tmp_path / "logs"
    config_path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "unsupported"\n'
        'database = "usagebassoon"\n'
        f'\n[logging]\ndirectory = "{log_directory}"\n'
    )

    result = CliRunner().invoke(app, ["init", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "backend must be one of" in result.output
    log_path = log_directory / "usagebassoon.log"
    assert "backend must be one of" in log_path.read_text()
