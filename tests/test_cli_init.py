# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_init.py — Typer integration tests for initialization."""

from __future__ import annotations

import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID

import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.config import (
    CONFIG_PATH_ENV_VAR,
    ConfigurationManager,
    default_local_database_path,
    write_initial_config,
)
from usagebassoon.logger import LOG_DIRECTORY_ENV_VAR


def test_missing_source_identity_is_repaired_atomically_without_reformatting(
    tmp_path: Path,
) -> None:
    """Simultaneous initialization preserves comments and one shared new identity."""
    path = tmp_path / "config.toml"
    content = (
        "# Keep this comment.\n"
        'backend.provider = "duckdb"\n'
        "\n"
        "[snapshots]\n"
        "max_snapshots = 8\n"
    )
    path.write_text(content)
    with ThreadPoolExecutor(max_workers=3) as executor:
        created = list(executor.map(write_initial_config, [path] * 3))
    assert created == [False, False, False]
    text = path.read_text()
    parsed = tomllib.loads(text)
    assert str(UUID(parsed["source_id"])) == parsed["source_id"]
    assert text.endswith(content)
    assert parsed["snapshots"]["max_snapshots"] == 8
    assert write_initial_config(path) is False
    assert path.read_text() == text


def test_recovery_init_rejects_data_and_leaves_configuration_identity_intact(
    tmp_path: Path,
) -> None:
    """Recovery initialization never bypasses destination-emptiness validation."""
    path = tmp_path / "config.toml"
    database = tmp_path / "destination.duckdb"
    source = "11111111-1111-4111-8111-111111111111"
    path.write_text(
        f'source_id = "{source}"\n'
        f'backend.provider = "duckdb"\n'
        f'backend.duckdb.database = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.connection.execute("CREATE TABLE foreign_data (value INTEGER)")
    backend.connection.execute("INSERT INTO foreign_data VALUES (7)")
    backend.close()
    result = CliRunner().invoke(app, ["init", "--restore", "--config", str(path)])
    assert result.exit_code != 0
    assert "restore requires an empty warehouse" in plain_cli_output(result.output)
    assert tomllib.loads(path.read_text())["source_id"] == source


def test_init_creates_source_config_and_local_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create a stable source namespace and the default DuckDB schema."""
    config_path = tmp_path / "config.toml"
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "AppData" / "Local"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / ".local" / "share"))
    monkeypatch.delenv(CONFIG_PATH_ENV_VAR, raising=False)

    result = CliRunner().invoke(app, ["init", "--config", str(config_path)])

    assert result.exit_code == 0
    original = config_path.read_bytes()
    assert set(tomllib.loads(original.decode())) == {"source_id", "backend", "spinner"}
    configuration = ConfigurationManager(config_path).load()
    assert UUID(configuration.source_id)
    assert configuration.backend == "duckdb"
    assert configuration.spinner == "pong"
    assert configuration.local_database == default_local_database_path()
    assert configuration.collection.schedule.interval == "15m"
    assert configuration.logging.max_files == 5
    repeated = CliRunner().invoke(app, ["init", "--config", str(config_path)])
    assert repeated.exit_code == 0
    assert "Using existing configuration" in plain_cli_output(repeated.output)
    assert config_path.read_bytes() == original
    assert configuration.local_database is not None
    backend = DuckDBBackend(configuration.local_database)
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
        f'source_id = "{source_id}"\n'
        'backend.provider = "duckdb"\n'
        f'backend.duckdb.database = "{database}"\n'
    )

    result = CliRunner().invoke(app, ["init", "--config", str(config_path)])

    assert result.exit_code == 0
    assert ConfigurationManager(config_path).load().source_id == source_id
    assert "Using existing configuration" in plain_cli_output(result.output)
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
        'backend.provider = "unsupported"\n'
        'backend.duckdb.database = "usagebassoon"\n'
        f'\n[logging]\ndirectory = "{log_directory}"\n'
    )

    result = CliRunner().invoke(app, ["init", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "backend.provider must be one of" in plain_cli_output(result.output)
    log_path = log_directory / "usagebassoon.log"
    assert "backend.provider must be one of" in log_path.read_text()
