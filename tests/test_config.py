# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_config.py — Tests for configuration path resolution and validation."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from usagebassoon.config import (
    CONFIG_PATH_ENV_VAR,
    DEFAULT_LOG_DIRECTORY,
    ConfigurationError,
    ConfigurationManager,
)
from usagebassoon.logger import LOGGER_NAME


@pytest.fixture(autouse=True)
def isolate_config_error_logs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Iterator[None]:
    """Keep invalid-config log files inside each test's temporary directory."""
    monkeypatch.setenv("HOME", str(tmp_path))
    logger = logging.getLogger(LOGGER_NAME)
    existing_handlers = tuple(logger.handlers)
    yield
    for handler in tuple(logger.handlers):
        if handler not in existing_handlers:
            logger.removeHandler(handler)
            handler.close()


def test_explicit_config_path_has_highest_precedence(tmp_path: Path) -> None:
    """Use --config's path even when the environment points elsewhere."""
    explicit = tmp_path / "explicit.toml"
    environment = tmp_path / "environment.toml"
    explicit.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\ndatabase = ":memory:"\n'
    )
    environment.write_text(
        'source_id = "22222222-2222-4222-8222-222222222222"\n'
        'backend = "duckdb"\ndatabase = "other.duckdb"\n'
    )
    manager = ConfigurationManager(
        explicit,
        environ={CONFIG_PATH_ENV_VAR: str(environment)},
    )
    assert manager.path == explicit
    assert manager.load().database == ":memory:"


def test_environment_path_precedes_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the environment-selected file when no explicit path is supplied."""
    configured = tmp_path / "config.toml"
    configured.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\ndatabase = ":memory:"\n'
    )
    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(configured))
    manager = ConfigurationManager()
    assert manager.path == configured
    assert manager.load().backend == "duckdb"


def test_bigquery_requires_its_connection_settings(tmp_path: Path) -> None:
    """Reject a partial configuration before any backend is opened."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "bigquery"\ndatabase = "usagebassoon"\n'
    )
    with pytest.raises(ConfigurationError, match=r"\[bigquery\]"):
        ConfigurationManager(path).load()


def test_source_id_must_be_a_uuid(tmp_path: Path) -> None:
    """Reject an unparsable source namespace before opening a backend."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "not-a-uuid"\nbackend = "duckdb"\ndatabase = ":memory:"\n'
    )
    with pytest.raises(ConfigurationError, match="source_id must be a canonical UUID"):
        ConfigurationManager(path).load()


@pytest.mark.parametrize("scope", ["root", "tokscale"])
def test_unknown_config_keys_fail_and_are_written_to_the_log(
    tmp_path: Path,
    scope: str,
) -> None:
    """Reject unsupported keys and log their location before failing."""
    path = tmp_path / "config.toml"
    log_directory = tmp_path / "configured-logs"
    content = (
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\n'
        'database = ":memory:"\n'
    )
    if scope == "root":
        content += "unknown_option = true\n"
    content += f'\n[logging]\ndirectory = "{log_directory}"\n'
    if scope == "tokscale":
        content += '\n[tokscale]\ncommand = "bunx tokscale@latest"\n'
    path.write_text(content)

    with pytest.raises(ConfigurationError) as captured:
        ConfigurationManager(path).load()

    error = str(captured.value)
    expected_scope = "the root configuration table" if scope == "root" else "[tokscale]"
    assert f"unknown key(s) in {expected_scope}" in error
    log_path = log_directory / "usagebassoon.log"
    assert str(log_path) in error
    log_content = log_path.read_text()
    assert "ERROR usagebassoon configuration file" in log_content
    assert str(path) in log_content
    assert f"unknown key(s) in {expected_scope}" in log_content


def test_invalid_logging_settings_use_the_default_error_log(
    tmp_path: Path,
) -> None:
    """Use the default file logger if the configured logger settings are invalid."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\ndatabase = ":memory:"\n'
        "[logging]\nmax_files = 0\n"
    )

    with pytest.raises(ConfigurationError, match=r"logging\.max_files"):
        ConfigurationManager(path).load()

    log_path = tmp_path / ".local/state/usagebassoon/logs/usagebassoon.log"
    assert log_path.is_file()
    assert "logging.max_files must be a positive integer" in log_path.read_text()


def test_bigquery_credentials_and_operational_settings_are_typed(
    tmp_path: Path,
) -> None:
    """Parse explicit service-account paths and scheduled-cycle settings."""
    path = tmp_path / "config.toml"
    credentials = tmp_path / "service-account.json"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "bigquery"\ndatabase = "usagebassoon"\n'
        '[bigquery]\nproject = "usagebassoon-test"\n'
        'location = "us-central1"\n'
        f'credentials_file = "{credentials}"\n'
        "[collection]\nmax_retries = 4\nretry_initial_seconds = 0.5\n"
        "[logging]\nmax_files = 5\nmax_bytes = 4096\n"
    )
    config = ConfigurationManager(path).load()
    assert config.bigquery is not None
    assert config.bigquery.credentials_file == credentials
    assert config.collection.max_retries == 4
    assert config.collection.retry_initial_seconds == 0.5
    assert config.logging.directory == DEFAULT_LOG_DIRECTORY.expanduser()
    assert (config.logging.max_files, config.logging.max_bytes) == (5, 4096)


def test_snapshot_defaults_and_interval_validation_are_consistent(
    tmp_path: Path,
) -> None:
    """Use the documented default retention and reject invalid cadence values."""
    valid = tmp_path / "valid.toml"
    valid.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\ndatabase = ":memory:"\n'
        '[snapshots]\ninterval = "12h"\n'
    )
    snapshots = ConfigurationManager(valid).load().snapshots
    assert snapshots is not None
    assert snapshots.max_snapshots == 3
    invalid = tmp_path / "invalid.toml"
    invalid.write_text(valid.read_text().replace('"12h"', '"zero"'))
    with pytest.raises(ConfigurationError, match=r"snapshots\.interval"):
        ConfigurationManager(invalid).load()
