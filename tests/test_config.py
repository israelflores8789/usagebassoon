# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_config.py — Tests for configuration path resolution and validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from usagebassoon.config import (
    CONFIG_PATH_ENV_VAR,
    DEFAULT_LOG_DIRECTORY,
    ConfigurationError,
    ConfigurationManager,
)


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
    with pytest.raises(ConfigurationError, match="source_id must be a UUID"):
        ConfigurationManager(path).load()


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
