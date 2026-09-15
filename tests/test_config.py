# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""Tests for configuration path resolution and validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from usagebassoon.config import (
    CONFIG_PATH_ENV_VAR,
    ConfigurationError,
    ConfigurationManager,
)


def test_explicit_config_path_has_highest_precedence(tmp_path: Path) -> None:
    """Use --config's path even when the environment points elsewhere."""
    explicit = tmp_path / "explicit.toml"
    environment = tmp_path / "environment.toml"
    explicit.write_text('backend = "duckdb"\ndatabase = ":memory:"\n')
    environment.write_text('backend = "duckdb"\ndatabase = "other.duckdb"\n')
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
    configured.write_text('backend = "duckdb"\ndatabase = ":memory:"\n')
    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(configured))
    manager = ConfigurationManager()
    assert manager.path == configured
    assert manager.load().backend == "duckdb"


def test_bigquery_requires_its_connection_settings(tmp_path: Path) -> None:
    """Reject a partial configuration before any backend is opened."""
    path = tmp_path / "config.toml"
    path.write_text('backend = "bigquery"\ndatabase = "usagebassoon"\n')
    with pytest.raises(ConfigurationError, match=r"\[bigquery\]"):
        ConfigurationManager(path).load()
