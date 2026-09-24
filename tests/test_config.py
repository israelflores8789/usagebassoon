# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_config.py — Tests for configuration path resolution and validation."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

import usagebassoon.config as config_module
from usagebassoon.config import (
    CONFIG_PATH_ENV_VAR,
    ConfigurationError,
    ConfigurationManager,
    default_config_path,
    default_local_database_path,
    default_log_directory,
    default_snapshot_directory,
    update_schedule_interval,
)
from usagebassoon.logger import LOG_DIRECTORY_ENV_VAR, LOGGER_NAME


@pytest.fixture(autouse=True)
def isolate_config_error_logs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Iterator[None]:
    """Keep invalid-config log files inside each test's temporary directory."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "AppData" / "Local"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / ".local" / "share"))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / ".local" / "state"))
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
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
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
    )
    environment.write_text(
        'source_id = "22222222-2222-4222-8222-222222222222"\n'
        'backend = "duckdb"\nlocal_database = "other.duckdb"\n'
    )
    manager = ConfigurationManager(
        explicit,
        environ={CONFIG_PATH_ENV_VAR: str(environment)},
    )
    assert manager.path == explicit
    assert manager.load().local_database == Path(":memory:")


def test_environment_path_precedes_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the environment-selected file when no explicit path is supplied."""
    configured = tmp_path / "config.toml"
    configured.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
    )
    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(configured))
    manager = ConfigurationManager()
    assert manager.path == configured
    assert manager.load().backend == "duckdb"


def test_default_configuration_path_uses_platformdirs() -> None:
    """Resolve the implicit configuration file through platformdirs."""
    assert ConfigurationManager().path == default_config_path()


def test_default_timeouts_and_schedule_are_typed(tmp_path: Path) -> None:
    """Load subprocess, schedule, and logging defaults without TOML sections."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
    )

    configuration = ConfigurationManager(path).load()

    assert configuration.schedule.interval == "15m"
    assert configuration.tokscale_timeout_seconds == 180.0
    assert configuration.logging.max_files == 5
    assert configuration.logging.max_bytes == 5 * 1024 * 1024


def test_duckdb_local_database_defaults_when_omitted(tmp_path: Path) -> None:
    """Use the local DuckDB path when a config omits local_database."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\nbackend = "duckdb"\n'
    )

    assert (
        ConfigurationManager(path).load().local_database
        == default_local_database_path()
    )


@pytest.mark.parametrize(
    ("backend", "required_section"),
    [("motherduck", "motherduck"), ("bigquery", "bigquery")],
)
def test_remote_backend_requires_its_settings(
    tmp_path: Path,
    backend: str,
    required_section: str,
) -> None:
    """Require the selected remote backend's configuration table."""
    path = tmp_path / "config.toml"
    path.write_text(
        f'source_id = "11111111-1111-4111-8111-111111111111"\nbackend = "{backend}"\n'
    )

    with pytest.raises(ConfigurationError, match=f"\\[{required_section}\\].*required"):
        ConfigurationManager(path).load()


def test_backend_remains_required(tmp_path: Path) -> None:
    """Keep backend explicit so storage selection is clear."""
    path = tmp_path / "config.toml"
    path.write_text('source_id = "11111111-1111-4111-8111-111111111111"\n')

    with pytest.raises(ConfigurationError, match="backend is required"):
        ConfigurationManager(path).load()


@pytest.mark.parametrize("interval", ["3m", "2m"])
def test_schedule_interval_must_exceed_tokscale_timeout(
    tmp_path: Path, interval: str
) -> None:
    """Reject schedules no longer than one permitted tokscale invocation."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
        f'[schedule]\ninterval = "{interval}"\n'
    )

    with pytest.raises(
        ConfigurationError,
        match=r"schedule\.interval must be greater than tokscale\.timeout",
    ):
        ConfigurationManager(path).load()


@pytest.mark.parametrize(
    ("field", "value"),
    [("schedule", "1d"), ("schedule", "1s"), ("schedule", "1w")],
)
def test_schedule_durations_are_limited_to_minutes_or_hours(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    """Reject day-, week-, and second-granularity scheduler durations."""
    path = tmp_path / "config.toml"
    setting = f'[{field}]\ninterval = "{value}"\n'
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\nlocal_database = ":memory:"\n' + setting
    )

    with pytest.raises(ConfigurationError, match="minutes or hours"):
        ConfigurationManager(path).load()


@pytest.mark.parametrize(
    ("section", "setting", "expected"),
    [
        ("tokscale", 'timeout = "1h"', "tokscale.timeout"),
        ("bigquery", 'timeout = "1h"', "bigquery.timeout"),
        ("gcs", 'timeout = "1h"', "gcs.timeout"),
        ("snapshots", 'interval = "1s"', "snapshots.interval"),
        ("snapshots", 'interval = "1w"', "snapshots.interval"),
    ],
)
def test_duration_units_are_setting_specific(
    tmp_path: Path, section: str, setting: str, expected: str
) -> None:
    """Reject duration units outside each setting's supported grain."""
    path = tmp_path / "config.toml"
    extras = {
        "bigquery": 'project = "usagebassoon-test"\ndataset = "usagebassoon_it"\n',
        "gcs": 'uri = "gs://bucket/archive"\nproject = "usagebassoon-test"\n',
    }
    backend_settings = (
        'backend = "bigquery"\n'
        if section == "bigquery"
        else 'backend = "duckdb"\nlocal_database = ":memory:"\n'
    )
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        f"{backend_settings}"
        f"[{section}]\n{extras.get(section, '')}{setting}\n"
    )

    with pytest.raises(ConfigurationError, match=expected):
        ConfigurationManager(path).load()


@pytest.mark.parametrize(
    ("section", "setting"),
    [
        ("tokscale", "timeout_seconds = 180"),
        ("collection", 'timeout = "5m"'),
        ("gcs", 'location = "US"'),
    ],
)
def test_removed_config_keys_are_rejected(
    tmp_path: Path, section: str, setting: str
) -> None:
    """Surface obsolete keys as configuration errors before collection."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
        f"[{section}]\n{setting}\n"
    )

    with pytest.raises(ConfigurationError, match="unknown key"):
        ConfigurationManager(path).load()


def test_update_schedule_interval_preserves_other_configuration(tmp_path: Path) -> None:
    """Persist an interval in place without rewriting unrelated TOML values."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
        '[schedule]\ninterval = "15m"\n'
        "[logging]\nmax_files = 4\n"
    )

    update_schedule_interval(path, "30m")

    content = path.read_text()
    assert 'interval = "30m"' in content
    assert "max_files = 4" in content
    assert ConfigurationManager(path).load().schedule.interval == "30m"


def test_bigquery_requires_its_connection_settings(tmp_path: Path) -> None:
    """Reject a partial configuration before any backend is opened."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\nbackend = "bigquery"\n'
    )
    with pytest.raises(ConfigurationError, match=r"\[bigquery\]"):
        ConfigurationManager(path).load()


def test_source_id_must_be_a_uuid(tmp_path: Path) -> None:
    """Reject an unparsable source namespace before opening a backend."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "not-a-uuid"\nbackend = "duckdb"\nlocal_database = ":memory:"\n'
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
        'local_database = ":memory:"\n'
    )
    if scope == "root":
        content += 'database = "usagebassoon"\n'
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
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
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
        'backend = "bigquery"\n'
        '[bigquery]\nproject = "usagebassoon-test"\n'
        'dataset = "usagebassoon_it"\n'
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
    assert config.logging.directory == default_log_directory()
    assert (config.logging.max_files, config.logging.max_bytes) == (5, 4096)


def test_snapshot_defaults_and_interval_validation_are_consistent(
    tmp_path: Path,
) -> None:
    """Use the documented default retention and reject invalid cadence values."""
    valid = tmp_path / "valid.toml"
    valid.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
        '[snapshots]\ninterval = "12h"\n'
    )
    snapshots = ConfigurationManager(valid).load().snapshots
    assert snapshots is not None
    assert snapshots.max_snapshots == 3
    invalid = tmp_path / "invalid.toml"
    invalid.write_text(valid.read_text().replace('"12h"', '"zero"'))
    with pytest.raises(ConfigurationError, match=r"snapshots\.interval"):
        ConfigurationManager(invalid).load()


@pytest.mark.parametrize(
    ("platform", "app_name"),
    [("linux", "usagebassoon"), ("darwin", "UsageBassoon"), ("win32", "UsageBassoon")],
)
def test_platform_default_directories_use_platformdirs_and_app_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    platform: str,
    app_name: str,
) -> None:
    """Use platformdirs with stable app names and flat Windows directories."""
    calls: list[tuple[str, str, bool]] = []

    def fake_path_factory(kind: str) -> Callable[..., Path]:
        def get_path(appname: str, *, appauthor: bool) -> Path:
            calls.append((kind, appname, appauthor))
            return tmp_path / kind / appname

        return get_path

    monkeypatch.setattr(config_module.sys, "platform", platform)
    monkeypatch.setattr(config_module, "user_config_path", fake_path_factory("config"))
    monkeypatch.setattr(config_module, "user_data_path", fake_path_factory("data"))
    monkeypatch.setattr(config_module, "user_log_path", fake_path_factory("log"))
    monkeypatch.setattr(config_module, "user_state_path", fake_path_factory("state"))

    assert default_config_path() == tmp_path / "config" / app_name / "config.toml"
    assert default_local_database_path() == (
        tmp_path / "data" / app_name / "usagebassoon.duckdb"
    )
    assert default_snapshot_directory() == tmp_path / "data" / app_name / "snapshots"
    if platform == "linux":
        assert default_log_directory() == tmp_path / "state" / app_name / "logs"
        assert calls[-1] == ("state", app_name, False)
    else:
        assert default_log_directory() == tmp_path / "log" / app_name
        assert calls[-1] == ("log", app_name, False)
    assert all(name == app_name and author is False for _, name, author in calls)


def test_linux_default_paths_follow_xdg_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Honor XDG config, data, and state roots when resolving Linux defaults."""
    if config_module.sys.platform != "linux":
        pytest.skip("XDG defaults are Linux-specific in this test")
    config_root = tmp_path / "xdg-config"
    data_root = tmp_path / "xdg-data"
    state_root = tmp_path / "xdg-state"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_root))
    monkeypatch.setenv("XDG_DATA_HOME", str(data_root))
    monkeypatch.setenv("XDG_STATE_HOME", str(state_root))

    assert default_config_path() == config_root / "usagebassoon" / "config.toml"
    assert default_local_database_path() == (
        data_root / "usagebassoon" / "usagebassoon.duckdb"
    )
    assert default_snapshot_directory() == data_root / "usagebassoon" / "snapshots"
    assert default_log_directory() == state_root / "usagebassoon" / "logs"


def test_gcs_and_local_snapshot_destinations_are_typed(
    tmp_path: Path,
) -> None:
    """Parse independent GCS and local snapshot destinations."""
    path = tmp_path / "config.toml"
    credentials = tmp_path / "service-account.json"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
        '[gcs]\nuri = "gs://bucket/archive"\n'
        'project = "usagebassoon-test"\n'
        f'credentials_file = "{credentials}"\n'
        "[snapshots]\n"
        f'file_uri = "file://{tmp_path / "snapshots"}"\n'
        'max_snapshots = 5\ninterval = "12h"\n'
    )

    config = ConfigurationManager(path).load()

    assert config.gcs is not None
    assert config.gcs.uri == "gs://bucket/archive"
    assert config.gcs.project == "usagebassoon-test"
    assert config.gcs.timeout_seconds == 60.0
    assert config.gcs.credentials_file == credentials
    assert config.snapshots is not None
    assert config.snapshots.file_uri == f"file://{tmp_path / 'snapshots'}"
    assert config.snapshots.max_snapshots == 5


def test_snapshots_gcs_uri_is_removed(tmp_path: Path) -> None:
    """Reject the removed unified snapshot URI field."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend = "duckdb"\nlocal_database = ":memory:"\n'
        '[snapshots]\ngcs_uri = "gs://bucket/archive"\n'
    )

    with pytest.raises(ConfigurationError, match=r"\[snapshots\]"):
        ConfigurationManager(path).load()
