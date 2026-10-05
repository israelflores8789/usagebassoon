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
        'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n'
    )
    environment.write_text(
        'source_id = "22222222-2222-4222-8222-222222222222"\n'
        'backend.provider = "duckdb"\nbackend.duckdb.database = "other.duckdb"\n'
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
        'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n'
    )
    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(configured))
    manager = ConfigurationManager()
    assert manager.path == configured
    assert manager.load().backend == "duckdb"


def test_default_timeouts_and_schedule_are_typed(tmp_path: Path) -> None:
    """Load subprocess, schedule, and logging defaults without TOML sections."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n'
    )

    configuration = ConfigurationManager(path).load()

    assert configuration.collection.schedule.interval == "15m"
    assert configuration.spinner == "pong"
    assert configuration.tokscale_timeout_seconds == 180.0
    assert configuration.logging.max_files == 5
    assert configuration.logging.max_bytes == 5 * 1024 * 1024


@pytest.mark.parametrize("name", ["pong", "dots", "bouncingBall"])
def test_spinner_configuration(tmp_path: Path, name: str) -> None:
    """Accept yaspin's case-sensitive animation names."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        f'backend.provider = "duckdb"\nspinner = "{name}"\n'
    )
    assert ConfigurationManager(path).load().spinner == name


@pytest.mark.parametrize("value", ['"missing"', '"Pong"', '""', "123", "false"])
def test_invalid_spinner_configuration(tmp_path: Path, value: str) -> None:
    """Reject unsupported names and non-string settings with a config error."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        f'backend.provider = "duckdb"\nspinner = {value}\n'
    )
    with pytest.raises(ConfigurationError, match="spinner"):
        ConfigurationManager(path).load()


def test_duckdb_local_database_defaults_when_omitted(tmp_path: Path) -> None:
    """Use the local DuckDB path when a config omits local_database."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend.provider = "duckdb"\n'
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
        f'source_id = "11111111-1111-4111-8111-111111111111"\n'
        f'backend.provider = "{backend}"\n'
    )

    with pytest.raises(
        ConfigurationError, match=f"\\[backend.{required_section}\\].*required"
    ):
        ConfigurationManager(path).load()


def test_backend_defaults_to_duckdb(tmp_path: Path) -> None:
    """Use DuckDB when the backend table is omitted."""
    path = tmp_path / "config.toml"
    path.write_text('source_id = "11111111-1111-4111-8111-111111111111"\n')

    assert ConfigurationManager(path).load().backend == "duckdb"


@pytest.mark.parametrize("interval", ["3m", "2m"])
def test_schedule_interval_must_exceed_tokscale_timeout(
    tmp_path: Path, interval: str
) -> None:
    """Reject schedules no longer than one permitted tokscale invocation."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n'
        f'[collection.schedule]\ninterval = "{interval}"\n'
    )

    with pytest.raises(
        ConfigurationError,
        match=r"schedule\.interval must be greater than tokscale\.timeout",
    ):
        ConfigurationManager(path).load()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("collection.schedule", "1d"),
        ("collection.schedule", "1s"),
        ("collection.schedule", "1w"),
    ],
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
        'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n' + setting
    )

    with pytest.raises(ConfigurationError, match="minutes or hours"):
        ConfigurationManager(path).load()


@pytest.mark.parametrize(
    ("section", "setting", "expected"),
    [
        ("tokscale", 'timeout = "1h"', "tokscale.timeout"),
        ("backend.bigquery", 'timeout = "1h"', "bigquery.timeout"),
        ("snapshots.gcs", 'timeout = "1h"', "gcs.timeout"),
        ("snapshots.schedule", 'interval = "1s"', "snapshots.schedule.interval"),
        ("snapshots.schedule", 'interval = "1w"', "snapshots.schedule.interval"),
    ],
)
def test_duration_units_are_setting_specific(
    tmp_path: Path, section: str, setting: str, expected: str
) -> None:
    """Reject duration units outside each setting's supported grain."""
    path = tmp_path / "config.toml"
    extras = {
        "backend.bigquery": (
            'project = "usagebassoon-test"\ndataset = "usagebassoon_it"\n'
        ),
        "snapshots.gcs": (
            'enable = true\nuri = "gs://bucket/archive"\n'
            'project = "usagebassoon-test"\n'
        ),
    }
    backend_settings = (
        'backend.provider = "bigquery"\n'
        if section == "backend.bigquery"
        else 'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n'
    )
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        f"{backend_settings}"
        f"[{section}]\n{extras.get(section, '')}{setting}\n"
    )

    with pytest.raises(ConfigurationError, match=expected):
        ConfigurationManager(path).load()


def test_update_schedule_interval_preserves_other_configuration(tmp_path: Path) -> None:
    """Persist an interval in place without rewriting unrelated TOML values."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n'
        '[collection.schedule]\ninterval = "15m"\n'
        "[logging]\nmax_files = 4\n"
    )

    update_schedule_interval(path, "30m")

    content = path.read_text()
    assert 'interval = "30m"' in content
    assert "max_files = 4" in content
    assert ConfigurationManager(path).load().collection.schedule.interval == "30m"


def test_source_id_must_be_a_uuid(tmp_path: Path) -> None:
    """Reject an unparsable source namespace before opening a backend."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "not-a-uuid"\n'
        'backend.provider = "duckdb"\n'
        'backend.duckdb.database = ":memory:"\n'
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
        'backend.provider = "duckdb"\n'
        'backend.duckdb.database = ":memory:"\n'
    )
    if scope == "root":
        content += "unexpected_setting = true\n"
    content += f'\n[logging]\ndirectory = "{log_directory}"\n'
    if scope == "tokscale":
        content += "\n[tokscale]\nunexpected_setting = true\n"
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
        'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n'
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
        'backend.provider = "bigquery"\n'
        '[backend.bigquery]\nproject = "usagebassoon-test"\n'
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
        'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n'
        '[snapshots.schedule]\ninterval = "12h"\n'
    )
    snapshots = ConfigurationManager(valid).load().snapshots
    assert snapshots is not None
    assert snapshots.max_snapshots == 3
    invalid = tmp_path / "invalid.toml"
    invalid.write_text(valid.read_text().replace('"12h"', '"zero"'))
    with pytest.raises(ConfigurationError, match=r"snapshots\.schedule\.interval"):
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

    assert ConfigurationManager(environ={}).path == default_config_path()
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
        'backend.provider = "duckdb"\nbackend.duckdb.database = ":memory:"\n'
        '[snapshots.gcs]\nenable = true\nuri = "gs://bucket/archive"\n'
        'project = "usagebassoon-test"\n'
        f'credentials_file = "{credentials}"\n'
        "[snapshots.local]\n"
        f'path = "{tmp_path / "snapshots"}"\n'
        '[snapshots]\nmax_snapshots = 5\n[snapshots.schedule]\ninterval = "12h"\n'
    )

    config = ConfigurationManager(path).load()

    assert config.snapshots.gcs is not None
    assert config.snapshots.gcs.uri == "gs://bucket/archive"
    assert config.snapshots.gcs.project == "usagebassoon-test"
    assert config.snapshots.gcs.timeout_seconds == 60.0
    assert config.snapshots.gcs.credentials_file == credentials
    assert config.snapshots is not None
    assert config.snapshots.local.path == tmp_path / "snapshots"
    assert config.snapshots.max_snapshots == 5


def test_namespaced_configuration_defaults(tmp_path: Path) -> None:
    """Keep automatic archives opt-in and retain the twelve-hour cadence."""
    path = tmp_path / "config.toml"
    path.write_text('source_id = "11111111-1111-4111-8111-111111111111"\n')
    config = ConfigurationManager(path).load()
    assert config.backend == "duckdb"
    assert config.local_database == default_local_database_path()
    assert config.collection.schedule.interval == "15m"
    assert not config.snapshots.local.enable
    assert config.snapshots.local.path == default_snapshot_directory()
    assert config.snapshots.gcs is None
    assert config.snapshots.schedule.interval == "12h"
    assert not config.snapshots.local.disable_weekly
    assert not config.logging.disable


@pytest.mark.parametrize(
    "setting",
    [
        '[logging]\ndisable = "true"',
        "[snapshots.local]\nenable = 1",
        '[snapshots.gcs]\nenable = "false"',
        "[snapshots.local]\ndisable_weekly = 0",
    ],
)
def test_enable_and_disable_flags_require_booleans(
    tmp_path: Path, setting: str
) -> None:
    """Reject ambiguous flag values before constructing storage adapters."""
    path = tmp_path / "config.toml"
    path.write_text('source_id = "11111111-1111-4111-8111-111111111111"\n' + setting)
    with pytest.raises(ConfigurationError, match="must be a Boolean"):
        ConfigurationManager(path).load()


@pytest.mark.parametrize("enabled", [False, True])
def test_snapshot_destination_enablement(tmp_path: Path, enabled: bool) -> None:
    """Disabled GCS requires no credentials or destination settings."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        f"[snapshots.local]\nenable = {str(enabled).lower()}\n"
        "[snapshots.gcs]\nenable = false\n"
    )
    config = ConfigurationManager(path).load()
    assert config.snapshots.enabled is enabled
    assert config.snapshots.gcs is not None and not config.snapshots.gcs.enable


@pytest.mark.parametrize(
    ("literal", "expected"),
    [
        ("'~/my snapshots'", Path("~/my snapshots")),
        ("'relative snapshots'", Path("relative snapshots")),
        ("'file:///archive'", Path("/archive")),
        ("'file://~/my snapshots'", Path("~/my snapshots")),
        (
            r"'file://C:\Users\alice\UsageBassoon\snapshots'",
            Path(r"C:\Users\alice\UsageBassoon\snapshots"),
        ),
        (
            r"'C:\Users\alice\UsageBassoon\snapshots'",
            Path(r"C:\Users\alice\UsageBassoon\snapshots"),
        ),
    ],
)
def test_local_snapshot_paths(tmp_path: Path, literal: str, expected: Path) -> None:
    """Accept filesystem paths and file:// alternatives, including Windows paths."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        f"[snapshots.local]\npath = {literal}\n"
    )
    assert (
        ConfigurationManager(path).load().snapshots.local.path == expected.expanduser()
    )


@pytest.mark.parametrize(
    "value",
    [
        '"gs://bucket/archive"',
        '"https://host/archive"',
        '"file://"',
        '"file://   "',
        "123",
        '""',
    ],
)
def test_local_snapshot_path_requires_a_filesystem_path(
    tmp_path: Path, value: str
) -> None:
    """Reject non-file URIs, empty destinations, and invalid path values."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        f"[snapshots.local]\npath = {value}\n"
    )
    with pytest.raises(ConfigurationError, match=r"snapshots\.local\.path"):
        ConfigurationManager(path).load()


@pytest.mark.parametrize("existing", [True, False])
def test_update_snapshot_interval_preserves_collection_and_policy(
    tmp_path: Path, existing: bool
) -> None:
    """Persist snapshot cadence without changing collection, flags, or file mode."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        '[collection.schedule]\ninterval = "30m"\n'
        '[snapshots.local]\npath = "my archive"\n'
        + ("disable_weekly = true\n" if existing else "")
        + ('[snapshots.schedule]\ninterval = "12h"\n' if existing else "")
    )
    path.chmod(0o600)
    update_schedule_interval(path, "2d", domain="snapshots")
    config = ConfigurationManager(path).load()
    assert config.collection.schedule.interval == "30m"
    assert config.snapshots.schedule.interval == "2d"
    assert config.snapshots.local.disable_weekly is existing
    assert config.snapshots.local.path == Path("my archive")
    assert path.stat().st_mode & 0o777 == 0o600


def test_disabled_cloud_keeps_authentication_and_independent_weekly_settings(
    tmp_path: Path,
) -> None:
    """Disable publication without discarding credentials for explicit recovery."""
    path = tmp_path / "config.toml"
    credentials = tmp_path / "credentials.json"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        "[snapshots.local]\nenable = true\ndisable_weekly = true\n"
        "[snapshots.gcs]\nenable = false\ndisable_weekly = false\n"
        f'project = "recovery-project"\ncredentials_file = "{credentials}"\n'
    )
    config = ConfigurationManager(path).load()
    assert config.snapshots.local.disable_weekly
    cloud = config.snapshots.gcs
    assert cloud is not None and not cloud.enable and not cloud.disable_weekly
    assert cloud.project == "recovery-project" and cloud.credentials_file == credentials


def test_disabled_logging_applies_to_configuration_errors(tmp_path: Path) -> None:
    """Explicit opt-out prevents error logs even for an invalid configuration."""
    path = tmp_path / "config.toml"
    directory = tmp_path / "disabled-logs"
    path.write_text(
        f'source_id = "invalid"\n[logging]\ndisable = true\ndirectory = "{directory}"\n'
    )
    with pytest.raises(ConfigurationError, match="source_id") as captured:
        ConfigurationManager(path).load()
    assert "logged to" not in str(captured.value)
    assert not directory.exists()


def test_failed_preflight_preserves_error_when_close_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Backend cleanup cannot hide a schema or connection preflight failure."""
    from usagebassoon.backends.duckdb_local import DuckDBBackend
    from usagebassoon.backends.factory import open_backend
    from usagebassoon.config import UsageBassoonConfig

    failure = RuntimeError("primary preflight failure")
    backend = DuckDBBackend(":memory:")
    connection = backend.connection

    def failed_preflight() -> None:
        raise failure

    def failed_close() -> None:
        raise OSError("secondary close failure")

    def opened_backend(_path: Path) -> DuckDBBackend:
        """Return the backend whose preflight and cleanup are under test."""
        return backend

    monkeypatch.setattr(
        "usagebassoon.backends.duckdb_local.DuckDBBackend", opened_backend
    )
    monkeypatch.setattr(backend, "preflight", failed_preflight)
    monkeypatch.setattr(backend, "close", failed_close)
    monkeypatch.setattr(logging.getLogger(LOGGER_NAME), "propagate", True)
    monkeypatch.setattr(logging.getLogger(LOGGER_NAME), "disabled", False)
    config = UsageBassoonConfig(
        tmp_path / "config.toml",
        "11111111-1111-4111-8111-111111111111",
        "duckdb",
        local_database=Path(":memory:"),
    )
    try:
        with pytest.raises(RuntimeError) as caught:
            open_backend(config)
        assert caught.value is failure
        assert "secondary close failure" in caplog.text
    finally:
        connection.close()


def test_atomic_config_failure_survives_failed_temporary_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Failed writes retain the original config and report leftover temporary files."""
    target = tmp_path / "config.toml"
    target.write_text("original")
    failure = OSError("primary replace failure")

    def failed_replace(_source: object, _target: object) -> None:
        raise failure

    def failed_unlink(_path: Path, *, missing_ok: bool = False) -> None:
        assert missing_ok
        raise PermissionError("secondary cleanup failure")

    monkeypatch.setattr(config_module.os, "replace", failed_replace)
    monkeypatch.setattr(Path, "unlink", failed_unlink)
    monkeypatch.setattr(logging.getLogger(LOGGER_NAME), "propagate", True)
    monkeypatch.setattr(logging.getLogger(LOGGER_NAME), "disabled", False)
    with pytest.raises(OSError) as caught:
        config_module._atomic_replace(target, "replacement")
    assert caught.value is failure
    assert target.read_text() == "original"
    assert "secondary cleanup failure" in caplog.text
