# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""config.py — Configuration loading and backend construction for UsageBassoon."""

from __future__ import annotations

import os
import re
import tomllib
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import Literal, cast
from uuid import UUID, uuid4

from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.backends.motherduck import MotherDuckBackend

BackendName = Literal["duckdb", "motherduck", "bigquery"]
DEFAULT_CONFIG_PATH = Path("~/.config/usagebassoon/config.toml")
DEFAULT_DUCKDB_DATABASE = "~/.local/share/usagebassoon/usagebassoon.duckdb"
DEFAULT_LOG_DIRECTORY = Path("~/.local/state/usagebassoon/logs")
CONFIG_PATH_ENV_VAR = "USAGEBASSOON_CONFIG"
SUPPORTED_BACKENDS = frozenset({"duckdb", "motherduck", "bigquery"})
_UUID_PATTERN = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_ENVIRONMENT_VARIABLE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_BIGQUERY_LOCATION_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9-]{0,62}\Z")
_ROOT_CONFIG_KEYS = frozenset(
    {
        "source_id",
        "backend",
        "database",
        "tokscale",
        "bigquery",
        "collection",
        "logging",
        "snapshots",
    }
)
_TOKSCALE_CONFIG_KEYS = frozenset(
    {"bin", "env", "timeout_seconds", "max_stdout_bytes", "max_stderr_bytes"}
)
_BIGQUERY_CONFIG_KEYS = frozenset(
    {"project", "location", "credentials_file", "maximum_bytes_billed"}
)
_COLLECTION_CONFIG_KEYS = frozenset({"max_retries", "retry_initial_seconds", "cadence"})
_LOGGING_CONFIG_KEYS = frozenset({"directory", "max_files", "max_bytes"})
_SNAPSHOT_CONFIG_KEYS = frozenset({"gcs_uri", "max_snapshots", "interval"})


class ConfigurationError(ValueError):
    """Raised when the UsageBassoon configuration is absent or invalid."""


def write_initial_config(path: Path) -> bool:
    """Create a default local-DuckDB configuration without overwriting one.

    Args:
        path: Fully resolved target configuration path.

    Returns:
        True when a new configuration was created; otherwise False.

    Raises:
        OSError: If the configuration directory cannot be created or written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    content = (
        f'source_id = "{uuid4()}"\n'
        'backend = "duckdb"\n'
        f'database = "{DEFAULT_DUCKDB_DATABASE}"\n'
    )
    try:
        with path.open("x") as handle:
            handle.write(content)
    except FileExistsError:
        return False
    return True


@dataclass(frozen=True, slots=True)
class BigQueryConfig:
    """BigQuery connection settings.

    Attributes:
        project: GCP project identifier.
        location: BigQuery dataset and job location.
        credentials_file: Optional service-account credential file path.
        maximum_bytes_billed: Per-query billing cap for user-facing reads.
    """

    project: str
    location: str = "US"
    credentials_file: Path | None = None
    maximum_bytes_billed: int = 1_073_741_824


@dataclass(frozen=True, slots=True)
class CollectionConfig:
    """Retry settings for one scheduled collection cycle.

    Attributes:
        max_retries: Additional persistence attempts after the first failure.
        retry_initial_seconds: Initial exponential-backoff delay.
        cadence: Optional external collection cadence used to bound subprocesses.
    """

    max_retries: int = 3
    retry_initial_seconds: float = 1.0
    cadence: str | None = None


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    """Local operational log settings for scheduled collection failures.

    Attributes:
        directory: Directory containing the active and rotated log files.
        max_files: Number of retained log files including the active file.
        max_bytes: Maximum size of the active file before rotation.
    """

    directory: Path = DEFAULT_LOG_DIRECTORY
    max_files: int = 10
    max_bytes: int = 10 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class SnapshotConfig:
    """Optional snapshot settings from the configuration file.

    Attributes:
        gcs_uri: Optional remote snapshot prefix.
        max_snapshots: Maximum retained snapshots.
        interval: Optional collection interval.
    """

    gcs_uri: str | None = None
    max_snapshots: int = 3
    interval: str | None = None


@dataclass(frozen=True, slots=True)
class UsageBassoonConfig:
    """Validated settings used by the library and CLI.

    Attributes:
        path: Configuration file from which these values were loaded.
        source_id: Stable UUID namespace for one intentional collection source.
        backend: Selected storage backend.
        database: Backend database, dataset, or local file path.
        tokscale_bin: Optional tokscale executable override.
        tokscale_env: Explicit parent environment variables passed to tokscale.
        tokscale_timeout_seconds: Maximum duration for one tokscale command.
        tokscale_max_stdout_bytes: Maximum captured tokscale standard output.
        tokscale_max_stderr_bytes: Maximum captured tokscale standard error.
        bigquery: BigQuery settings when that backend is selected.
        collection: Collection retry settings.
        logging: Local operational logging settings.
        snapshots: Optional snapshot settings.
    """

    path: Path
    source_id: str
    backend: BackendName
    database: str
    tokscale_bin: str | None = None
    tokscale_env: tuple[str, ...] = ()
    tokscale_timeout_seconds: float = 300.0
    tokscale_max_stdout_bytes: int = 64 * 1024 * 1024
    tokscale_max_stderr_bytes: int = 8 * 1024 * 1024
    bigquery: BigQueryConfig | None = None
    collection: CollectionConfig = CollectionConfig()
    logging: LoggingConfig = LoggingConfig()
    snapshots: SnapshotConfig | None = None


def _reject_unknown_keys(
    value: Mapping[str, object], name: str, allowed_keys: frozenset[str]
) -> None:
    """Reject keys that are not part of a supported configuration table."""
    unknown = sorted(set(value) - allowed_keys)
    if unknown:
        scope = "the root configuration table" if name == "root" else f"[{name}]"
        formatted = ", ".join(repr(key) for key in unknown)
        raise ConfigurationError(f"unknown key(s) in {scope}: {formatted}")


def _table(
    value: object | None, name: str, allowed_keys: frozenset[str]
) -> dict[str, object]:
    """Return a TOML table or an empty mapping when it is absent."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigurationError(f"[{name}] must be a TOML table")
    table = cast(dict[str, object], value)
    _reject_unknown_keys(table, name, allowed_keys)
    return table


def _string(value: object | None, name: str, *, required: bool = False) -> str | None:
    """Validate a nullable TOML string."""
    if value is None:
        if required:
            raise ConfigurationError(f"{name} is required")
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{name} must be a non-empty string")
    return value


def _snapshot_config(value: object | None) -> SnapshotConfig | None:
    """Parse optional snapshot settings."""
    if value is None:
        return None
    table = _table(value, "snapshots", _SNAPSHOT_CONFIG_KEYS)
    gcs_uri = _string(table.get("gcs_uri"), "snapshots.gcs_uri")
    interval = _string(table.get("interval"), "snapshots.interval")
    max_snapshots = table.get("max_snapshots", 3)
    if not isinstance(max_snapshots, int) or isinstance(max_snapshots, bool):
        raise ConfigurationError("snapshots.max_snapshots must be an integer")
    if max_snapshots < 1:
        raise ConfigurationError("snapshots.max_snapshots must be positive")
    if interval is not None:
        if interval != interval.strip():
            raise ConfigurationError("snapshots.interval must be like 30m, 12h, or 7d")
        from usagebassoon.snapshots import parse_interval

        try:
            parse_interval(interval)
        except ValueError as error:
            raise ConfigurationError(str(error)) from error
    return SnapshotConfig(
        gcs_uri=gcs_uri,
        max_snapshots=max_snapshots,
        interval=interval,
    )


def _positive_int(value: object, name: str) -> int:
    """Return a strictly positive TOML integer.

    Args:
        value: Decoded TOML value.
        name: Fully qualified configuration field name.

    Raises:
        ConfigurationError: If the value is not a positive integer.
    """
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ConfigurationError(f"{name} must be a positive integer")
    return value


def _environment_names(value: object | None) -> tuple[str, ...]:
    """Validate explicit tokscale environment variable names."""
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigurationError("tokscale.env must be a list of variable names")
    names = tuple(value)
    if len(set(names)) != len(names):
        raise ConfigurationError("tokscale.env must not contain duplicate names")
    if any(_ENVIRONMENT_VARIABLE_PATTERN.fullmatch(name) is None for name in names):
        raise ConfigurationError("tokscale.env contains an invalid variable name")
    return names


def _tokscale_config(
    value: object | None,
) -> tuple[str | None, tuple[str, ...], float, int, int]:
    """Parse restricted tokscale subprocess settings."""
    table = _table(value, "tokscale", _TOKSCALE_CONFIG_KEYS)
    timeout_seconds = table.get("timeout_seconds", 300.0)
    if (
        not isinstance(timeout_seconds, (int, float))
        or isinstance(timeout_seconds, bool)
        or not isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise ConfigurationError("tokscale.timeout_seconds must be a positive number")
    return (
        _string(table.get("bin"), "tokscale.bin"),
        _environment_names(table.get("env")),
        float(timeout_seconds),
        _positive_int(
            table.get("max_stdout_bytes", 64 * 1024 * 1024),
            "tokscale.max_stdout_bytes",
        ),
        _positive_int(
            table.get("max_stderr_bytes", 8 * 1024 * 1024),
            "tokscale.max_stderr_bytes",
        ),
    )


def _collection_config(value: object | None) -> CollectionConfig:
    """Parse optional collection retry settings."""
    table = _table(value, "collection", _COLLECTION_CONFIG_KEYS)
    max_retries = table.get("max_retries", 3)
    retry_initial_seconds = table.get("retry_initial_seconds", 1.0)
    cadence = _string(table.get("cadence"), "collection.cadence")
    if (
        not isinstance(max_retries, int)
        or isinstance(max_retries, bool)
        or max_retries < 0
    ):
        raise ConfigurationError(
            "collection.max_retries must be a non-negative integer"
        )
    if (
        not isinstance(retry_initial_seconds, (int, float))
        or isinstance(retry_initial_seconds, bool)
        or not isfinite(retry_initial_seconds)
        or retry_initial_seconds <= 0
    ):
        raise ConfigurationError(
            "collection.retry_initial_seconds must be a positive number"
        )
    if cadence is not None:
        from usagebassoon.snapshots import parse_interval

        try:
            parsed_cadence = parse_interval(cadence)
        except ValueError as error:
            raise ConfigurationError(
                "collection.cadence must be like 30m, 12h, or 7d"
            ) from error
        if parsed_cadence is None:
            raise ConfigurationError("collection.cadence must be like 30m, 12h, or 7d")
    return CollectionConfig(
        max_retries=max_retries,
        retry_initial_seconds=float(retry_initial_seconds),
        cadence=cadence,
    )


def _logging_config(value: object | None) -> LoggingConfig:
    """Parse optional local rotating-log settings."""
    table = _table(value, "logging", _LOGGING_CONFIG_KEYS)
    directory = _string(table.get("directory"), "logging.directory")
    max_files = _positive_int(table.get("max_files", 10), "logging.max_files")
    max_bytes = _positive_int(
        table.get("max_bytes", 10 * 1024 * 1024), "logging.max_bytes"
    )
    return LoggingConfig(
        directory=Path(directory).expanduser()
        if directory
        else DEFAULT_LOG_DIRECTORY.expanduser(),
        max_files=max_files,
        max_bytes=max_bytes,
    )


def _bigquery_config(value: object | None) -> BigQueryConfig | None:
    """Parse optional BigQuery settings."""
    if value is None:
        return None
    table = _table(value, "bigquery", _BIGQUERY_CONFIG_KEYS)
    project = _string(table.get("project"), "bigquery.project", required=True)
    location = _string(
        table.get("location", "US"),
        "bigquery.location",
        required=True,
    )
    credentials_file = _string(
        table.get("credentials_file"),
        "bigquery.credentials_file",
    )
    if project is None or location is None:
        raise ConfigurationError("bigquery.project and bigquery.location are required")
    if _BIGQUERY_LOCATION_PATTERN.fullmatch(location) is None:
        raise ConfigurationError(
            "bigquery.location must be a canonical location identifier"
        )
    maximum_bytes_billed = _positive_int(
        table.get("maximum_bytes_billed", 1_073_741_824),
        "bigquery.maximum_bytes_billed",
    )
    return BigQueryConfig(
        project=project,
        location=location,
        credentials_file=Path(credentials_file).expanduser()
        if credentials_file
        else None,
        maximum_bytes_billed=maximum_bytes_billed,
    )


def _parse_config(path: Path, payload: Mapping[str, object]) -> UsageBassoonConfig:
    """Validate decoded TOML and create the typed configuration object."""
    _reject_unknown_keys(payload, "root", _ROOT_CONFIG_KEYS)
    source_id = _string(payload.get("source_id"), "source_id", required=True)
    backend_value = _string(payload.get("backend"), "backend", required=True)
    database = _string(payload.get("database"), "database", required=True)
    if backend_value not in SUPPORTED_BACKENDS:
        raise ConfigurationError(
            f"backend must be one of: {', '.join(sorted(SUPPORTED_BACKENDS))}"
        )
    if source_id is None or database is None:
        raise ConfigurationError("source_id and database are required")
    if _UUID_PATTERN.fullmatch(source_id) is None:
        raise ConfigurationError("source_id must be a canonical UUID")
    try:
        canonical_source_id = str(UUID(source_id))
    except ValueError as error:
        raise ConfigurationError("source_id must be a UUID") from error
    (
        tokscale_bin,
        tokscale_env,
        tokscale_timeout_seconds,
        tokscale_max_stdout_bytes,
        tokscale_max_stderr_bytes,
    ) = _tokscale_config(payload.get("tokscale"))
    bigquery = _bigquery_config(payload.get("bigquery"))
    if backend_value == "bigquery" and bigquery is None:
        raise ConfigurationError("[bigquery] is required for the BigQuery backend")
    return UsageBassoonConfig(
        path=path,
        source_id=canonical_source_id,
        backend=cast(BackendName, backend_value),
        database=database,
        tokscale_bin=tokscale_bin,
        tokscale_env=tokscale_env,
        tokscale_timeout_seconds=tokscale_timeout_seconds,
        tokscale_max_stdout_bytes=tokscale_max_stdout_bytes,
        tokscale_max_stderr_bytes=tokscale_max_stderr_bytes,
        bigquery=bigquery,
        collection=_collection_config(payload.get("collection")),
        logging=_logging_config(payload.get("logging")),
        snapshots=_snapshot_config(payload.get("snapshots")),
    )


def _configuration_error_with_log(
    path: Path,
    error: ConfigurationError,
    payload: object | None,
) -> ConfigurationError:
    """Log one configuration error and add the log location to its message."""
    log_config = LoggingConfig(directory=DEFAULT_LOG_DIRECTORY.expanduser())
    if isinstance(payload, Mapping):
        with suppress(ConfigurationError):
            log_config = _logging_config(payload.get("logging"))

    default_log_config = LoggingConfig(directory=DEFAULT_LOG_DIRECTORY.expanduser())
    candidates = [log_config]
    if log_config != default_log_config:
        candidates.append(default_log_config)

    from usagebassoon.logger import log_configuration_error

    last_error: OSError | None = None
    attempted_path = DEFAULT_LOG_DIRECTORY.expanduser() / "usagebassoon.log"
    for candidate in candidates:
        attempted_path = candidate.directory.expanduser() / "usagebassoon.log"
        try:
            log_configuration_error(
                f"configuration file {path} is invalid: {error}",
                directory=candidate.directory,
                max_files=candidate.max_files,
                max_bytes=candidate.max_bytes,
            )
        except OSError as log_error:
            last_error = log_error
            continue
        return ConfigurationError(f"{error} (details logged to {attempted_path})")

    return ConfigurationError(
        f"{error} (could not write the configuration error log at "
        f"{attempted_path}: {last_error})"
    )


def open_backend(config: UsageBassoonConfig) -> StorageBackend:
    """Open the backend selected by a validated configuration.

    Args:
        config: Validated UsageBassoon settings.

    Returns:
        An open storage backend owned by the caller.

    Raises:
        ValueError: If required backend-specific settings are absent.
    """
    if config.backend == "duckdb":
        return DuckDBBackend(config.database)
    if config.backend == "motherduck":
        return MotherDuckBackend(config.database)
    if config.bigquery is None:
        raise ValueError("BigQuery settings are missing")
    from usagebassoon.backends.bigquery import BigQueryBackend

    return BigQueryBackend(
        config.bigquery.project,
        config.database,
        location=config.bigquery.location,
        credentials_file=config.bigquery.credentials_file,
        maximum_bytes_billed=config.bigquery.maximum_bytes_billed,
    )


class ConfigurationManager:
    """Resolve and load one immutable UsageBassoon configuration.

    Explicit ``--config`` paths take precedence over ``USAGEBASSOON_CONFIG``;
    otherwise the manager uses ``~/.config/usagebassoon/config.toml``. No
    No source, backend, or database setting can be overridden independently.
    """

    def __init__(
        self,
        config_path: Path | None = None,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        """Create a manager with optional explicit path and environment.

        Args:
            config_path: Explicit configuration file, normally from ``--config``.
            environ: Environment mapping, injectable for tests.
        """
        self._config_path = config_path
        self._environ = environ

    @property
    def path(self) -> Path:
        """Return the resolved configuration path."""
        if self._config_path is not None:
            return self._config_path.expanduser()
        environ: Mapping[str, str] = (
            self._environ if self._environ is not None else os.environ
        )
        configured = environ.get(CONFIG_PATH_ENV_VAR)
        if configured:
            return Path(configured).expanduser()
        return DEFAULT_CONFIG_PATH.expanduser()

    def load(self) -> UsageBassoonConfig:
        """Load and validate the resolved configuration file.

        Returns:
            Typed, immutable configuration settings.

        Raises:
            ConfigurationError: If the file is unreadable, invalid TOML, or has
                unsupported or invalid configuration values. Details are logged
                to the operational log when it is writable.
        """
        path = self.path
        decoded: object | None = None
        try:
            decoded = tomllib.loads(path.read_text())
        except FileNotFoundError as error:
            failure = ConfigurationError(f"configuration file not found: {path}")
            raise _configuration_error_with_log(path, failure, decoded) from error
        except OSError as error:
            failure = ConfigurationError(
                f"could not read configuration {path}: {error}"
            )
            raise _configuration_error_with_log(path, failure, decoded) from error
        except UnicodeError as error:
            failure = ConfigurationError(f"configuration is not valid UTF-8: {path}")
            raise _configuration_error_with_log(path, failure, decoded) from error
        except tomllib.TOMLDecodeError as error:
            failure = ConfigurationError(
                f"invalid TOML in configuration {path}: {error}"
            )
            raise _configuration_error_with_log(path, failure, decoded) from error
        if not isinstance(decoded, dict):
            failure = ConfigurationError("configuration root must be a TOML table")
            raise _configuration_error_with_log(path, failure, decoded)
        try:
            return _parse_config(path, cast(dict[str, object], decoded))
        except ConfigurationError as error:
            raise _configuration_error_with_log(path, error, decoded) from error
