# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""config.py — Configuration loading and backend construction for UsageBassoon."""

from __future__ import annotations

import logging
import os
import re
import stat
import tempfile
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from math import isfinite
from pathlib import Path
from typing import Literal, cast
from urllib.parse import urlparse
from uuid import UUID, uuid4

from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.backends.motherduck import MotherDuckBackend

_LOG = logging.getLogger("usagebassoon")

BackendName = Literal["duckdb", "motherduck", "bigquery"]
DEFAULT_CONFIG_PATH = Path("~/.config/usagebassoon/config.toml")
DEFAULT_DUCKDB_DATABASE = "~/.local/share/usagebassoon/usagebassoon.duckdb"
DEFAULT_LOG_DIRECTORY = Path("~/.local/state/usagebassoon/logs")
CONFIG_PATH_ENV_VAR = "USAGEBASSOON_CONFIG"
SUPPORTED_BACKENDS = frozenset({"duckdb", "motherduck", "bigquery"})
_INTERVAL = re.compile(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>[smhdw])\Z", re.I)
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
        "gcs",
        "collection",
        "schedule",
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
_COLLECTION_CONFIG_KEYS = frozenset({"max_retries", "retry_initial_seconds", "timeout"})
_SCHEDULE_CONFIG_KEYS = frozenset({"interval"})
_LOGGING_CONFIG_KEYS = frozenset({"directory", "max_files", "max_bytes"})
_GCS_CONFIG_KEYS = frozenset({"uri", "project", "location", "credentials_file"})
_SNAPSHOT_CONFIG_KEYS = frozenset({"file_uri", "max_snapshots", "interval"})
DEFAULT_SCHEDULE_INTERVAL = "15m"


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
        "\n[schedule]\n"
        f'interval = "{DEFAULT_SCHEDULE_INTERVAL}"\n'
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
class GcsConfig:
    """Google Cloud Storage snapshot settings.

    Attributes:
        uri: GCS archive root used for snapshots.
        project: GCP project identifier used by the Storage client.
        location: Configured GCS bucket location metadata.
        credentials_file: Optional service-account credential file path.
    """

    uri: str
    project: str
    location: str = "US"
    credentials_file: Path | None = None


@dataclass(frozen=True, slots=True)
class CollectionConfig:
    """Retry settings for one scheduled collection cycle.

    Attributes:
        max_retries: Additional persistence attempts after the first failure.
        retry_initial_seconds: Initial exponential-backoff delay.
        timeout: Optional end-to-end collection deadline.
    """

    max_retries: int = 3
    retry_initial_seconds: float = 1.0
    timeout: str | None = None


@dataclass(frozen=True, slots=True)
class ScheduleConfig:
    """Configuration for the external or container collection scheduler.

    Attributes:
        interval: Positive duration between scheduled collection attempts.
    """

    interval: str = DEFAULT_SCHEDULE_INTERVAL


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
        file_uri: Optional local snapshot archive path or URI.
        max_snapshots: Maximum retained snapshots.
        interval: Optional collection interval.
    """

    file_uri: str | None = None
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
        gcs: Google Cloud Storage settings when GCS snapshots are configured.
        schedule: Scheduler interval settings.
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
    gcs: GcsConfig | None = None
    schedule: ScheduleConfig = ScheduleConfig()
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
    file_uri = _string(table.get("file_uri"), "snapshots.file_uri")
    if file_uri is not None:
        parsed = urlparse(file_uri)
        if parsed.scheme not in {"", "file"} or (
            parsed.scheme == "file" and not parsed.path
        ):
            raise ConfigurationError(
                "snapshots.file_uri must be a local path or file:// URI"
            )
    interval = _string(table.get("interval"), "snapshots.interval")
    max_snapshots = table.get("max_snapshots", 3)
    if not isinstance(max_snapshots, int) or isinstance(max_snapshots, bool):
        raise ConfigurationError("snapshots.max_snapshots must be an integer")
    if max_snapshots < 1:
        raise ConfigurationError("snapshots.max_snapshots must be positive")
    if interval is not None:
        if interval != interval.strip():
            raise ConfigurationError("snapshots.interval must be like 30m, 12h, or 7d")
        try:
            parse_interval(interval)
        except ValueError as error:
            raise ConfigurationError(f"snapshots.interval {error}") from error
    return SnapshotConfig(
        file_uri=file_uri,
        max_snapshots=max_snapshots,
        interval=interval,
    )


def parse_interval(value: str | None) -> timedelta | None:
    """Parse a positive configured interval in compact ``<number><unit>`` form.

    This common parser serves collection timeout, scheduling, and snapshot
    cadence settings; callers provide setting-specific error context.
    """
    if value is None:
        return None
    match = _INTERVAL.fullmatch(value.strip())
    if match is None:
        raise ValueError("interval must be like 30m, 12h, or 7d")
    amount = float(match["value"])
    if amount <= 0:
        raise ValueError("interval must be positive")
    return timedelta(
        seconds=amount
        * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[match["unit"].lower()]
    )


def _duration(value: object | None, name: str) -> tuple[str | None, timedelta | None]:
    """Validate one optional minute/hour duration and return its parsed value."""
    text = _string(value, name)
    if text is None:
        return None, None
    message = f"{name} must be a positive duration in minutes or hours"
    if text != text.strip() or re.fullmatch(r"\d+(?:\.\d+)?[mh]", text, re.I) is None:
        raise ConfigurationError(message)
    try:
        parsed = parse_interval(text)
    except ValueError as error:
        raise ConfigurationError(message) from error
    if parsed is None:
        raise ConfigurationError(message)
    return text, parsed


def _schedule_config(value: object | None) -> ScheduleConfig:
    """Parse the configured scheduler interval."""
    table = _table(value, "schedule", _SCHEDULE_CONFIG_KEYS)
    raw_interval = table.get("interval", DEFAULT_SCHEDULE_INTERVAL)
    interval, parsed = _duration(raw_interval, "schedule.interval")
    if interval is None or parsed is None:
        raise ConfigurationError("schedule.interval must be positive")
    return ScheduleConfig(interval=interval)


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
    timeout, _ = _duration(table.get("timeout"), "collection.timeout")
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
    return CollectionConfig(
        max_retries=max_retries,
        retry_initial_seconds=float(retry_initial_seconds),
        timeout=timeout,
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


def _gcs_config(value: object | None) -> GcsConfig | None:
    """Parse optional Google Cloud Storage snapshot settings."""
    if value is None:
        return None
    table = _table(value, "gcs", _GCS_CONFIG_KEYS)
    uri = _string(table.get("uri"), "gcs.uri", required=True)
    project = _string(table.get("project"), "gcs.project", required=True)
    location = _string(table.get("location", "US"), "gcs.location", required=True)
    credentials_file = _string(table.get("credentials_file"), "gcs.credentials_file")
    if uri is None or project is None or location is None:
        raise ConfigurationError("gcs.uri, gcs.project, and gcs.location are required")
    from usagebassoon.buckets.gcs import parse_gcs_uri

    try:
        parse_gcs_uri(uri)
    except ValueError as error:
        raise ConfigurationError(str(error)) from error
    if _BIGQUERY_LOCATION_PATTERN.fullmatch(location) is None:
        raise ConfigurationError("gcs.location must be a canonical location identifier")
    return GcsConfig(
        uri=uri,
        project=project,
        location=location,
        credentials_file=Path(credentials_file).expanduser()
        if credentials_file
        else None,
    )


def _parse_config(
    path: Path,
    payload: Mapping[str, object],
    *,
    schedule_interval: str | None = None,
) -> UsageBassoonConfig:
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
    gcs = _gcs_config(payload.get("gcs"))
    schedule_payload = payload.get("schedule")
    if schedule_interval is not None:
        if schedule_payload is None:
            schedule_payload = {"interval": schedule_interval}
        elif isinstance(schedule_payload, dict):
            schedule_payload = {**schedule_payload, "interval": schedule_interval}
    schedule = _schedule_config(schedule_payload)
    collection = _collection_config(payload.get("collection"))
    if collection.timeout is not None:
        _, interval_duration = _duration(schedule.interval, "schedule.interval")
        _, timeout_duration = _duration(collection.timeout, "collection.timeout")
        assert interval_duration is not None
        assert timeout_duration is not None
        if interval_duration < timeout_duration:
            raise ConfigurationError(
                "schedule.interval must be greater than or equal to collection.timeout"
            )
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
        gcs=gcs,
        schedule=schedule,
        collection=collection,
        logging=_logging_config(payload.get("logging")),
        snapshots=_snapshot_config(payload.get("snapshots")),
    )


def update_schedule_interval(path: Path, interval: str) -> None:
    """Persist one schedule interval in an existing TOML configuration.

    Args:
        path: Configuration file to update.
        interval: Validated compact duration such as ``15m``.

    Raises:
        OSError: If the configuration cannot be read or atomically replaced.
        ValueError: If the interval contains unsafe TOML text.
    """
    if not re.fullmatch(r"\d+(?:\.\d+)?[mh]", interval, re.IGNORECASE):
        raise ValueError(
            "schedule.interval must be a positive duration in minutes or hours"
        )
    original = path.read_text(encoding="utf-8")
    lines = original.splitlines(keepends=True)
    schedule_start: int | None = None
    schedule_end = len(lines)
    interval_line: int | None = None
    for index, line in enumerate(lines):
        if re.fullmatch(r"\s*\[schedule\]\s*(?:#.*)?(?:\r?\n)?", line):
            schedule_start = index
            continue
        if schedule_start is not None and re.match(r"\s*\[.*\]", line):
            schedule_end = index
            break
        if schedule_start is not None and re.match(r"\s*interval\s*=", line):
            interval_line = index
    replacement = f'interval = "{interval}"\n'
    if interval_line is not None:
        lines[interval_line] = replacement
    elif schedule_start is not None:
        lines.insert(schedule_end, replacement)
    else:
        if lines and not lines[-1].endswith(("\n", "\r")):
            lines[-1] += "\n"
        if lines and lines[-1].strip():
            lines.append("\n")
        lines.extend(["[schedule]\n", replacement])
    _atomic_replace(path, "".join(lines))


def _atomic_replace(path: Path, content: str) -> None:
    """Replace one file atomically while preserving its permission bits."""
    mode = stat.S_IMODE(path.stat().st_mode)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _configuration_error_with_log(
    path: Path,
    error: ConfigurationError,
    payload: object | None,
) -> ConfigurationError:
    """Log one configuration error and add the log location to its message."""
    log_config = LoggingConfig(directory=DEFAULT_LOG_DIRECTORY.expanduser())
    if isinstance(payload, Mapping):
        try:
            log_config = _logging_config(payload.get("logging"))
        except ConfigurationError:
            _LOG.exception(
                "configuration logging settings are invalid; using the default log"
            )

    default_log_config = LoggingConfig(directory=DEFAULT_LOG_DIRECTORY.expanduser())
    candidates = [log_config]
    if log_config != default_log_config:
        candidates.append(default_log_config)

    from usagebassoon.logger import log_configuration_error

    last_error: Exception | None = None
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
        except Exception as log_error:
            last_error = log_error
            _LOG.exception(
                "could not write the configuration error log at %s", attempted_path
            )
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

    def load(self, *, schedule_interval: str | None = None) -> UsageBassoonConfig:
        """Load and validate the resolved configuration file.

        Args:
            schedule_interval: Optional in-memory schedule override used by the
                schedule command before persisting a requested interval.

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
            return _parse_config(
                path,
                cast(dict[str, object], decoded),
                schedule_interval=schedule_interval,
            )
        except ConfigurationError as error:
            raise _configuration_error_with_log(path, error, decoded) from error
