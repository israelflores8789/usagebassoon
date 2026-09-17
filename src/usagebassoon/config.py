# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""config.py — Configuration loading and backend construction for UsageBassoon."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
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
    """

    project: str
    location: str = "US"
    credentials_file: Path | None = None


@dataclass(frozen=True, slots=True)
class CollectionConfig:
    """Retry settings for one scheduled collection cycle.

    Attributes:
        max_retries: Additional persistence attempts after the first failure.
        retry_initial_seconds: Initial exponential-backoff delay.
    """

    max_retries: int = 3
    retry_initial_seconds: float = 1.0


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
    bigquery: BigQueryConfig | None = None
    collection: CollectionConfig = CollectionConfig()
    logging: LoggingConfig = LoggingConfig()
    snapshots: SnapshotConfig | None = None


def _table(value: object | None, name: str) -> dict[str, object]:
    """Return a TOML table or an empty mapping when it is absent."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigurationError(f"[{name}] must be a TOML table")
    return cast(dict[str, object], value)


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
    table = _table(value, "snapshots")
    gcs_uri = _string(table.get("gcs_uri"), "snapshots.gcs_uri")
    interval = _string(table.get("interval"), "snapshots.interval")
    max_snapshots = table.get("max_snapshots", 3)
    if not isinstance(max_snapshots, int) or isinstance(max_snapshots, bool):
        raise ConfigurationError("snapshots.max_snapshots must be an integer")
    if max_snapshots < 1:
        raise ConfigurationError("snapshots.max_snapshots must be positive")
    if interval is not None:
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


def _collection_config(value: object | None) -> CollectionConfig:
    """Parse optional collection retry settings."""
    table = _table(value, "collection")
    max_retries = table.get("max_retries", 3)
    retry_initial_seconds = table.get("retry_initial_seconds", 1.0)
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
        or retry_initial_seconds <= 0
    ):
        raise ConfigurationError(
            "collection.retry_initial_seconds must be a positive number"
        )
    return CollectionConfig(
        max_retries=max_retries,
        retry_initial_seconds=float(retry_initial_seconds),
    )


def _logging_config(value: object | None) -> LoggingConfig:
    """Parse optional local rotating-log settings."""
    table = _table(value, "logging")
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
    table = _table(value, "bigquery")
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
    return BigQueryConfig(
        project=project,
        location=location,
        credentials_file=Path(credentials_file).expanduser()
        if credentials_file
        else None,
    )


def _parse_config(path: Path, payload: Mapping[str, object]) -> UsageBassoonConfig:
    """Validate decoded TOML and create the typed configuration object."""
    source_id = _string(payload.get("source_id"), "source_id", required=True)
    backend_value = _string(payload.get("backend"), "backend", required=True)
    database = _string(payload.get("database"), "database", required=True)
    if backend_value not in SUPPORTED_BACKENDS:
        raise ConfigurationError(
            f"backend must be one of: {', '.join(sorted(SUPPORTED_BACKENDS))}"
        )
    if source_id is None or database is None:
        raise ConfigurationError("source_id and database are required")
    try:
        canonical_source_id = str(UUID(source_id))
    except ValueError as error:
        raise ConfigurationError("source_id must be a UUID") from error
    tokscale = _table(payload.get("tokscale"), "tokscale")
    tokscale_bin = _string(tokscale.get("bin"), "tokscale.bin")
    bigquery = _bigquery_config(payload.get("bigquery"))
    if backend_value == "bigquery" and bigquery is None:
        raise ConfigurationError("[bigquery] is required for the BigQuery backend")
    return UsageBassoonConfig(
        path=path,
        source_id=canonical_source_id,
        backend=cast(BackendName, backend_value),
        database=database,
        tokscale_bin=tokscale_bin,
        bigquery=bigquery,
        collection=_collection_config(payload.get("collection")),
        logging=_logging_config(payload.get("logging")),
        snapshots=_snapshot_config(payload.get("snapshots")),
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
            ConfigurationError: If the file cannot be read or is invalid TOML.
        """
        path = self.path
        try:
            decoded = tomllib.loads(path.read_text())
        except FileNotFoundError as error:
            raise ConfigurationError(f"configuration file not found: {path}") from error
        except OSError as error:
            raise ConfigurationError(
                f"could not read configuration {path}: {error}"
            ) from error
        except tomllib.TOMLDecodeError as error:
            raise ConfigurationError(
                f"invalid TOML in configuration {path}: {error}"
            ) from error
        if not isinstance(decoded, dict):
            raise ConfigurationError("configuration root must be a TOML table")
        return _parse_config(path, cast(dict[str, object], decoded))
