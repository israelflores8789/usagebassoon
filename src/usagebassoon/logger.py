# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""logger.py — Privacy-conscious rotating operational logging."""

from __future__ import annotations

import logging
import os
import sys
from copy import copy
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TextIO, override

from .config import LoggingConfig, UsageBassoonConfig
from .privacy import credential_values, redact_credentials

LOGGER_NAME = "usagebassoon"
LOG_DIRECTORY_ENV_VAR = "USAGEBASSOON_LOG_DIRECTORY"
_FALLBACK_HANDLER_ATTRIBUTE = "_usagebassoon_fallback_handler"
_CREDENTIAL_ENVIRONMENT_NAMES: tuple[str, ...] = ()
_KNOWN_CREDENTIALS: tuple[str, ...] = ()


def _redact(value: str) -> str:
    """Apply the privacy policy using captured and currently active credentials."""
    return redact_credentials(
        value,
        known_values=(
            *_KNOWN_CREDENTIALS,
            *credential_values(
                os.environ, environment_names=_CREDENTIAL_ENVIRONMENT_NAMES
            ),
        ),
    )


class _CredentialFormatter(logging.Formatter):
    """Redact the complete message, stack information, and exception chain."""

    @override
    def format(self, record: logging.LogRecord) -> str:
        """Render a private copy so raw exception caches never alter other sinks."""
        return _redact(super().format(copy(record)))


def worker_diagnostic(message: str) -> None:
    """Write an unattended stderr diagnostic when operational logging is enabled.

    Args:
        message: Rendered worker status or failure detail.
    """
    if not logging.getLogger(LOGGER_NAME).disabled:
        print(_redact(message), file=sys.stderr, flush=True)


class _FallbackStderrHandler(logging.StreamHandler[TextIO]):
    """A stderr handler that tolerates terminal capture streams closing."""

    @override
    def flush(self) -> None:
        """Flush an open stream without surfacing an already-closed capture stream."""
        try:
            super().flush()
        except ValueError:
            return

    @override
    def handleError(self, record: logging.LogRecord) -> None:
        """Avoid logging's default error report, which dumps raw message arguments."""
        if getattr(self.stream, "closed", False):
            return
        if logging.raiseExceptions:
            try:
                print(
                    "UsageBassoon could not write an operational log.", file=sys.stderr
                )
            except (OSError, ValueError):
                return


class _RotatingFileHandler(RotatingFileHandler):
    """Preserve safe diagnostics on stderr if an established file sink fails."""

    @override
    def handleError(self, record: logging.LogRecord) -> None:
        """Use the same safe formatter instead of dumping the original record."""
        handler = _FallbackStderrHandler(sys.stderr)
        handler.setFormatter(self.formatter)
        handler.handle(record)


def _ensure_fallback_handler(logger: logging.Logger) -> None:
    """Attach a stderr handler when the configured file log is unavailable."""
    for handler in tuple(logger.handlers):
        if not getattr(handler, _FALLBACK_HANDLER_ATTRIBUTE, False):
            continue
        if isinstance(handler, logging.StreamHandler) and not getattr(
            handler.stream, "closed", False
        ):
            return
        logger.removeHandler(handler)
        handler.close()
    handler = _FallbackStderrHandler(sys.stderr)
    setattr(handler, _FALLBACK_HANDLER_ATTRIBUTE, True)
    handler.setFormatter(
        _CredentialFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
    )
    logger.addHandler(handler)


def _remove_fallback_handlers(logger: logging.Logger) -> None:
    """Remove transient stderr handlers once a durable file sink is available."""
    for handler in tuple(logger.handlers):
        if getattr(handler, _FALLBACK_HANDLER_ATTRIBUTE, False):
            logger.removeHandler(handler)
            handler.close()


def _log_directory(config: LoggingConfig) -> Path:
    """Resolve the environment override before falling back to configuration."""
    override = os.environ.get(LOG_DIRECTORY_ENV_VAR)
    return Path(override).expanduser() if override else config.directory.expanduser()


def configure(config: LoggingConfig | UsageBassoonConfig) -> logging.Logger:
    """Configure the UsageBassoon rotating operational log.

    Args:
        config: Logging settings or full runtime settings for child diagnostics.

    Returns:
        The package logger configured for exception diagnostics.
    """
    global _CREDENTIAL_ENVIRONMENT_NAMES, _KNOWN_CREDENTIALS
    if isinstance(config, UsageBassoonConfig):
        _CREDENTIAL_ENVIRONMENT_NAMES = tuple(
            dict.fromkeys((*_CREDENTIAL_ENVIRONMENT_NAMES, *config.tokscale_env))
        )
        config = config.logging
    # Reconfiguration must still protect credentials used by in-flight operations.
    _KNOWN_CREDENTIALS = tuple(
        dict.fromkeys(
            (
                *_KNOWN_CREDENTIALS,
                *credential_values(
                    os.environ, environment_names=_CREDENTIAL_ENVIRONMENT_NAMES
                ),
            )
        )
    )
    logger = logging.getLogger(LOGGER_NAME)
    logger.disabled = config.disable
    if config.disable:
        for handler in tuple(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
        return logger
    directory = _log_directory(config)
    log_path = directory / "usagebassoon.log"
    logger.setLevel(logging.INFO)
    logger.propagate = False
    reusable: RotatingFileHandler | None = None
    for handler in tuple(logger.handlers):
        if not isinstance(handler, RotatingFileHandler):
            continue
        if (
            handler.baseFilename == str(log_path.resolve())
            and handler.maxBytes == config.max_bytes
            and handler.backupCount == config.max_files - 1
        ):
            reusable = handler
        else:
            logger.removeHandler(handler)
            handler.close()
    if reusable is not None:
        _remove_fallback_handlers(logger)
        return logger
    try:
        directory.mkdir(parents=True, exist_ok=True)
        handler = _RotatingFileHandler(
            log_path,
            maxBytes=config.max_bytes,
            backupCount=config.max_files - 1,
            encoding="utf-8",
        )
    except OSError:
        _ensure_fallback_handler(logger)
        print(
            "WARNING: UsageBassoon logging directory is unavailable; "
            "operational logs are being written to stderr instead.",
            file=sys.stderr,
        )
        logger.warning(
            "could not configure the operational log at %s; using stderr",
            log_path,
            exc_info=True,
        )
        return logger
    handler.setFormatter(
        _CredentialFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
    )
    _remove_fallback_handlers(logger)
    logger.addHandler(handler)
    return logger


def log_configuration_error(
    message: str,
    *,
    directory: Path,
    max_files: int,
    max_bytes: int,
) -> None:
    """Write one configuration error using the selected rotating log settings.

    Args:
        message: Configuration failure detail to record.
        directory: Directory containing the operational log.
        max_files: Number of retained log files, including the active file.
        max_bytes: Maximum active log file size before rotation.
    """
    config = LoggingConfig(
        directory=directory,
        max_files=max_files,
        max_bytes=max_bytes,
    )
    configure(config).error("%s", message)
