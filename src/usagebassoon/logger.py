# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""logger.py — Privacy-conscious rotating operational logging."""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TextIO, override

from usagebassoon.config import LoggingConfig

LOGGER_NAME = "usagebassoon"
LOG_DIRECTORY_ENV_VAR = "USAGEBASSOON_LOG_DIRECTORY"
_FALLBACK_HANDLER_ATTRIBUTE = "_usagebassoon_fallback_handler"


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
        """Suppress only closed-stream errors from ephemeral terminal captures."""
        if getattr(self.stream, "closed", False):
            return
        super().handleError(record)


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
        logging.Formatter(
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


def configure(config: LoggingConfig) -> logging.Logger:
    """Configure the UsageBassoon rotating operational log.

    Args:
        config: Validated file location and retention settings.

    Returns:
        The package logger configured for exception diagnostics.
    """
    directory = _log_directory(config)
    log_path = directory / "usagebassoon.log"
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in logger.handlers:
        if isinstance(handler, RotatingFileHandler) and handler.baseFilename == str(
            log_path
        ):
            _remove_fallback_handlers(logger)
            return logger
    try:
        directory.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
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
        logger.error(
            "could not configure the operational log at %s; using stderr",
            log_path,
        )
        return logger
    handler.setFormatter(
        logging.Formatter(
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
