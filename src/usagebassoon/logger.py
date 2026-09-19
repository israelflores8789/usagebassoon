# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""logger.py — Privacy-conscious rotating operational logging."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from usagebassoon.config import LoggingConfig

LOGGER_NAME = "usagebassoon"
_FALLBACK_HANDLER_ATTRIBUTE = "_usagebassoon_fallback_handler"


def _ensure_fallback_handler(logger: logging.Logger) -> None:
    """Attach a stderr handler when the configured file log is unavailable."""
    if any(
        getattr(handler, _FALLBACK_HANDLER_ATTRIBUTE, False)
        for handler in logger.handlers
    ):
        return
    handler = logging.StreamHandler(sys.stderr)
    setattr(handler, _FALLBACK_HANDLER_ATTRIBUTE, True)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
    )
    logger.addHandler(handler)


def configure(config: LoggingConfig) -> logging.Logger:
    """Configure the UsageBassoon rotating operational log.

    Args:
        config: Validated file location and retention settings.

    Returns:
        The package logger configured for exception diagnostics.
    """
    directory = config.directory.expanduser()
    log_path = directory / "usagebassoon.log"
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in logger.handlers:
        if isinstance(handler, RotatingFileHandler) and handler.baseFilename == str(
            log_path
        ):
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
        logger.exception(
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
