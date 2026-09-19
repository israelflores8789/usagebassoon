# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""logger.py — Privacy-conscious rotating operational logging."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from usagebassoon.config import LoggingConfig

LOGGER_NAME = "usagebassoon"


def configure(config: LoggingConfig) -> logging.Logger:
    """Configure the UsageBassoon rotating operational log.

    Args:
        config: Validated file location and retention settings.

    Returns:
        The package logger configured for exception diagnostics.
    """
    directory = config.directory.expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / "usagebassoon.log"
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in logger.handlers:
        if isinstance(handler, RotatingFileHandler) and handler.baseFilename == str(
            log_path
        ):
            return logger
    handler = RotatingFileHandler(
        log_path,
        maxBytes=config.max_bytes,
        backupCount=config.max_files - 1,
        encoding="utf-8",
    )
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
