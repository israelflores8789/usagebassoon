# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_logger.py — Rotating local collection diagnostics."""

from __future__ import annotations

from pathlib import Path

from usagebassoon.config import LoggingConfig
from usagebassoon.logger import configure


def test_rotating_log_sink_writes_under_the_configured_state_directory(
    tmp_path: Path,
) -> None:
    """Persist operational errors without requiring a console or cloud service."""
    logger = configure(LoggingConfig(directory=tmp_path, max_files=2, max_bytes=128))
    logger.error("warehouse cycle failed")
    for handler in logger.handlers:
        handler.flush()
    assert "warehouse cycle failed" in (tmp_path / "usagebassoon.log").read_text()
