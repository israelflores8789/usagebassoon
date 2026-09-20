# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_logger.py — Rotating local collection diagnostics."""

from __future__ import annotations

from pathlib import Path

import pytest

from usagebassoon.config import LoggingConfig
from usagebassoon.logger import LOG_DIRECTORY_ENV_VAR, configure


def test_log_directory_environment_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prefer the environment-selected directory over logging.directory."""
    override = tmp_path / "environment-logs"
    monkeypatch.setenv(LOG_DIRECTORY_ENV_VAR, str(override))

    logger = configure(LoggingConfig(directory=tmp_path / "configured-logs"))
    logger.error("environment log directory")
    for handler in logger.handlers:
        handler.flush()

    assert "environment log directory" in (override / "usagebassoon.log").read_text()
    assert not (tmp_path / "configured-logs" / "usagebassoon.log").exists()


def test_rotating_log_sink_writes_under_the_configured_state_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist operational errors without requiring a console or cloud service."""
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR)
    logger = configure(LoggingConfig(directory=tmp_path, max_files=2, max_bytes=128))
    logger.error("warehouse cycle failed")
    for handler in logger.handlers:
        handler.flush()
    assert "warehouse cycle failed" in (tmp_path / "usagebassoon.log").read_text()


def test_unavailable_log_directory_falls_back_to_stderr(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep error reporting alive when the configured log path is unusable."""
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR)
    target = tmp_path / "not-a-directory"
    target.write_text("occupied")

    logger = configure(LoggingConfig(directory=target))
    logger.error("fallback collection error")

    captured = capsys.readouterr()
    assert "WARNING: UsageBassoon logging directory is unavailable" in captured.err
    assert "operational log" in captured.err
    assert "fallback collection error" in captured.err
