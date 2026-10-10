# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_logger.py — Rotating local collection diagnostics."""

from __future__ import annotations

import logging
import os
import stat
import time
from pathlib import Path
from types import TracebackType

import pytest

from tests._cli import plain_cli_output
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


def test_log_sink_writes_under_the_configured_state_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist operational errors without requiring a console or cloud service."""
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
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
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
    target = tmp_path / "not-a-directory"
    target.write_text("occupied")

    logger = configure(LoggingConfig(directory=target))
    logger.error("fallback collection error")

    captured = capsys.readouterr()
    assert "WARNING: UsageBassoon logging directory is unavailable" in plain_cli_output(
        captured.err
    )
    assert "operational log" in plain_cli_output(captured.err)
    assert "fallback collection error" in plain_cli_output(captured.err)


def test_logging_disable_closes_sinks_and_can_be_reenabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An explicit opt-out blocks logging even with a directory override."""
    monkeypatch.setenv(LOG_DIRECTORY_ENV_VAR, str(tmp_path / "logs"))
    enabled = LoggingConfig()
    logger = configure(enabled)
    logger.info("first enabled message")
    path = tmp_path / "logs" / "usagebassoon.log"
    original = path.read_bytes()
    configure(LoggingConfig(disable=True)).error("disabled message")
    assert not logger.handlers
    assert path.read_bytes() == original
    configure(enabled).info("reenabled message")
    assert "reenabled message" in path.read_text()
    assert "disabled message" not in path.read_text()


@pytest.mark.parametrize("sink", ["rotating", "fallback", "write_failure"])
def test_operational_sinks_redact_complete_exception_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sink: str,
) -> None:
    """Hide arguments and chained exceptions while retaining stack context."""
    from usagebassoon.config import UsageBassoonConfig
    from usagebassoon.logger import _CredentialFormatter

    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "opaque-backend-credential")
    monkeypatch.setenv("EXTRA_DIAGNOSTIC", "opaque-child-credential")
    directory = tmp_path / "logs"
    if sink == "fallback":
        directory.write_text("occupied")
    logger = configure(
        UsageBassoonConfig(
            path=tmp_path / "config.toml",
            source_id="11111111-1111-4111-8111-111111111111",
            backend="duckdb",
            tokscale_env=("EXTRA_DIAGNOSTIC",),
            logging=LoggingConfig(directory=directory, max_files=2, max_bytes=512),
        )
    )
    handler = next(
        handler
        for handler in logger.handlers
        if isinstance(handler.formatter, _CredentialFormatter)
    )
    if sink == "write_failure":

        def fail_rotation() -> None:
            raise OSError("rotation unavailable")

        monkeypatch.setattr(handler, "doRollover", fail_rotation)
    # Capture-time values remain protected after the environment changes.
    monkeypatch.delenv("EXTRA_DIAGNOSTIC")
    configure(LoggingConfig(directory=directory, max_files=2, max_bytes=512))
    cause = RuntimeError("backend failed: opaque-backend-credential")
    failure = OSError("child failed: opaque-child-credential")
    traceback: TracebackType | None = None
    try:
        try:
            raise cause
        except RuntimeError:
            raise failure from cause
    except OSError as error:
        traceback = error.__traceback__
        record = logging.LogRecord(
            logger.name,
            logging.ERROR,
            __file__,
            1,
            "publication failed %s Authorization: Bearer bearer-credential "
            "details={'password': 'structured-credential'}",
            ("opaque-backend-credential",),
            (type(error), error, traceback),
            sinfo="stack context /workspace/collector.py:42",
        )
        handler.format(record)
        assert record.exc_text is None
        logger.handle(record)
        logger.handle(record)
    if sink == "rotating":
        paths = [directory / "usagebassoon.log", directory / "usagebassoon.log.1"]
        assert all(path.exists() for path in paths)
        output = "\n".join(path.read_text() for path in paths)
    else:
        output = plain_cli_output(capsys.readouterr().err)
    for secret in (
        "opaque-backend-credential",
        "opaque-child-credential",
        "bearer-credential",
        "structured-credential",
    ):
        assert secret not in output
    assert "publication failed" in output
    assert "backend failed" in output
    assert "child failed" in output
    assert "direct cause" in output
    assert __file__ in output
    assert "stack context /workspace/collector.py:42" in output
    assert failure.__cause__ is cause
    assert failure.__traceback__ is traceback
    assert "opaque-child-credential" in str(failure)


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="process timezone switching")
@pytest.mark.parametrize("fallback", [False, True])
def test_operational_timestamps_use_utc_in_non_utc_timezone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    fallback: bool,
) -> None:
    """Both durable and fallback logs must label the actual UTC instant."""
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
    directory = tmp_path / "logs"
    if fallback:
        directory.write_text("occupied")
    record = logging.LogRecord(
        "usagebassoon", logging.ERROR, __file__, 1, "UTC clock", (), None
    )
    record.created = 0
    try:
        with monkeypatch.context() as patch:
            patch.setenv("TZ", "EST5EDT")
            time.tzset()
            assert time.localtime(0).tm_hour != time.gmtime(0).tm_hour
            logger = configure(LoggingConfig(directory=directory))
            logger.handle(record)
            for handler in logger.handlers:
                handler.flush()
            output = (
                capsys.readouterr().err
                if fallback
                else (directory / "usagebassoon.log").read_text()
            )
            assert "1970-01-01T00:00:00Z ERROR usagebassoon UTC clock" in output
    finally:
        time.tzset()


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission modes")
def test_log_files_remain_private_after_rollover_with_permissive_umask(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep every rollover private without changing the process umask."""
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
    shared = tmp_path / "shared"
    shared.mkdir(mode=0o755)
    shared.chmod(0o755)
    directory = shared / "new-parent" / "logs"
    previous = os.umask(0)
    try:
        logger = configure(
            LoggingConfig(directory=directory, max_files=3, max_bytes=128)
        )
        for index in range(5):
            logger.error("private log message %s %s", index, "x" * 100)
        files = list(directory.iterdir())
        assert len(files) == 3
        assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in files)
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
        assert stat.S_IMODE(directory.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(shared.stat().st_mode) == 0o755
        assert os.umask(0) == 0
    finally:
        os.umask(previous)
