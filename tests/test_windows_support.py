# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_windows_support.py — Required Windows runtime smoke coverage."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests._cli import plain_cli_output
from usagebassoon.collector import (
    TOKSCALE_CLEANUP_TIMEOUT_SECONDS,
    _capture_process,
    _child_environment,
    _start_tokscale_process,
)
from usagebassoon.config import ConfigurationManager
from usagebassoon.system_metadata import capture_system_metadata

_WINDOWS_ONLY = pytest.mark.skipif(os.name != "nt", reason="requires Windows")


def _configuration(tmp_path: Path) -> Path:
    """Write a minimal offline configuration with an isolated DuckDB backend."""
    path = tmp_path / "config.toml"
    path.write_text(
        'source_id = "11111111-1111-4111-8111-111111111111"\n'
        'backend.provider = "duckdb"\n'
        'backend.duckdb.database = ":memory:"\n'
        "[tokscale]\n"
        'bin = "usagebassoon-missing-tokscale"\n'
        "[logging]\n"
        "disable = true\n",
        encoding="utf-8",
    )
    return path


@pytest.mark.windows_smoke
@_WINDOWS_ONLY
def test_windows_cli_help_and_doctor_render(tmp_path: Path) -> None:
    """Run the module entry point and render a local-only doctor report."""
    help_result = subprocess.run(
        [sys.executable, "-m", "usagebassoon", "--help"],
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="replace",
        timeout=20,
    )
    help_output = plain_cli_output(f"{help_result.stdout}\n{help_result.stderr}")
    assert help_result.returncode == 0, help_output
    assert "UsageBassoon" in help_output

    config_path = _configuration(tmp_path)
    doctor_result = subprocess.run(
        [sys.executable, "-m", "usagebassoon", "doctor", "--config", str(config_path)],
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    doctor_output = plain_cli_output(f"{doctor_result.stdout}\n{doctor_result.stderr}")
    assert doctor_result.returncode in {0, 1}, doctor_output
    assert "Overall status:" in doctor_output
    assert "tokscale:" in doctor_output


@pytest.mark.windows_smoke
@_WINDOWS_ONLY
def test_windows_command_timeout_kills_cmd_descendants_only(tmp_path: Path) -> None:
    """Contain a space-named .cmd wrapper and its child without killing a peer."""
    config_path = _configuration(tmp_path)
    configuration = ConfigurationManager(config_path).load()
    environment = _child_environment(configuration)
    system_root = os.environ.get("SYSTEMROOT")
    assert system_root
    assert environment.get("SystemRoot") == system_root

    child_script = tmp_path / "fake tokscale child.py"
    child_pid_file = tmp_path / "child pid.txt"
    child_root_file = tmp_path / "child root.txt"
    child_finished_file = tmp_path / "child finished.txt"
    child_script.write_text(
        "from pathlib import Path\n"
        "import os, sys, time\n"
        "Path(sys.argv[1]).write_text(str(os.getpid()))\n"
        "Path(sys.argv[2]).write_text(os.environ.get('SystemRoot', ''))\n"
        "time.sleep(1.5)\n"
        "Path(sys.argv[3]).write_text('finished')\n",
        encoding="utf-8",
    )
    wrapper = tmp_path / "fake tokscale wrapper.cmd"
    wrapper.write_text(
        f'@echo off\r\n"{sys.executable}" "{child_script}" '
        f'"{child_pid_file}" "{child_root_file}" "{child_finished_file}"\r\n',
        encoding="utf-8",
    )

    unrelated = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    process = None
    started = time.monotonic()
    try:
        process = _start_tokscale_process((str(wrapper),), environment)
        with pytest.raises(RuntimeError, match="tokscale exceeded"):
            _capture_process(
                process,
                timeout_seconds=0.4,
                max_stdout_bytes=4096,
                max_stderr_bytes=4096,
            )
        elapsed = time.monotonic() - started
        assert elapsed < TOKSCALE_CLEANUP_TIMEOUT_SECONDS + 1.5
        assert child_pid_file.is_file()
        assert child_root_file.read_text(encoding="utf-8") == system_root
        assert unrelated.poll() is None
        time.sleep(1.7)
        assert not child_finished_file.exists()
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=3)
        if unrelated.poll() is None:
            unrelated.terminate()
            try:
                unrelated.wait(timeout=3)
            except subprocess.TimeoutExpired:
                unrelated.kill()
                unrelated.wait(timeout=3)


@pytest.mark.windows_smoke
@_WINDOWS_ONLY
def test_windows_system_metadata_captures_cpu_and_memory() -> None:
    """Capture available CPU and RAM metadata without assuming particular hardware."""
    metadata = capture_system_metadata()
    assert metadata.os_name == "Windows"
    assert metadata.cpu_model is not None and metadata.cpu_model.strip()
    assert metadata.cpu_count is not None and metadata.cpu_count > 0
    assert metadata.memory_bytes is not None and metadata.memory_bytes > 0
