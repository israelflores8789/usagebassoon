# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_spinner.py — Interactive indicators, colors, and CLI boundaries."""

from __future__ import annotations

import io
import json
import subprocess
import threading
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, TextIO, override

import pytest
from rich.console import Console
from typer.testing import CliRunner
from yaspin.core import Spinner

import usagebassoon.cli.spinner as spinner_module
from tests._cli import plain_cli_output
from usagebassoon.backends.base import StorageBackend
from usagebassoon.cli._colorterm import TerminalColors
from usagebassoon.cli.app import app
from usagebassoon.config import UsageBassoonConfig


def _console_factory(console: Console) -> Callable[..., Console]:
    """Keep an explicit console when the CLI requests its themed stderr console."""

    def build(**_kwargs: object) -> Console:
        return console

    return build


def _theme(_console: Console) -> TerminalColors:
    return (12, 34, 56), (240, 240, 240)


def _last_message(values: Sequence[str]) -> str:
    return values[-1]


class Terminal(io.StringIO):
    """A terminal output stream that also observes actual animation writes."""

    def __init__(self) -> None:
        super().__init__()
        self.frame_written = threading.Event()

    @override
    def isatty(self) -> bool:
        return True

    @override
    def write(self, text: str) -> int:
        if text.startswith("\r") or "\r" in text:
            self.frame_written.set()
        return super().write(text)


@dataclass
class Display:
    """Record the helper's indicator lifecycle without an animation thread."""

    calls: list[tuple[Spinner, str, TextIO]] = field(default_factory=list)
    active: bool = False

    @contextmanager
    def animate(
        self, animation: Spinner, *, text: str, stream: TextIO
    ) -> Generator[None]:
        self.calls.append((animation, text, stream))
        self.active = True
        stream.write(f"\r{animation.frames[0]} {text}")
        try:
            yield
        finally:
            self.active = False
            stream.write("\r\x1b[K")


@pytest.fixture
def terminal(monkeypatch: pytest.MonkeyPatch) -> Terminal:
    """Make interaction and color selection independent of the pytest environment."""
    for name in ["CI", "GITHUB_ACTIONS", "NO_COLOR", "FORCE_COLOR", "COLORTERM"]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TERM", "xterm-256color")
    stream = Terminal()
    console = Console(file=stream, color_system="truecolor", width=80)
    monkeypatch.setattr(spinner_module, "themed_console", _console_factory(console))
    monkeypatch.setattr(
        spinner_module,
        "resolve_theme_colors",
        _theme,
    )
    spinner_module._accent.cache_clear()
    return stream


@pytest.fixture
def display(monkeypatch: pytest.MonkeyPatch) -> Display:
    recorder = Display()
    monkeypatch.setattr(spinner_module, "yaspin", recorder.animate)
    return recorder


@pytest.mark.parametrize(
    "reason", ["redirected", "CI", "GITHUB_ACTIONS", "dumb", "narrow"]
)
def test_noninteractive_operations_stay_quiet(
    monkeypatch: pytest.MonkeyPatch, display: Display, reason: str
) -> None:
    if reason in {"CI", "GITHUB_ACTIONS"}:
        monkeypatch.setenv(reason, "true")
    monkeypatch.setenv("FORCE_COLOR", "1")
    stream = io.StringIO() if reason == "redirected" else Terminal()
    console = Console(file=stream, width=2 if reason == "narrow" else 80)
    if reason == "dumb":
        monkeypatch.setenv("TERM", "dumb")
    monkeypatch.setattr(spinner_module, "themed_console", _console_factory(console))
    with spinner_module.spinner():
        pass
    assert not display.calls
    assert stream.getvalue() == ""


@pytest.mark.parametrize(
    ("system", "no_color", "expected"),
    [
        ("truecolor", False, "\x1b[38;2;12;34;56m"),
        ("256", False, "\x1b[38;5;"),
        ("standard", False, "\x1b[35m"),
        ("truecolor", True, ""),
    ],
)
def test_configured_animation_and_theme_colors(
    monkeypatch: pytest.MonkeyPatch,
    terminal: Terminal,
    display: Display,
    system: Literal["truecolor", "256", "standard"],
    no_color: bool,
    expected: str,
) -> None:
    if no_color:
        monkeypatch.setenv("NO_COLOR", "1")
    console = Console(file=terminal, color_system=system, width=80)
    monkeypatch.setattr(spinner_module, "themed_console", _console_factory(console))
    monkeypatch.setattr(spinner_module.random, "choice", _last_message)
    configuration = UsageBassoonConfig(
        Path("config.toml"), "source", "duckdb", spinner="line"
    )
    with spinner_module.spinner(configuration):
        assert display.active
    animation, message, _stream = display.calls[0]
    assert animation.frames == ["-", "\\", "|", "/"]
    assert message == spinner_module.MESSAGES[-1]
    assert message.strip()
    assert "\x1b" not in message
    output = terminal.getvalue()
    if no_color:
        assert "\x1b[38" not in output and "\x1b[35" not in output
    else:
        assert expected in output
    assert output.endswith("\r\x1b[K")
    assert not display.active


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt])
def test_real_animation_stops_on_failure(
    terminal: Terminal, error: type[BaseException]
) -> None:
    threads_before = set(threading.enumerate())
    with pytest.raises(error), spinner_module.spinner():
        assert terminal.frame_written.wait(timeout=2)
        raise error("operation interrupted")
    assert set(threading.enumerate()) <= threads_before
    assert "\x1b[38;2;12;34;56m" in terminal.getvalue()


@pytest.mark.parametrize(
    "report", ["activity", "daily", "graph", "models", "sessions", "summary"]
)
def test_sample_reports_skip_spinner(display: Display, report: str) -> None:
    result = CliRunner().invoke(app, ["report", report, "--test"])
    assert result.exit_code == 0, plain_cli_output(result.output)
    assert not display.calls


def test_query_uses_configured_spinner_and_keeps_json_clean(
    tmp_path: Path, terminal: Terminal, display: Display
) -> None:
    from tests.test_cli_query import _configured_store

    config, backend = _configured_store(tmp_path)
    backend.close()
    with config.open("a") as stream:
        stream.write('spinner = "line"\n')
    result = CliRunner().invoke(
        app, ["query", "session_notes", "--config", str(config), "--format", "json"]
    )
    assert result.exit_code == 0, plain_cli_output(result.output)
    assert json.loads(plain_cli_output(result.stdout))[0]["client"] == "codex"
    assert len(display.calls) == 2
    assert all(
        animation.frames == ["-", "\\", "|", "/"] for animation, _, _ in display.calls
    )
    assert not display.active
    assert terminal.getvalue()


def test_note_editor_runs_between_spinner_operations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: Terminal,
    display: Display,
) -> None:
    import usagebassoon.cli.note as note_module
    from tests.test_cli_query import _configured_store

    config, backend = _configured_store(tmp_path)
    backend.close()
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.setenv("EDITOR", "editor")

    def edit(command: list[str], *, check: bool) -> subprocess.CompletedProcess[str]:
        assert not check
        assert not display.active
        assert len(display.calls) == 2
        Path(command[-1]).write_text("An edited note")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(note_module.subprocess, "run", edit)
    result = CliRunner().invoke(
        app,
        [
            "note",
            "edit",
            "--client",
            "codex",
            "--session",
            "ses_private",
            "--config",
            str(config),
        ],
    )
    assert result.exit_code == 0, plain_cli_output(result.output)
    assert len(display.calls) == 3
    assert not display.active
    assert terminal.getvalue()


def test_restore_confirmation_precedes_spinner(
    terminal: Terminal, display: Display
) -> None:
    result = CliRunner().invoke(app, ["restore"], input="n\n")
    assert result.exit_code != 0
    assert not display.calls
    assert terminal.getvalue() == ""


def test_query_closes_backend_if_spinner_startup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: Terminal,
    display: Display,
) -> None:
    import usagebassoon.cli.query as query_module
    from tests.test_cli_query import _configured_store

    config, backend = _configured_store(tmp_path)
    backend.close()
    closed: list[str] = []

    @contextmanager
    def fail_second_animation(
        animation: Spinner, *, text: str, stream: TextIO
    ) -> Generator[None]:
        if display.calls:
            raise RuntimeError("indicator failed to start")
        with display.animate(animation, text=text, stream=stream):
            yield

    def close(opened: StorageBackend, *, context: str) -> None:
        closed.append(context)
        opened.close()

    monkeypatch.setattr(spinner_module, "yaspin", fail_second_animation)
    monkeypatch.setattr(query_module, "close_backend", close)
    result = CliRunner().invoke(
        app, ["query", "session_notes", "--config", str(config), "--format", "json"]
    )
    assert result.exit_code != 0
    assert closed == ["running a CLI query"]
    assert not display.active
    assert terminal.getvalue()
