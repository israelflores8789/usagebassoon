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
import typer
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


@pytest.mark.windows_compatible
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


@pytest.mark.windows_compatible
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


@pytest.mark.windows_compatible
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
    terminal.frame_written.clear()
    with pytest.raises(error), spinner_module.delayed_spinner(delay_seconds=0.01):
        assert terminal.frame_written.wait(timeout=2)
        raise error("delayed operation interrupted")
    assert set(threading.enumerate()) <= threads_before


@pytest.mark.windows_compatible
@pytest.mark.parametrize(
    "report", ["activity", "daily", "graph", "models", "sessions", "summary"]
)
def test_sample_reports_skip_spinner(display: Display, report: str) -> None:
    result = CliRunner().invoke(app, ["report", report, "--test"])
    assert result.exit_code == 0, plain_cli_output(result.output)
    assert not display.calls


@pytest.mark.windows_compatible
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


@pytest.mark.windows_compatible
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
        Path(command[-1]).write_text("An edited note", encoding="utf-8")
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


@pytest.mark.windows_compatible
def test_restore_warning_precedes_backend_setup(
    monkeypatch: pytest.MonkeyPatch, terminal: Terminal, display: Display
) -> None:
    """Emit the quiescence warning before backend setup or animation begins."""
    import usagebassoon.cli.restore as restore_module

    calls: list[Path | None] = []

    def stop_setup(config: Path | None) -> tuple[UsageBassoonConfig, StorageBackend]:
        """Stop before accessing any user configuration or storage backend."""
        calls.append(config)
        raise typer.Abort

    monkeypatch.setattr(restore_module, "configured_backend", stop_setup)
    result = CliRunner().invoke(app, ["restore"])
    assert result.exit_code != 0
    assert calls == [None]
    assert "stop all UsageBassoon instances and writers" in plain_cli_output(
        result.stderr
    )
    assert not display.calls
    assert terminal.getvalue() == ""


@pytest.mark.windows_compatible
@pytest.mark.parametrize(
    ("location_answer", "answer"), [("n", "y"), ("y", "n"), ("y", "y")]
)
def test_restore_prompts_stop_spinner_before_cleanup_and_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: Terminal,
    display: Display,
    location_answer: str,
    answer: str,
) -> None:
    """Review a verified snapshot without animation before authorizing writes."""
    import usagebassoon.cli.restore as restore_module
    from tests.test_cli_restore import _seed_session, _write_config
    from usagebassoon.archiver import SnapshotArchiver
    from usagebassoon.backends.duckdb_local import DuckDBBackend
    from usagebassoon.snapshot.reader import PreparedSnapshot

    source_database = tmp_path / "source.duckdb"
    _seed_session(source_database, "ses_source")
    source = DuckDBBackend(source_database)
    try:
        uri = SnapshotArchiver(str(tmp_path / "archive")).write(
            source, run_id="prompt", manual=True
        )
    finally:
        source.close()
    assert uri is not None
    database = tmp_path / "destination.duckdb"
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.close()
    config = tmp_path / "config.toml"
    _write_config(config, database)
    events: list[str] = []
    files: list[Path] = []
    original_location = restore_module.confirm_location
    original_confirm = typer.confirm
    original_cleanup = DuckDBBackend.cleanup_restore_stages

    def location(archiver: SnapshotArchiver, selection: str) -> None:
        """Check that the disabled-location prompt runs without animation."""
        assert not display.active
        events.append("location")
        original_location(archiver, selection)

    def advice(_source_id: str, prepared: PreparedSnapshot) -> str:
        """Observe verified files while providing deterministic source advice."""
        assert not display.active
        files.extend(prepared.files.values())
        assert files and all(path.is_file() for path in files)
        events.append("advice")
        return "Source identity advice."

    def confirm(text: str, *, default: bool, abort: bool) -> bool:
        """Keep the final confirmation outside animation and before cleanup."""
        assert not display.active
        assert events == ["location", "advice"]
        events.append("confirm")
        return original_confirm(text, default=default, abort=abort)

    def cleanup(self: DuckDBBackend) -> None:
        """Allow cleanup only after confirmation, with progress indication."""
        assert display.active
        assert events == ["location", "advice", "confirm"]
        assert all(path.is_file() for path in files)
        events.append("cleanup")
        original_cleanup(self)

    monkeypatch.setattr(restore_module, "confirm_location", location)
    monkeypatch.setattr(restore_module, "source_identity_warning", advice)
    monkeypatch.setattr(typer, "confirm", confirm)
    monkeypatch.setattr(DuckDBBackend, "cleanup_restore_stages", cleanup)
    result = CliRunner().invoke(
        app,
        [
            "restore",
            "--config",
            str(config),
            "--from-snapshot",
            uri,
            "--cleanup-stages",
        ],
        input=f"{location_answer}\n{answer}\n",
    )
    output = plain_cli_output(result.output)
    accepted = location_answer == answer == "y"
    assert (result.exit_code == 0) is accepted, output
    assert "Snapshot:" in output and "Destination:" in output
    if location_answer == "n":
        assert events == ["location"]
        assert "Have all writers stopped" not in output
    else:
        assert events == ["location", "advice", "confirm"] + (
            ["cleanup"] if accepted else []
        )
        assert output.index("Snapshot:") < output.index("Have all writers stopped")
        assert output.index("Destination:") < output.index("Have all writers stopped")
        assert output.index("Source identity advice.") < output.index(
            "Have all writers stopped"
        )
    assert not display.active
    assert all(not path.exists() for path in files)
    assert len(display.calls) == (3 if accepted else 2)
    assert terminal.getvalue()
    backend = DuckDBBackend(database)
    try:
        assert backend.query("SELECT session_id FROM sessions").to_pylist() == (
            [{"session_id": "ses_source"}] if accepted else []
        )
    finally:
        backend.close()


@pytest.mark.windows_compatible
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
