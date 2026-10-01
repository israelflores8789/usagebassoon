# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_colorterm.py — Shared terminal discovery and color conversion tests."""

from __future__ import annotations

import os

import pytest
from rich.console import Console

from usagebassoon.cli._colorterm import blend_palette


@pytest.mark.parametrize("ending", ["\x07", "\x1b\\"])
def test_theme_reply_decoding_and_light_background(ending: str) -> None:
    """Decode either OSC terminator and blend from light as well as dark themes."""
    from usagebassoon.cli._colorterm import _decode_colors

    response = f"\x1b]11;rgb:ffff/ffff/ffff{ending}\x1b]4;5;rgb:8080/4040/c0c0{ending}"
    assert _decode_colors(response) == ((128, 64, 192), (255, 255, 255))
    assert _decode_colors("\x1b]4;5;rgb:80/40/c0\x07") is None
    palette = blend_palette(3, (128, 64, 192), (255, 255, 255))
    assert palette == ["#ffffff", "#c0a0e0", "#8040c0"]


def test_fragmented_theme_replies_and_timeout() -> None:
    """Collect split replies without requiring a response from unsupported terminals."""
    from usagebassoon.cli._colorterm import _COLOR_QUERY, _probe_colors

    sent: list[str] = []
    replies = iter(["\x1b]4;5;rgb:80/40", "/c0\x07", "\x1b]11;rgb:18/18/18\x1b\\"])
    assert _probe_colors(sent.append, lambda _timeout: next(replies, "")) == (
        (128, 64, 192),
        (24, 24, 24),
    )
    assert sent == [_COLOR_QUERY]
    assert _probe_colors(sent.append, lambda _timeout: "") is None
    trace: list[str] = []
    assert _probe_colors(sent.append, lambda _timeout: "", trace=trace) is None
    assert trace[1] == "reply=''"
    assert "No complete" in trace[2]


@pytest.mark.skipif(os.name == "nt", reason="POSIX terminal attributes")
def test_terminal_query_restores_input_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Restore echo and canonical input after an unsupported terminal's timeout."""
    import importlib
    import sys
    import termios
    from collections.abc import Callable

    from usagebassoon.cli._colorterm import _posix_colors

    module = importlib.import_module("usagebassoon.cli._colorterm")
    master, slave = os.openpty()
    original = termios.tcgetattr(slave)
    try:
        with (
            os.fdopen(os.dup(slave), "r") as input_stream,
            os.fdopen(os.dup(slave), "w") as output_stream,
        ):
            monkeypatch.setattr(sys, "stdin", input_stream)
            console = Console(file=output_stream)

            def timeout_probe(
                write: Callable[[str], object],
                read: Callable[[float], str],
                *,
                ansi_index: int = 5,
            ) -> None:
                """Inspect query mode without sending a simulated terminal request."""
                assert ansi_index == 5
                assert callable(write) and callable(read)
                attributes = termios.tcgetattr(slave)
                assert not attributes[3] & (termios.ICANON | termios.ECHO)
                return None

            monkeypatch.setattr(module, "_probe_colors", timeout_probe)
            assert _posix_colors(console) is None
            assert termios.tcgetattr(slave) == original
    finally:
        os.close(master)
        os.close(slave)


def test_shared_console_honors_stream_width_and_no_color(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep shared console settings usable beyond activity and respect NO_COLOR."""
    import io

    from usagebassoon.cli._colorterm import (
        color_enabled,
        supports_gradients,
        themed_console,
    )

    monkeypatch.setenv("COLORTERM", "truecolor")
    monkeypatch.setenv("TERM", "xterm")
    monkeypatch.delenv("NO_COLOR", raising=False)
    stream = io.StringIO()
    console = themed_console(file=stream, width=41, record=True, force_terminal=True)
    assert console.file is stream
    assert console.width == 41
    assert console.color_system == "truecolor"
    assert color_enabled(console)
    assert supports_gradients(console)
    console.print("shared console")
    assert console.export_text().strip() == "shared console"
    monkeypatch.setenv("NO_COLOR", "1")
    disabled = themed_console(file=stream, force_terminal=True)
    assert not color_enabled(disabled)
    assert not supports_gradients(disabled)
    redirected = themed_console(file=stream, force_terminal=False)
    assert not supports_gradients(redirected)


def test_shared_blending_and_contrast() -> None:
    """Provide bounded blending and foreground contrast without activity policy."""
    from usagebassoon.cli._colorterm import blend_color, contrasting_foreground

    assert blend_color((0, 0, 0), (200, 100, 50), -1) == "#000000"
    assert blend_color((0, 0, 0), (200, 100, 50), 0.5) == "#643219"
    assert blend_color((0, 0, 0), (200, 100, 50), 2) == "#c86432"
    assert contrasting_foreground("#ffffff") == "black"
    assert contrasting_foreground("#000000") == "white"
    with pytest.raises(ValueError, match="at least two"):
        blend_palette(1)


def test_shared_ansi_selection_and_indexed_palette() -> None:
    """Validate theme slots and convert shared RGB styles for 256-color output."""
    import io

    from usagebassoon.cli._colorterm import (
        adapt_palette,
        ansi_color_index,
        normalize_ansi_color,
    )

    assert normalize_ansi_color(" Bright-Blue ") == "bright_blue"
    assert ansi_color_index("bright_blue") == 12
    with pytest.raises(ValueError, match="ANSI color"):
        normalize_ansi_color("#ff00ff")
    console = Console(file=io.StringIO(), force_terminal=True, color_system="256")
    palette = adapt_palette(console, ["#8040c0"])
    assert palette[0].startswith("color(")
    assert palette != ["#8040c0"]
