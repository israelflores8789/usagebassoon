# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_colorterm.py — Shared CLI terminal capabilities, theme discovery, and colors.

Theme discovery uses bounded, read-only terminal queries with restored input
settings, then Windows metadata or deterministic fallback colors. This module
contains no report metrics, calendar layout, or activity intensity policy.
"""

from __future__ import annotations

import logging
import os
import re
import struct
import sys
import time
from collections.abc import Callable, Sequence
from typing import TextIO, cast

from rich.color import Color, ColorSystem
from rich.console import Console

from usagebassoon.cli._output import output_console

_LOG = logging.getLogger("usagebassoon")

type RGB = tuple[int, int, int]
type TerminalColors = tuple[RGB, RGB]

_FALLBACK_PURPLE: RGB = (160, 96, 208)
_FALLBACK_BACKGROUND: RGB = (24, 24, 24)
_COLOR_QUERY = "\x1b]4;5;?\x1b\\\x1b]11;?\x1b\\"
_COLOR_RESPONSE = re.compile(
    r"\x1b\](4;[0-9]{1,2}|11);rgb:([0-9a-fA-F]{1,4})/"
    r"([0-9a-fA-F]{1,4})/([0-9a-fA-F]{1,4})(?:\x07|\x1b\\)"
)
_ANSI_NAMES = ("black", "red", "green", "yellow", "blue", "magenta", "cyan", "white")
_ANSI_COLORS = {
    name: index
    for index, name in enumerate(
        (*_ANSI_NAMES, *(f"bright_{name}" for name in _ANSI_NAMES))
    )
}


def normalize_ansi_color(value: str) -> str:
    """Validate a standard ANSI color name and normalize bright-name separators."""
    name = value.strip().lower().replace("-", "_")
    if name not in _ANSI_COLORS:
        raise ValueError("Choose an ANSI color: " + ", ".join(_ANSI_COLORS))
    return name


def fallback_accent(name: str) -> RGB:
    """Use the selected ANSI hue when the terminal cannot report its theme."""
    name = normalize_ansi_color(name)
    if name == "magenta":
        return _FALLBACK_PURPLE
    rgb = Color.parse(name).get_truecolor()
    return rgb.red, rgb.green, rgb.blue


def _windows_colors(ansi_index: int = 5) -> TerminalColors | None:
    """Read the selected Windows console color and its current background."""
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes

    try:
        loader = cast(type[ctypes.CDLL], vars(ctypes)["WinDLL"])
        kernel = loader("kernel32", use_last_error=True)
        kernel.GetStdHandle.argtypes = [wintypes.DWORD]
        kernel.GetStdHandle.restype = wintypes.HANDLE
        kernel.GetConsoleScreenBufferInfoEx.argtypes = [
            wintypes.HANDLE,
            ctypes.c_void_p,
        ]
        kernel.GetConsoleScreenBufferInfoEx.restype = wintypes.BOOL
        # CONSOLE_SCREEN_BUFFER_INFOEX: fixed 32-byte header and 16 COLORREFs.
        buffer = ctypes.create_string_buffer(96)
        struct.pack_into("<I", buffer, 0, 96)
        if not kernel.GetConsoleScreenBufferInfoEx(kernel.GetStdHandle(-11), buffer):
            return None
        # Windows stores red/blue bits in the opposite order from ANSI.
        windows_index = (
            (ansi_index & 8)
            | ((ansi_index & 1) << 2)
            | (ansi_index & 2)
            | ((ansi_index & 4) >> 2)
        )
        accent = struct.unpack_from("<I", buffer, 32 + windows_index * 4)[0]
        attributes = struct.unpack_from("<H", buffer, 12)[0]
        background = struct.unpack_from(
            "<I", buffer, 32 + ((attributes >> 4) & 15) * 4
        )[0]
        return (
            (accent & 255, (accent >> 8) & 255, (accent >> 16) & 255),
            (background & 255, (background >> 8) & 255, (background >> 16) & 255),
        )
    except (AttributeError, KeyError, OSError, ValueError):
        _LOG.debug("Windows terminal palette unavailable", exc_info=True)
        return None


def _decode_colors(response: str, ansi_index: int = 5) -> TerminalColors | None:
    """Decode OSC palette and default-background responses at any RGB precision."""
    found: dict[str, RGB] = {}
    for match in _COLOR_RESPONSE.finditer(response):
        components = tuple(
            round(int(part, 16) * 255 / (16 ** len(part) - 1))
            for part in match.groups()[1:]
        )
        found[match[1]] = (components[0], components[1], components[2])
    key = f"4;{ansi_index}"
    if key not in found or "11" not in found:
        return None
    return found[key], found["11"]


def _probe_colors(
    write: Callable[[str], object],
    read: Callable[[float], str],
    *,
    trace: list[str] | None = None,
    ansi_index: int = 5,
) -> TerminalColors | None:
    """Collect bounded terminal replies with one second for SSH round trips."""
    query = f"\x1b]4;{ansi_index};?\x1b\\\x1b]11;?\x1b\\"
    write(query)
    if trace is not None:
        trace.append(f"query={query!r}")
    deadline = time.monotonic() + 1.0
    response = ""
    while len(response) < 1024:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        chunk = read(remaining)
        if not chunk:
            break
        response += chunk
        colors = _decode_colors(response, ansi_index)
        if colors is not None:
            if trace is not None:
                trace.append(f"reply={response!r}")
            return colors
    if trace is not None:
        trace.append(f"reply={response!r}")
        trace.append("No complete palette/background response within one second.")
    return None


def _posix_colors(
    console: Console, *, trace: list[str] | None = None, ansi_index: int = 5
) -> TerminalColors | None:
    """Query an idle terminal, restoring input attributes even after a timeout."""
    import select
    import termios

    descriptor = sys.stdin.fileno()
    output = console.file.fileno()
    if not os.isatty(descriptor) or not os.isatty(output):
        if trace is not None:
            trace.append("Skipped: input or output is not a terminal.")
        return None
    # Do not consume input that was already waiting before discovery.
    if select.select([descriptor], [], [], 0)[0]:
        if trace is not None:
            trace.append("Skipped: terminal input was already pending.")
        return None
    try:
        original = termios.tcgetattr(descriptor)
    except termios.error:
        _LOG.debug("terminal input settings unavailable for theme probe", exc_info=True)
        if trace is not None:
            trace.append("Could not read terminal input settings.")
        return None
    modified = list(original)
    modified[3] &= ~(termios.ICANON | termios.ECHO)
    controls = list(original[6])
    controls[termios.VMIN] = 0
    controls[termios.VTIME] = 0
    modified[6] = controls

    def write(query: str) -> None:
        """Send only read-only OSC requests through the chart's output terminal."""
        console.file.write(query)
        console.file.flush()

    def read(timeout: float) -> str:
        """Read available replies without extending the probe deadline."""
        if not select.select([descriptor], [], [], timeout)[0]:
            return ""
        return os.read(descriptor, 1024).decode("ascii", errors="replace")

    try:
        termios.tcsetattr(descriptor, termios.TCSANOW, modified)
        if select.select([descriptor], [], [], 0)[0]:
            if trace is not None:
                trace.append("Skipped: terminal input was pending after mode change.")
            return None
        if trace is not None:
            return _probe_colors(write, read, trace=trace, ansi_index=ansi_index)
        return _probe_colors(write, read, ansi_index=ansi_index)
    except termios.error:
        _LOG.debug("terminal theme probe unavailable", exc_info=True)
        return None
    finally:
        try:
            termios.tcsetattr(descriptor, termios.TCSANOW, original)
        except termios.error:
            _LOG.warning("could not restore terminal input settings", exc_info=True)


def _windows_query_colors(
    console: Console, ansi_index: int = 5
) -> TerminalColors | None:
    """Query Windows VT colors with bounded reads and restore console input mode."""
    import ctypes
    import msvcrt
    from ctypes import wintypes

    kbhit = cast(Callable[[], bool], vars(msvcrt)["kbhit"])
    getwch = cast(Callable[[], str], vars(msvcrt)["getwch"])
    ungetwch = cast(Callable[[str], None], vars(msvcrt)["ungetwch"])
    if kbhit():
        return None
    loader = cast(type[ctypes.CDLL], vars(ctypes)["WinDLL"])
    kernel = loader("kernel32", use_last_error=True)
    kernel.GetStdHandle.argtypes = [wintypes.DWORD]
    kernel.GetStdHandle.restype = wintypes.HANDLE
    kernel.GetConsoleMode.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetConsoleMode.restype = wintypes.BOOL
    kernel.SetConsoleMode.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.SetConsoleMode.restype = wintypes.BOOL
    handle = kernel.GetStdHandle(-10)
    original = wintypes.DWORD()
    if not kernel.GetConsoleMode(handle, ctypes.byref(original)):
        return None
    # Enable VT input and disable line buffering and echo during the query.
    if not kernel.SetConsoleMode(handle, (original.value & ~6) | 0x200):
        return None

    def write(query: str) -> None:
        """Send the color query to the terminal used for chart output."""
        console.file.write(query)
        console.file.flush()

    in_response = False

    def read(timeout: float) -> str:
        """Poll console replies, returning an unrelated initial keystroke to input."""
        nonlocal in_response
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if kbhit():
                character = getwch()
                if not in_response and character != "\x1b":
                    ungetwch(character)
                    return ""
                in_response = character not in {"\x07", "\\"}
                return character
            time.sleep(0.002)
        return ""

    try:
        return _probe_colors(write, read, ansi_index=ansi_index)
    finally:
        if not kernel.SetConsoleMode(handle, original.value):
            _LOG.warning("could not restore Windows terminal input settings")


def resolve_theme_colors(console: Console, color: str = "magenta") -> TerminalColors:
    """Prefer actual terminal replies, then Windows metadata, then neutral defaults."""
    ansi_index = ansi_color_index(color)
    if color_enabled(console) and sys.stdin.isatty():
        try:
            colors = (
                _windows_query_colors(console, ansi_index)
                if os.name == "nt"
                else _posix_colors(console, ansi_index=ansi_index)
            )
            if colors is not None:
                return colors
        except (AttributeError, KeyError, OSError, ValueError):
            _LOG.debug(
                "terminal theme discovery failed; using fallback colors", exc_info=True
            )
    name = next(name for name, index in _ANSI_COLORS.items() if index == ansi_index)
    return _windows_colors(ansi_index) or (fallback_accent(name), _FALLBACK_BACKGROUND)


def blend_palette(
    steps: int,
    accent: RGB = _FALLBACK_PURPLE,
    background: RGB = _FALLBACK_BACKGROUND,
) -> list[str]:
    """Blend the terminal background toward the selected theme color."""
    if steps < 2:
        raise ValueError("A color palette requires at least two steps")
    return [
        blend_color(background, accent, level / (steps - 1)) for level in range(steps)
    ]


def ansi_color_index(name: str) -> int:
    """Return the standard ANSI palette slot for a validated color name."""
    return _ANSI_COLORS[normalize_ansi_color(name)]


def color_enabled(console: Console) -> bool:
    """Return whether terminal output permits ANSI color styling."""
    return (
        console.is_terminal
        and console.color_system is not None
        and not console.no_color
    )


def supports_gradients(console: Console) -> bool:
    """Return whether RGB gradients can be rendered exactly or with 256 colors."""
    return color_enabled(console) and console.color_system in {"truecolor", "256"}


def blend_color(background: RGB, accent: RGB, intensity: float) -> str:
    """Blend a background toward an accent at an intensity clamped between zero and one.

    Args:
        background: Background RGB components from zero to 255.
        accent: Accent RGB components from zero to 255.
        intensity: Fraction of accent color in the result.

    Returns:
        A Rich-compatible hexadecimal RGB color.
    """
    fraction = max(0.0, min(1.0, intensity))
    return "#" + "".join(
        f"{round(bg + fraction * (fg - bg)):02x}"
        for bg, fg in zip(background, accent, strict=True)
    )


def adapt_palette(console: Console, palette: Sequence[str]) -> list[str]:
    """Convert RGB shades to explicit indexed styles when using 256-color output."""
    if console.color_system == "256":
        return [
            f"color({Color.parse(shade).downgrade(ColorSystem.EIGHT_BIT).number})"
            for shade in palette
        ]
    return list(palette)


def contrasting_foreground(background: str) -> str:
    """Choose black or white text against a known RGB or indexed background."""
    rgb = Color.parse(background).get_truecolor()
    luminance = 0.2126 * rgb.red + 0.7152 * rgb.green + 0.0722 * rgb.blue
    return "black" if luminance > 140 else "white"


def themed_console(
    *,
    record: bool = False,
    stderr: bool = False,
    file: TextIO | None = None,
    width: int | None = None,
    force_terminal: bool | None = None,
) -> Console:
    """Build a CLI console honoring advertised truecolor and disabled-color output.

    Args:
        record: Retain output for plain-text export.
        stderr: Select standard error when no file is supplied.
        file: Optional output stream.
        width: Optional rendering width.
        force_terminal: Optional terminal-detection override.

    Returns:
        A safe CLI console with automatic or explicitly advertised color support.
    """
    console = output_console(
        record=record,
        stderr=stderr,
        file=file,
        width=width,
        force_terminal=force_terminal,
    )
    advertised = bool(os.environ.get("WT_SESSION")) or os.environ.get(
        "COLORTERM", ""
    ).strip().lower() in {"truecolor", "24bit"}
    if (
        advertised
        and console.is_terminal
        and not console.is_dumb_terminal
        and not console.no_color
        and console.color_system != "truecolor"
    ):
        return output_console(
            record=record,
            stderr=stderr,
            file=file,
            width=width,
            force_terminal=force_terminal,
            color_system="truecolor",
        )
    return console
