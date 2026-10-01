# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""spinner.py — Theme-aware, playful indicators for bounded CLI operations."""

from __future__ import annotations

import os
import random
from collections.abc import Generator
from contextlib import contextmanager
from functools import lru_cache
from io import TextIOBase
from typing import TextIO, cast, override

from rich.color import Color, ColorSystem
from rich.style import Style
from yaspin import yaspin
from yaspin.core import Spinner
from yaspin.spinners import Spinners

from usagebassoon.cli._colorterm import (
    RGB,
    color_enabled,
    resolve_theme_colors,
    supports_gradients,
    themed_console,
)
from usagebassoon.config import UsageBassoonConfig

MESSAGES: tuple[str, ...] = (
    "Cogitating...",
    "Pontificating...",
    "Brewing...",
    "Brainstorming...",
    "Consulting the oracle...",
    "Weaving the spells...",
    "Performing the incantation...",
    "Summoning the numbers...",
    "Herding the tokens...",
    "Tuning the bassoon...",
    "Polishing the reeds...",
    "Conducting the orchestra...",
    "Chasing the muse...",
    "Unrolling the scrolls...",
    "Deciphering the runes...",
    "Stirring the cauldron...",
    "Aligning the stars...",
    "Gathering the stardust...",
    "Counting the constellations...",
    "Negotiating with goblins...",
    "Awakening the abacus...",
    "Tickling the synapses...",
    "Untangling the noodles...",
    "Reviewing the tomes...",
    "Opening the grimoire...",
    "Whispering to databases...",
    "Summoning the essence...",
    "Reviewing the chronicles...",
    "Manifesting the query...",
    "Wrangling the electrons...",
    "Composing the overture...",
    "Incanting the formula...",
    "Pondering...",
    "Ruminating...",
    "Percolating...",
    "Conjuring...",
    "Divining...",
    "Marinating...",
    "Improvising...",
    "Orchestrating...",
    "Transmuting...",
    "Dreamweaving...",
    "Charting the unknown...",
    "Peering into the Aether...",
    "Translating the runes...",
    "Deciphering the scrolls...",
    "Inviting the gremlins...",
    "Befriending the bits...",
    "Coaxing the clockwork...",
    "Folding the universe...",
    "Unboxing the mysteries...",
    "Consulting the archives...",
    "Seeking hidden harmonies...",
    "Bottling the lightning...",
    "Feeding the dragon...",
    "Transcribing the scripts...",
    "Balancing the moonbeams...",
    "Taming the whirlwinds...",
    "Mining the moonlight...",
    "Fetching enchanted parcels...",
    "Calibrating the crystal...",
    "Tracing the ley lines...",
    "Asking the sphinx...",
    "Preparing the crescendo...",
)


@lru_cache(maxsize=4)
def _accent(stream: TextIO) -> RGB:
    """Discover the terminal's magenta once per output stream."""
    return resolve_theme_colors(themed_console(file=stream))[0]


class _ThemedStream(TextIOBase):
    """Style completed frames so yaspin measures and truncates plain text."""

    def __init__(self, stream: TextIO, style: Style, system: ColorSystem) -> None:
        """Retain the output stream and its discovered terminal color style."""
        super().__init__()
        self._stream = stream
        self._style = style
        self._system = system

    @override
    def write(self, text: str) -> int:
        """Color frame writes while preserving cursor and line-clear controls."""
        rendered = (
            self._style.render(text, color_system=self._system)
            if text.startswith("\r") and not text.startswith("\r\x1b")
            else text
        )
        self._stream.write(rendered)
        return len(text)

    @override
    def flush(self) -> None:
        """Flush output without taking ownership of the underlying stream."""
        if not self._stream.closed:
            self._stream.flush()

    @override
    def isatty(self) -> bool:
        """Report the wrapped stream's terminal capability."""
        return not self.closed and self._stream.isatty()

    @property
    @override
    def closed(self) -> bool:
        """Let yaspin's safe-stream handling observe a closed underlying stream."""
        return super().closed or self._stream.closed


@contextmanager
def spinner(configuration: UsageBassoonConfig | None = None) -> Generator[None]:
    """Animate a configured spinner while an interactive CLI operation waits.

    Args:
        configuration: Loaded settings, or None to use the Pong default.

    Yields:
        Control to the operation, clearing the indicator on every exit.
    """
    console = themed_console(stderr=True)
    if (
        not console.file.isatty()
        or console.is_dumb_terminal
        or os.environ.get("CI")
        or os.environ.get("GITHUB_ACTIONS")
    ):
        yield
        return
    name = configuration.spinner if configuration is not None else "pong"
    template = cast(Spinner, getattr(Spinners, name))
    if console.width <= max(len(frame) for frame in template.frames) + 1:
        yield
        return
    message = random.choice(MESSAGES)
    stream = cast(TextIO, console.file)
    if color_enabled(console):
        style = (
            Style(color=Color.from_rgb(*_accent(stream)))
            if supports_gradients(console)
            else Style(color="magenta")
        )
        system = {
            "truecolor": ColorSystem.TRUECOLOR,
            "256": ColorSystem.EIGHT_BIT,
            "windows": ColorSystem.WINDOWS,
        }.get(console.color_system or "standard", ColorSystem.STANDARD)
        stream = cast(TextIO, _ThemedStream(stream, style, system))
    with yaspin(template, text=message, stream=stream):
        yield
