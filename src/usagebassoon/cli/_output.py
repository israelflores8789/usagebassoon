# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_output.py — Shared Rich console construction for CLI-owned presentation."""

from __future__ import annotations

from typing import Literal, TextIO

from rich.console import Console

type ColorSystem = Literal["auto", "standard", "256", "truecolor", "windows"] | None


def output_console(
    *,
    stderr: bool = False,
    file: TextIO | None = None,
    width: int | None = None,
    record: bool = False,
    force_terminal: bool | None = None,
    color_system: ColorSystem = "auto",
) -> Console:
    """Create a Rich console with UsageBassoon's safe output defaults.

    Args:
        stderr: Write to standard error rather than standard output.
        file: Optional explicit text stream.
        width: Optional terminal width override.
        record: Retain rendering for plain-text export or visual tests.
        force_terminal: Override terminal auto-detection when explicitly needed.
        color_system: Color capability, or ``None`` to disable color.

    Returns:
        A console that disables markup interpretation and automatic highlighting.
    """
    return Console(
        stderr=stderr,
        file=file,
        width=width,
        record=record,
        force_terminal=force_terminal,
        color_system=color_system,
        markup=False,
        highlight=False,
    )
