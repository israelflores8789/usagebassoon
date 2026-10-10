# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_display.py — Terminal-control neutralization tests."""

from __future__ import annotations

from usagebassoon.display import sanitize_display


def test_display_sanitizer_escapes_terminal_and_bidi_controls() -> None:
    """Keep ordinary Unicode while neutralizing terminal control sequences."""
    value = "model\x1b[31mred\x1b[0m\u202e.txt\nΩ"

    rendered = sanitize_display(value)

    assert "\x1b" not in rendered
    assert "\n" not in rendered
    assert "\\u001b[31m" in rendered
    assert "\\u202e" in rendered
    assert rendered.endswith("Ω")
