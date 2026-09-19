# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""display.py — Safe terminal rendering for untrusted human-readable values."""

from __future__ import annotations

import unicodedata


def sanitize_display(value: object) -> str:
    """Return text with terminal-control and format characters escaped.

    Args:
        value: Arbitrary value destined for a terminal or saved text report.

    Returns:
        Readable text that preserves ordinary Unicode while visibly escaping
        control and formatting characters that could alter terminal output.
    """
    text = str(value)
    pieces: list[str] = []
    for character in text:
        category = unicodedata.category(character)
        if category in {"Cc", "Cf"}:
            pieces.append(f"\\u{ord(character):04x}")
        else:
            pieces.append(character)
    return "".join(pieces)
