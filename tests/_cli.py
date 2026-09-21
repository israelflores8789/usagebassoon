# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""_cli.py — Shared semantic assertions for terminal command tests."""

from __future__ import annotations

from rich.text import Text


def plain_cli_output(value: str) -> str:
    """Remove terminal styling before a semantic CLI-output assertion.

    Args:
        value: Captured standard output or standard error.

    Returns:
        The same output without ANSI styling information.
    """
    return Text.from_ansi(value).plain
