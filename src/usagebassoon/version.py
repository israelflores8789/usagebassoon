# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""version.py — Runtime UsageBassoon distribution version lookup."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version

_DISTRIBUTION_NAME = "usagebassoon"


def get_version() -> str:
    """Return the installed UsageBassoon distribution version."""
    try:
        return distribution_version(_DISTRIBUTION_NAME)
    except PackageNotFoundError:
        return "unknown"


__version__: str = get_version()
