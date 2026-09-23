# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

from usagebassoon.api import connect, query, query_arrow
from usagebassoon.version import __version__

__all__ = ["__version__", "connect", "query", "query_arrow"]


def main() -> None:
    """Print the package placeholder entrypoint."""
    print("Hello from UsageBassoon!")
