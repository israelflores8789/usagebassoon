# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""system_metadata.py — Best-effort collector-host metadata capture."""

from __future__ import annotations

import os
import platform
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SystemMetadata:
    """Stable attributes of the host that performed one collection run.

    Attributes:
        os_name: Operating-system family name.
        os_version: Operating-system release string.
        architecture: Host machine architecture.
        cpu_model: Processor description when the platform exposes one.
        cpu_count: Number of logical CPUs when known.
        memory_bytes: Physical memory size when the platform exposes it.
        shell: Value of the collector process's shell environment variable.
    """

    os_name: str | None
    os_version: str | None
    architecture: str | None
    cpu_model: str | None
    cpu_count: int | None
    memory_bytes: int | None
    shell: str | None


def _physical_memory_bytes() -> int | None:
    """Return physical memory capacity using portable POSIX interfaces.

    Returns:
        Total physical memory in bytes, or None when unavailable.
    """
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        page_count = os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None
    if not isinstance(page_size, int) or not isinstance(page_count, int):
        return None
    return page_size * page_count


def capture_system_metadata() -> SystemMetadata:
    """Capture best-effort system metadata for an ingest audit row.

    Returns:
        Metadata that is safe to omit field by field on constrained platforms.
    """
    processor = platform.processor().strip() or None
    return SystemMetadata(
        os_name=platform.system().strip() or None,
        os_version=platform.release().strip() or None,
        architecture=platform.machine().strip() or None,
        cpu_model=processor,
        cpu_count=os.cpu_count(),
        memory_bytes=_physical_memory_bytes(),
        shell=os.environ.get("SHELL") or None,
    )
