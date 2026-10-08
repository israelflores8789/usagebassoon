# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""system_metadata.py — Collection invocation metadata and best-effort host capture."""

from __future__ import annotations

import logging
import os
import platform
import subprocess
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

_LOG = logging.getLogger("usagebassoon")


class InvokeMethod(StrEnum):
    """Explicit entry point that initiated a UsageBassoon collection cycle."""

    CLI = "cli"
    SYSTEMD = "systemd"
    LAUNCHD = "launchd"
    WORKER = "worker"
    PYTHON = "python"


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
    """

    os_name: str | None
    os_version: str | None
    architecture: str | None
    cpu_model: str | None
    cpu_count: int | None
    memory_bytes: int | None


def _physical_memory_bytes() -> int | None:
    """Return physical memory capacity using portable POSIX interfaces.

    Returns:
        Total physical memory in bytes, or None when unavailable.
    """
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        page_count = os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        _LOG.exception("physical memory metadata is unavailable")
        return None
    if not isinstance(page_size, int) or not isinstance(page_count, int):
        return None
    return page_size * page_count


def _linux_cpu_model() -> str | None:
    """Read a bounded processor description from Linux's CPU metadata."""
    with Path("/proc/cpuinfo").open(encoding="utf-8") as cpuinfo:
        text = cpuinfo.read(65_536)
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if separator and key.strip().casefold() in {"model name", "processor"}:
            description = value.strip()
            if description and not description.isdecimal():
                return description
    return None


def _macos_cpu_model() -> str | None:
    """Query the macOS processor brand with a bounded direct subprocess."""
    result = subprocess.run(
        ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=True,
        timeout=2,
    )
    return result.stdout.strip() or None


def _windows_cpu_model() -> str | None:
    """Query Windows CIM for the processor's model with a bounded subprocess."""
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
            "$ErrorActionPreference = 'Stop'; "
            "(Get-CimInstance -ClassName Win32_Processor | "
            "Select-Object -First 1).Name",
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
        timeout=5,
    )
    return result.stdout.strip() or None


def _cpu_model(system: str) -> str | None:
    """Discover a processor model without dropping independent host metadata."""
    try:
        match system:
            case "Linux":
                model = _linux_cpu_model()
            case "Darwin":
                model = _macos_cpu_model()
            case "Windows":
                model = _windows_cpu_model()
            case _:
                model = None
        if model:
            return model
    except Exception:
        _LOG.exception("platform CPU model capture failed for %s", system)
    try:
        return platform.processor().strip() or None
    except Exception:
        _LOG.exception("portable CPU model capture failed")
        return None


def capture_system_metadata() -> SystemMetadata:
    """Capture best-effort system metadata for an ingest audit row.

    Returns:
        Metadata that is safe to omit field by field on constrained platforms.
    """
    try:
        system = platform.system().strip()
        return SystemMetadata(
            os_name=system or None,
            os_version=platform.release().strip() or None,
            architecture=platform.machine().strip() or None,
            cpu_model=_cpu_model(system),
            cpu_count=os.cpu_count(),
            memory_bytes=_physical_memory_bytes(),
        )
    except Exception:
        _LOG.exception("system metadata capture failed; omitting host metadata")
        return SystemMetadata(
            os_name=None,
            os_version=None,
            architecture=None,
            cpu_model=None,
            cpu_count=None,
            memory_bytes=None,
        )
