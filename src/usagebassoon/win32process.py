# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""win32process.py — Contain one child invocation in a Windows Job Object."""

from __future__ import annotations

import ctypes
import logging
import subprocess
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from importlib import import_module
from types import ModuleType
from typing import Protocol, cast

from usagebassoon.process import ManagedProcess, ProcessStream

_LOG = logging.getLogger("usagebassoon")
_CLEANUP_SECONDS = 2.0


def _api_attribute(module: ModuleType, name: str) -> object:
    """Read a dynamically loaded Windows API attribute."""
    return getattr(module, name)


def _api_integer(module: ModuleType, name: str) -> int:
    """Read an integer Windows API constant."""
    return cast(int, _api_attribute(module, name))


class _WindowsHandle(Protocol):
    """Subset of a pywin32 handle used by this module."""

    def Close(self) -> None:
        """Close the native handle."""

    def __int__(self) -> int:
        """Return the native handle value."""


class _WindowsJob:
    """Keep a kill-on-close job alive for one managed child invocation."""

    def __init__(self, handle: _WindowsHandle) -> None:
        """Own a configured Windows Job Object handle."""
        self._handle: _WindowsHandle | None = handle

    def terminate(self, process: subprocess.Popen[bytes]) -> None:
        """Close the job, which terminates only its associated process tree."""
        del process
        self.close()

    def close(self) -> None:
        """Close the job handle and terminate any processes still assigned to it."""
        if self._handle is None:
            return
        try:
            self._handle.Close()
        except Exception as error:
            raise _as_os_error("close the Windows process job", error) from error
        self._handle = None


def _as_os_error(operation: str, error: Exception) -> OSError:
    """Convert a native Windows error to a stable, actionable OSError."""
    winerror = getattr(error, "winerror", None)
    if isinstance(winerror, int):
        return OSError(winerror, f"could not {operation} (Windows error {winerror})")
    return OSError(f"could not {operation}: {error}")


def _create_job() -> _WindowsJob:
    """Create a job that terminates assigned processes when its handle closes."""
    job: _WindowsHandle | None = None
    try:
        win32job = import_module("win32job")
        create_job = cast(
            "Callable[[object | None, str], _WindowsHandle]",
            _api_attribute(win32job, "CreateJobObject"),
        )
        job = create_job(None, "")
        query = cast(
            "Callable[[int, int], dict[str, object]]",
            _api_attribute(win32job, "QueryInformationJobObject"),
        )
        set_information = cast(
            "Callable[[int, int, dict[str, object]], None]",
            _api_attribute(win32job, "SetInformationJobObject"),
        )
        information_class = _api_integer(win32job, "JobObjectExtendedLimitInformation")
        information = query(int(job), information_class)
        basic_limits = cast(
            "MutableMapping[str, int]", information["BasicLimitInformation"]
        )
        basic_limits["LimitFlags"] = _api_integer(
            win32job, "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE"
        )
        set_information(int(job), information_class, information)
        return _WindowsJob(job)
    except Exception as error:
        if job is not None:
            try:
                job.Close()
            except Exception:
                _LOG.exception("could not close an unconfigured Windows process job")
        if isinstance(error, OSError):
            raise
        raise _as_os_error("create a Windows process job", error) from error


def _resume_process(process_handle: _WindowsHandle) -> None:
    """Resume a suspended process after it has been assigned to its job."""
    try:
        load_library = cast("Callable[[str], object]", _api_attribute(ctypes, "WinDLL"))
        function_factory = cast(
            "Callable[..., Callable[..., int]]",
            _api_attribute(ctypes, "WINFUNCTYPE"),
        )
        ntdll = load_library("ntdll")
        resume_type = function_factory(ctypes.c_long, ctypes.c_void_p)
        resume = cast(
            "Callable[[int], int]",
            resume_type(("NtResumeProcess", ntdll)),
        )
        status = resume(int(process_handle))
    except Exception as error:
        raise _as_os_error("resume the contained Windows child", error) from error
    if status != 0:
        raise OSError(
            "could not resume the contained Windows child "
            f"(NTSTATUS 0x{status & 0xFFFFFFFF:08x})"
        )


def _assign_and_resume(pid: int, job: _WindowsJob) -> None:
    """Assign a suspended process to its job before allowing it to execute."""
    process_handle: _WindowsHandle | None = None
    try:
        win32api = import_module("win32api")
        win32con = import_module("win32con")
        win32job = import_module("win32job")
        open_process = cast(
            "Callable[[int, bool, int], _WindowsHandle]",
            _api_attribute(win32api, "OpenProcess"),
        )
        assign_process = cast(
            "Callable[[int, int], None]",
            _api_attribute(win32job, "AssignProcessToJobObject"),
        )
        access = (
            _api_integer(win32con, "PROCESS_SET_QUOTA")
            | _api_integer(win32con, "PROCESS_TERMINATE")
            | 0x0800
        )
        process_handle = open_process(access, False, pid)
        job_handle = cast(_WindowsHandle, job._handle)
        assign_process(int(job_handle), int(process_handle))
        _resume_process(process_handle)
    except Exception as error:
        if isinstance(error, OSError):
            raise
        raise _as_os_error(
            "assign the Windows child to its process job", error
        ) from error
    finally:
        if process_handle is not None:
            try:
                process_handle.Close()
            except Exception:
                _LOG.exception("could not close the Windows child process handle")


def _cleanup_failed_start(
    process: subprocess.Popen[bytes] | None,
    job: _WindowsJob,
) -> None:
    """Stop a partially started process and close its handles after launch fails."""
    try:
        job.close()
    except OSError:
        _LOG.exception("could not close a Windows process job after launch failed")
    if process is None:
        return
    try:
        process.kill()
    except ProcessLookupError:
        pass
    except OSError:
        _LOG.exception("could not terminate an uncontained Windows child")
    try:
        process.wait(timeout=_CLEANUP_SECONDS)
    except subprocess.TimeoutExpired:
        _LOG.warning("Windows child %s did not exit after launch failed", process.pid)
    except OSError:
        _LOG.exception("could not reap a Windows child after launch failed")
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None:
            try:
                stream.close()
            except OSError:
                _LOG.exception("could not close a Windows child process stream")


def start_windows_process(
    args: Sequence[str],
    *,
    stdin: ProcessStream = None,
    stdout: ProcessStream = None,
    stderr: ProcessStream = None,
    env: Mapping[str, str] | None = None,
    shell: bool = False,
    bufsize: int = -1,
) -> ManagedProcess:
    """Start the child suspended, assign it to a job, then resume it.

    Args:
        args: Executable and command-line arguments.
        stdin: Standard input redirection.
        stdout: Standard output redirection.
        stderr: Standard error redirection.
        env: Child environment, or ``None`` to inherit the current environment.
        shell: Whether to invoke a command shell.
        bufsize: Buffering policy for configured pipes.

    Returns:
        A process handle whose job contains only this invocation and descendants.

    Raises:
        OSError: If the process cannot be created or assigned to its job.
    """
    job = _create_job()
    process: subprocess.Popen[bytes] | None = None
    try:
        win32con = import_module("win32con")
        process = subprocess.Popen(
            list(args),
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            env=env,
            shell=shell,
            bufsize=bufsize,
            creationflags=_api_integer(win32con, "CREATE_SUSPENDED"),
        )
        _assign_and_resume(process.pid, job)
        return ManagedProcess(process, job)
    except Exception as error:
        _cleanup_failed_start(process, job)
        if isinstance(error, OSError):
            raise
        raise _as_os_error(
            "start and contain a Windows child process", error
        ) from error
