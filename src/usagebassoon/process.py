# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""process.py — Own subprocesses within an operating-system process scope."""

from __future__ import annotations

import logging
import os
import shlex
import signal
import subprocess
from collections.abc import Callable, Mapping, Sequence
from types import ModuleType
from typing import IO, Protocol, cast

_LOG = logging.getLogger("usagebassoon")

type ProcessStream = int | IO[bytes] | None


def split_command(command: str, *, windows: bool) -> tuple[str, ...]:
    """Split a configured command using the selected platform's path rules.

    Args:
        command: Executable path followed by optional quoted arguments.
        windows: Preserve backslashes used by Windows executable paths.

    Returns:
        The executable and its arguments without surrounding quotes.
    """
    if not windows:
        return tuple(shlex.split(command))
    lexer = shlex.shlex(command, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    lexer.escape = ""
    return tuple(lexer)


def _platform_attribute(module: ModuleType, name: str) -> object:
    """Read an attribute that exists only on the active operating system."""
    return getattr(module, name)


class _ProcessScope(Protocol):
    """Own the operating-system resources used to contain one child process."""

    def terminate(self, process: subprocess.Popen[bytes]) -> None:
        """Terminate the process and any descendants in its scope."""

    def close(self) -> None:
        """Release the scope, terminating any remaining scoped processes."""


class _PosixProcessScope:
    """Contain one child in a new POSIX session and process group."""

    def terminate(self, process: subprocess.Popen[bytes]) -> None:
        """Kill the process group, falling back to the direct child on error."""
        kill_group = cast(
            "Callable[[int, int], None]", _platform_attribute(os, "killpg")
        )
        kill_signal = cast(int, _platform_attribute(signal, "SIGKILL"))
        try:
            kill_group(process.pid, kill_signal)
        except ProcessLookupError:
            return
        except OSError:
            _LOG.exception("could not terminate subprocess group %s", process.pid)
            try:
                process.kill()
            except ProcessLookupError:
                return
            except OSError:
                _LOG.exception("could not terminate subprocess %s", process.pid)

    def close(self) -> None:
        """Release no additional resources on POSIX."""


class ManagedProcess:
    """Expose subprocess operations with scoped termination semantics."""

    def __init__(
        self,
        process: subprocess.Popen[bytes],
        scope: _ProcessScope,
    ) -> None:
        """Wrap a started process and its platform-specific scope."""
        self._process = process
        self._scope = scope

    @property
    def pid(self) -> int:
        """Return the process identifier."""
        return self._process.pid

    @property
    def stdin(self) -> IO[bytes] | None:
        """Return the child's standard input stream, if configured."""
        return self._process.stdin

    @property
    def stdout(self) -> IO[bytes] | None:
        """Return the child's standard output stream, if configured."""
        return self._process.stdout

    @property
    def stderr(self) -> IO[bytes] | None:
        """Return the child's standard error stream, if configured."""
        return self._process.stderr

    @property
    def returncode(self) -> int | None:
        """Return the child's exit code when it has exited."""
        return self._process.returncode

    def poll(self) -> int | None:
        """Check whether the direct child has exited."""
        return self._process.poll()

    def wait(self, timeout: float | None = None) -> int:
        """Wait for the direct child to exit."""
        return self._process.wait(timeout=timeout)

    def kill(self) -> None:
        """Terminate the process scope, not unrelated processes with its name."""
        self._scope.terminate(self._process)

    def close(self) -> None:
        """Release process-scope resources and terminate remaining descendants."""
        self._scope.close()


def terminate_process(process: ManagedProcess | subprocess.Popen[bytes]) -> None:
    """Terminate a managed process or preserve legacy direct Popen cleanup."""
    if isinstance(process, ManagedProcess):
        process.kill()
    elif os.name == "posix":
        kill_group = cast(
            "Callable[[int, int], None]", _platform_attribute(os, "killpg")
        )
        kill_signal = cast(int, _platform_attribute(signal, "SIGKILL"))
        kill_group(process.pid, kill_signal)
    else:
        process.kill()


def start_process(
    args: Sequence[str],
    *,
    stdin: ProcessStream = None,
    stdout: ProcessStream = None,
    stderr: ProcessStream = None,
    env: Mapping[str, str] | None = None,
    shell: bool = False,
    bufsize: int = -1,
) -> ManagedProcess:
    """Start a child in its own process group or Windows Job Object.

    Args:
        args: Executable and command-line arguments.
        stdin: Standard input redirection.
        stdout: Standard output redirection.
        stderr: Standard error redirection.
        env: Child environment, or ``None`` to inherit the current environment.
        shell: Whether to invoke a command shell.
        bufsize: Buffering policy for configured pipes.

    Returns:
        A process handle with scoped termination and cleanup.
    """
    if os.name == "nt":
        from usagebassoon.win32process import start_windows_process

        return start_windows_process(
            args,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            env=env,
            shell=shell,
            bufsize=bufsize,
        )

    process = subprocess.Popen(
        list(args),
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        env=env,
        shell=shell,
        bufsize=bufsize,
        start_new_session=True,
    )
    return ManagedProcess(process, _PosixProcessScope())
