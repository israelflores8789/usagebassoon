# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""collection_lock.py — Exclude simultaneous collection by one local user."""

import os
from collections.abc import Generator
from contextlib import contextmanager
from getpass import getuser
from pathlib import Path
from tempfile import gettempdir

from usagebassoon.config import UsageBassoonConfig


class CollectionBusy(RuntimeError):
    """Another local process is already collecting for this user environment."""


@contextmanager
def collection_lock(config: UsageBassoonConfig) -> Generator[None]:
    """Hold a nonblocking OS lock before local DuckDB collection begins."""
    if config.backend != "duckdb":
        yield
        return
    user = str(os.getuid()) if os.name != "nt" else getuser()
    path = Path(gettempdir()) / f"usagebassoon-{user}" / "collection.lock"
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt

            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as error:
                raise CollectionBusy(
                    "another UsageBassoon process is collecting"
                ) from error
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise CollectionBusy(
                    "another UsageBassoon process is collecting"
                ) from error
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
