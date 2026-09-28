# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_collection_lock.py — Local collection exclusion and release behavior."""

from pathlib import Path

import pytest

from usagebassoon.collection_lock import CollectionBusy, collection_lock
from usagebassoon.config import UsageBassoonConfig


def test_local_collection_lock_excludes_other_database_and_releases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject simultaneous local collectors and release after a failed collection."""
    monkeypatch.setattr(
        "usagebassoon.collection_lock.gettempdir", lambda: str(tmp_path)
    )
    first = UsageBassoonConfig(tmp_path / "first.toml", "source-a", "duckdb")
    second = UsageBassoonConfig(tmp_path / "second.toml", "source-b", "duckdb")
    with pytest.raises(ValueError, match="collection failed"), collection_lock(first):
        with (
            pytest.raises(CollectionBusy, match="another UsageBassoon process"),
            collection_lock(second),
        ):
            pytest.fail("another local collector entered the critical section")
        raise ValueError("collection failed")
    with collection_lock(second):
        pass


@pytest.mark.parametrize("backend", ["motherduck", "bigquery"])
def test_remote_collection_does_not_take_local_lock(
    backend: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Permit concurrent remote collectors without allocating local lock state."""
    from typing import cast

    from usagebassoon.config import BackendName

    config = UsageBassoonConfig(
        tmp_path / "config.toml", "source", cast(BackendName, backend)
    )
    monkeypatch.setattr(
        "usagebassoon.collection_lock.gettempdir",
        lambda: pytest.fail("remote collection requested a local lock"),
    )
    with collection_lock(config), collection_lock(config):
        pass
