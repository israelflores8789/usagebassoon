# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_collect.py — Typer integration tests for collection delegation."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon.cli.app import app
from usagebassoon.collection_lock import CollectionBusy
from usagebassoon.config import UsageBassoonConfig
from usagebassoon.ingest import IngestStatus
from usagebassoon.persistence import PersistSummary
from usagebassoon.system_metadata import InvokeMethod

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _write_config(path: Path, database: Path) -> None:
    """Write a local-DuckDB configuration for collection command tests."""
    path.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend.provider = "duckdb"\n'
        f'backend.duckdb.database = "{database}"\n'
    )


def test_collect_reports_the_delegated_persistence_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Display the run identity and persistence counts returned by the collector."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usagebassoon.duckdb")

    def collect_run(
        configuration: UsageBassoonConfig, *, invoke_method: InvokeMethod
    ) -> tuple[str, PersistSummary]:
        """Return a deterministic collector outcome for the CLI boundary."""
        assert configuration.path == config
        assert invoke_method is InvokeMethod.CLI
        return "run-123", PersistSummary(inserted=4, updated=2, per_table={})

    monkeypatch.setattr("usagebassoon.cli.collect.collect_run", collect_run)

    result = CliRunner().invoke(app, ["collect", "--config", str(config)])

    assert result.exit_code == 0
    assert (
        plain_cli_output(result.output)
        == "Collected run run-123: 4 inserted, 2 updated.\n"
    )


@pytest.mark.parametrize("refresh", [False, True])
def test_manual_collection_warns_after_persisting_partial_data(
    refresh: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both manual entry points explain partial publication and the retry action."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usagebassoon.duckdb")
    target = IngestStatus(
        date(2025, 1, 1), "models", "failed", 1, 0, "partial-run", "fetch"
    )

    def collect_run(
        _configuration: UsageBassoonConfig,
        **_kwargs: object,
    ) -> tuple[str, PersistSummary]:
        """Return successful publication containing an unfinished acquisition target."""
        return "partial-run", PersistSummary(4, 0, {}, (target,))

    monkeypatch.setattr("usagebassoon.cli.collect.collect_run", collect_run)
    args = ["collect", *(["refresh"] if refresh else []), "--config", str(config)]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0
    assert "4 inserted" in plain_cli_output(result.stdout)
    assert "Warning: collection is incomplete" in plain_cli_output(result.stderr)
    assert "Run bassoon collect again" in plain_cli_output(result.stderr)


@pytest.mark.parametrize(
    "override", [None, "STORAGE_EMULATOR_HOST", "API_ENDPOINT_OVERRIDE"]
)
def test_collect_formats_configuration_errors_as_cli_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    override: str | None,
) -> None:
    """Report fatal configuration failures before starting collection."""
    missing = tmp_path / "missing.toml"
    if override is not None:
        _write_config(missing, tmp_path / "usagebassoon.duckdb")
        with missing.open("a") as stream:
            stream.write(
                '[snapshots.gcs]\nenable = true\nuri = "gs://bucket/archive"\n'
                'project = "usagebassoon-test"\n'
            )
        for variable in ("STORAGE_EMULATOR_HOST", "API_ENDPOINT_OVERRIDE"):
            monkeypatch.delenv(variable, raising=False)
        monkeypatch.setenv(override, "http://localhost:4443")
    collect = MagicMock()
    monkeypatch.setattr("usagebassoon.cli.collect.collect_run", collect)
    monkeypatch.setenv("FORCE_COLOR", "1")
    monkeypatch.setenv("TERM", "xterm-256color")

    result = CliRunner().invoke(app, ["collect", "--config", str(missing)])

    assert result.exit_code == 2
    output = plain_cli_output(result.output)
    assert "--config" in output
    if override is not None:
        assert f"{override} must be unset" in output
        assert "http://localhost:4443" not in output
    collect.assert_not_called()


def test_collect_reports_local_collection_contention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report local collection contention as a clean CLI error."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usagebassoon.duckdb")

    def collect_run(
        _configuration: UsageBassoonConfig, *, invoke_method: InvokeMethod
    ) -> tuple[str, PersistSummary]:
        """Simulate a local collection already holding the environment lock."""
        assert invoke_method is InvokeMethod.CLI
        raise CollectionBusy("source is already collecting")

    monkeypatch.setattr("usagebassoon.cli.collect.collect_run", collect_run)
    result = CliRunner().invoke(app, ["collect", "--config", str(config)])

    assert result.exit_code == 1
    assert plain_cli_output(result.output) == (
        "Collection failed: source is already collecting\n"
    )


def test_collect_formats_unexpected_errors_without_a_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log unexpected command errors and return a clean nonzero CLI status."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usagebassoon.duckdb")

    def collect_run(
        _configuration: UsageBassoonConfig, *, invoke_method: InvokeMethod
    ) -> tuple[str, PersistSummary]:
        """Raise an unexpected exception from the delegated collector."""
        assert invoke_method is InvokeMethod.CLI
        raise TypeError("unexpected collector failure")

    monkeypatch.setattr("usagebassoon.cli.collect.collect_run", collect_run)

    with caplog.at_level(logging.ERROR, logger="usagebassoon"):
        result = CliRunner().invoke(app, ["collect", "--config", str(config)])

    assert result.exit_code == 1
    assert (
        "Collection failed unexpectedly; see the operational log."
        in plain_cli_output(result.output)
    )
    assert "Traceback" not in plain_cli_output(result.output)
    assert "unexpected collection command failure" in caplog.text


@pytest.mark.parametrize(
    ("options", "since", "until", "parent_config"),
    [
        ([], None, None, False),
        (["--since", "2026-08-01"], date(2026, 8, 1), None, False),
        (["--until", "2026-09-10"], None, date(2026, 9, 10), False),
        (
            ["--since", "2026-08-01", "--until", "2026-09-10"],
            date(2026, 8, 1),
            date(2026, 9, 10),
            True,
        ),
    ],
)
def test_collect_refresh_delegates_source_and_date_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    options: list[str],
    since: date | None,
    until: date | None,
    parent_config: bool,
) -> None:
    """Support ISO date bounds and configuration on either side of refresh."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usagebassoon.duckdb")
    calls = 0

    def collect_run(
        configuration: UsageBassoonConfig,
        *,
        refresh: bool,
        since: date | None,
        until: date | None,
        invoke_method: InvokeMethod,
    ) -> tuple[str, PersistSummary]:
        """Record exactly one source-scoped refresh invocation."""
        nonlocal calls
        calls += 1
        assert configuration.source_id == SOURCE_ID
        assert configuration.path == config
        assert refresh
        assert invoke_method is InvokeMethod.CLI
        assert (since, until) == expected
        return "refresh-run", PersistSummary(2, 1, {})

    expected = (since, until)
    monkeypatch.setattr("usagebassoon.cli.collect.collect_run", collect_run)
    args = (
        ["collect", "--config", str(config), "refresh"]
        if parent_config
        else ["collect", "refresh", "--config", str(config)]
    )
    result = CliRunner().invoke(app, args + options)
    assert result.exit_code == 0, result.output
    assert calls == 1
    assert "Collected run refresh-run" in plain_cli_output(result.output)


@pytest.mark.parametrize(
    "options",
    [
        ["--since", "20260910"],
        ["--until", "2026-02-30"],
        ["--since", "2026-09-11", "--until", "2026-09-10"],
        ["--until", (datetime.now(UTC).date() + timedelta(days=1)).isoformat()],
    ],
)
def test_collect_refresh_rejects_invalid_bounds_before_acquisition(
    tmp_path: Path,
    options: list[str],
) -> None:
    """Invalid or future ranges fail without opening an uninitialized backend."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "absent.duckdb")
    result = CliRunner().invoke(
        app, ["collect", "refresh", "--config", str(config), *options]
    )
    assert result.exit_code != 0
    output = plain_cli_output(result.output)
    assert "--since" in output or "--until" in output
    assert "schema" not in output
    assert not (tmp_path / "absent.duckdb").exists()
