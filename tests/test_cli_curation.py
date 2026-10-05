# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_curation.py — Typer command tests for curation operations."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import pytest
import yaml
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli import note as note_cli
from usagebassoon.cli.app import app
from usagebassoon.config import CONFIG_PATH_ENV_VAR
from usagebassoon.curation import NoteAssignment, set_note

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _write_config(path: Path, database: Path) -> None:
    """Write a local-DuckDB configuration for CLI integration tests."""
    path.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend.provider = "duckdb"\n'
        f'backend.duckdb.database = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    try:
        backend.apply_ddl()
    finally:
        backend.close()


def _command(config: Path, *parts: str) -> list[str]:
    """Return one curation command with its explicit configuration path."""
    return [*parts, "--config", str(config)]


def test_curation_commands_use_only_the_explicit_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Assert every curation subcommand honors its explicit configuration."""
    selected_config = tmp_path / "selected.toml"
    selected_database = tmp_path / "selected.duckdb"
    environment_config = tmp_path / "environment.toml"
    _write_config(selected_config, selected_database)
    _write_config(environment_config, tmp_path / "environment.duckdb")
    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(environment_config))
    runner = CliRunner()

    tag_result = runner.invoke(
        app,
        _command(selected_config, "tag", "add", "project-alpha", "--client", "codex"),
    )
    note_result = runner.invoke(
        app,
        _command(
            selected_config,
            "note",
            "set",
            "Track this session",
            "--client",
            "codex",
            "--session",
            "ses_123",
        ),
    )

    assert tag_result.exit_code == 0
    assert note_result.exit_code == 0
    backend = DuckDBBackend(selected_database)
    try:
        assert backend.query("SELECT tag FROM tags").to_pylist() == [
            {"tag": "project-alpha"}
        ]
        assert backend.query("SELECT note FROM notes").to_pylist() == [
            {"note": "Track this session"}
        ]
    finally:
        backend.close()


def test_curation_help_documents_the_supported_operations() -> None:
    """Expose each supported tag and note operation in command help."""
    runner = CliRunner()
    for command, operations in (
        ("tag", ("add", "rename", "remove")),
        ("note", ("set", "edit", "remove", "list", "describe")),
    ):
        result = runner.invoke(app, [command, "--help"])
        assert result.exit_code == 0
        output = plain_cli_output(result.stdout)
        for operation in operations:
            assert operation in output.split()


def test_tag_commands_support_each_scope_rename_and_remove(tmp_path: Path) -> None:
    """Assert tags mutate only at their declared complete target scope."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    runner = CliRunner()
    client = _command(config, "tag", "add", "client-tag", "--client", "codex")
    workspace = _command(config, "tag", "add", "workspace-tag", "--workspace", "/repo")
    session = _command(
        config,
        "tag",
        "add",
        "session-tag",
        "--client",
        "codex",
        "--session",
        "ses_123",
    )
    assert runner.invoke(app, client).exit_code == 0
    assert runner.invoke(app, workspace).exit_code == 0
    assert runner.invoke(app, session).exit_code == 0
    renamed = runner.invoke(
        app,
        _command(
            config,
            "tag",
            "rename",
            "session-tag",
            "renamed",
            "--client",
            "codex",
            "--session",
            "ses_123",
        ),
    )
    removed = runner.invoke(
        app, _command(config, "tag", "remove", "workspace-tag", "--workspace", "/repo")
    )
    assert renamed.exit_code == 0
    assert removed.exit_code == 0
    backend = DuckDBBackend(database)
    try:
        assert backend.query("SELECT tag FROM tags ORDER BY tag").to_pylist() == [
            {"tag": "client-tag"},
            {"tag": "renamed"},
        ]
    finally:
        backend.close()


def test_note_edit_uses_editor_and_does_not_write_on_invalid_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Assert editor success, blank content, and failures preserve note safety."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    runner = CliRunner()
    target = ["--client", "codex", "--session", "ses_123"]
    assert (
        runner.invoke(app, _command(config, "note", "set", "first", *target)).exit_code
        == 0
    )
    backend = DuckDBBackend(database)
    try:
        original = backend.query(
            "SELECT created_at, collected_at FROM notes"
        ).to_pylist()[0]
    finally:
        backend.close()

    editor = tmp_path / "editor.py"
    editor.write_text(
        "from pathlib import Path\nimport sys\n"
        "Path(sys.argv[-1]).write_text(sys.argv[1])\n"
        "sys.exit(1 if sys.argv[1] == 'failed content' else 0)\n"
    )
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.setenv("EDITOR", f"{sys.executable} {editor} edited")
    edited = runner.invoke(app, _command(config, "note", "edit", *target))
    assert edited.exit_code == 0
    assert "Updated note" in plain_cli_output(edited.output)

    backend = DuckDBBackend(database)
    try:
        revised = backend.query(
            "SELECT note, created_at, collected_at FROM notes"
        ).to_pylist()[0]
        assert revised["note"] == "edited"
        assert revised["created_at"] == original["created_at"]
        assert revised["collected_at"] > original["collected_at"]
    finally:
        backend.close()

    for content in ("   ", "failed content"):
        monkeypatch.setenv("EDITOR", f"{sys.executable} {editor} '{content}'")
        rejected = runner.invoke(app, _command(config, "note", "edit", *target))
        assert rejected.exit_code != 0
        if not content.strip():
            assert "note remove" in plain_cli_output(rejected.output)
        backend = DuckDBBackend(database)
        try:
            assert backend.query(
                "SELECT note, created_at, collected_at FROM notes"
            ).to_pylist() == [revised]
        finally:
            backend.close()


def test_note_edit_requires_a_configured_editor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Assert edit directs users to set when no editor has been configured."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usage.duckdb")
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.delenv("EDITOR", raising=False)
    result = CliRunner().invoke(
        app,
        _command(config, "note", "edit", "--client", "codex", "--session", "ses_123"),
    )
    assert result.exit_code != 0
    assert "note set" in plain_cli_output(result.output)


def _seed_notes(database: Path, count: int) -> list[NoteAssignment]:
    """Create ordered notes independently of the CLI's wall-clock timestamps."""
    backend = DuckDBBackend(database)
    notes = []
    created = datetime(2026, 9, 1, tzinfo=UTC)
    try:
        for index in range(count):
            note = NoteAssignment(
                SOURCE_ID,
                "codex",
                str(index),
                f"note{index:02d}\n one two three four five six seven eight nine "
                "hidden words",
            )
            notes.append(note)
            set_note(backend, note, at=created + timedelta(seconds=index))
    finally:
        backend.close()
    return notes


def test_note_list_defaults_to_16_rows_and_supports_page_selection(
    tmp_path: Path,
) -> None:
    """Show recent notes and only ten words while keeping piped output bounded."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    notes = _seed_notes(database, 35)
    runner = CliRunner()
    result = runner.invoke(app, _command(config, "note", "list"))
    assert result.exit_code == 0
    output = plain_cli_output(result.stdout)
    rows = output.splitlines()
    assert rows[0].split() == ["Note", "ID", "Note"]
    assert len(rows) == 17
    assert rows[1].split()[0] == notes[34].note_id
    assert rows[-1].split()[0] == notes[19].note_id
    assert "hidden" not in output
    assert "--page 2" in plain_cli_output(result.stderr)
    last = runner.invoke(app, _command(config, "note", "list", "--page", "3"))
    assert last.exit_code == 0
    assert [
        line.split()[0] for line in plain_cli_output(last.stdout).splitlines()[1:]
    ] == [note.note_id for note in reversed(notes[:3])]
    assert (
        runner.invoke(app, _command(config, "note", "list", "--page", "0")).exit_code
        != 0
    )
    empty = runner.invoke(
        app, _command(config, "note", "list", "--source-id", "absent")
    )
    assert empty.exit_code == 0
    assert "No notes found" in plain_cli_output(empty.stdout)


def test_note_list_pages_interactively_and_json_never_prompts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Enter continues in the same invocation, while quit/EOF and JSON remain safe."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    notes = _seed_notes(database, 35)
    monkeypatch.setattr(note_cli, "_can_page", lambda: True)
    runner = CliRunner()
    result = runner.invoke(app, _command(config, "note", "list"), input="\nq\n")
    assert result.exit_code == 0
    output = plain_cli_output(result.stdout)
    for note in notes[3:]:
        assert note.note_id in output
    for note in notes[:3]:
        assert note.note_id not in output
    assert "Enter for next 16 notes" in output
    for extra in ((), ("--no-pager",)):
        stopped = runner.invoke(app, _command(config, "note", "list", *extra), input="")
        assert stopped.exit_code == 0
        assert notes[18].note_id not in plain_cli_output(stopped.stdout)
    structured = runner.invoke(app, _command(config, "note", "list", "--json"))
    assert structured.exit_code == 0
    rows = json.loads(plain_cli_output(structured.stdout))
    assert len(rows) == 16
    assert rows[0] == {
        "note_id": notes[34].note_id,
        "note": "note34 one two three four five six seven eight nine …",
    }
    assert "Enter for next" not in plain_cli_output(structured.stdout)


def test_note_list_escapes_terminal_controls(tmp_path: Path) -> None:
    """Keep control sequences inside note previews from affecting the terminal."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    backend = DuckDBBackend(database)
    try:
        set_note(
            backend,
            NoteAssignment(
                SOURCE_ID, "codex", "unsafe", "\x1b[31mred [bold]text[/bold]"
            ),
        )
    finally:
        backend.close()
    result = CliRunner().invoke(app, _command(config, "note", "list"))
    assert result.exit_code == 0
    assert "\\u001b[31mred [bold]text[/bold]" in plain_cli_output(result.stdout)


@pytest.mark.parametrize("json_output", [False, True])
def test_note_describe_uses_shared_formats_for_one_or_multiple_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, json_output: bool
) -> None:
    """Produce ordered metadata using the shared YAML/JSON serializer."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    notes = _seed_notes(database, 2)
    calls = []
    original = note_cli.render_records

    def recorded(
        rows: Sequence[Mapping[str, object]],
        *,
        columns: Sequence[str],
        format: Literal["json", "csv", "yaml"],
        save: Path | None,
        json_payload: Mapping[str, object] | None = None,
    ) -> None:
        """Record calls while preserving shared serialization."""
        calls.append(format)
        original(
            rows, columns=columns, format=format, save=save, json_payload=json_payload
        )

    monkeypatch.setattr(note_cli, "render_records", recorded)
    for selected in ([notes[0]], [notes[1], notes[0]]):
        flags = ["--json"] if json_output else []
        result = CliRunner().invoke(
            app,
            _command(
                config, "note", "describe", *(note.note_id for note in selected), *flags
            ),
        )
        assert result.exit_code == 0
        output = plain_cli_output(result.stdout)
        rows = json.loads(output) if json_output else yaml.safe_load(output)
        assert [row["note_id"] for row in rows] == [note.note_id for note in selected]
        for row, note in zip(rows, selected, strict=True):
            assert row == {
                "note_id": note.note_id,
                "source_id": SOURCE_ID,
                "client": "codex",
                "session_id": note.session_id,
                "created_at": (
                    datetime(2026, 9, 1, tzinfo=UTC)
                    + timedelta(seconds=int(note.session_id))
                ).isoformat(),
                "updated_at": (
                    datetime(2026, 9, 1, tzinfo=UTC)
                    + timedelta(seconds=int(note.session_id))
                ).isoformat(),
            }
    assert calls == ["json" if json_output else "yaml"] * 2


def test_note_describe_rejects_missing_and_invalid_ids_without_partial_output(
    tmp_path: Path,
) -> None:
    """Fail clearly rather than silently omit unknown IDs from a description."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    note = _seed_notes(database, 1)[0]
    unknown = NoteAssignment(SOURCE_ID, "codex", "absent", "absent").note_id
    for invalid in (unknown, "invalid-id"):
        result = CliRunner().invoke(
            app, _command(config, "note", "describe", note.note_id, invalid, "--json")
        )
        assert result.exit_code != 0
        assert plain_cli_output(result.stdout) == ""


def test_note_id_edit_and_remove_resolve_the_notes_own_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """UUID selection reaches another source without changing the configured key."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    original = NoteAssignment("another-source", "codex", "session", "first")
    own = NoteAssignment(SOURCE_ID, "codex", "session", "untouched")
    backend = DuckDBBackend(database)
    try:
        set_note(backend, original)
        set_note(backend, own)
    finally:
        backend.close()
    editor = tmp_path / "editor.py"
    editor.write_text(
        "from pathlib import Path\nimport sys\n"
        "Path(sys.argv[-1]).write_text('edited')\n"
    )
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.setenv("EDITOR", f"{sys.executable} {editor}")
    runner = CliRunner()
    edited = runner.invoke(
        app, _command(config, "note", "edit", "--id", original.note_id)
    )
    assert edited.exit_code == 0
    described = runner.invoke(
        app, _command(config, "note", "describe", original.note_id, "--json")
    )
    assert described.exit_code == 0
    row = json.loads(plain_cli_output(described.stdout))[0]
    assert row["source_id"] == original.source_id
    assert row["updated_at"] > row["created_at"]
    backend = DuckDBBackend(database)
    try:
        assert backend.query(
            "SELECT note FROM session_notes WHERE note_id = :note_id",
            {"note_id": original.note_id},
        ).column("note").to_pylist() == ["edited"]
    finally:
        backend.close()
    assert (
        runner.invoke(
            app, _command(config, "note", "remove", "--id", original.note_id)
        ).exit_code
        == 0
    )
    backend = DuckDBBackend(database)
    try:
        assert backend.query("SELECT note_id, note FROM session_notes").to_pylist() == [
            {"note_id": own.note_id, "note": own.note}
        ]
    finally:
        backend.close()
    for selector in (
        (),
        ["--client", "codex"],
        ["--id", own.note_id, "--client", "codex", "--session", "session"],
    ):
        result = runner.invoke(app, _command(config, "note", "remove", *selector))
        assert result.exit_code != 0
