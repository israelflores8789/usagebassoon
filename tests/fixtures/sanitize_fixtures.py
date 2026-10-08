# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""sanitize_fixtures.py — sanitize tokscale JSON fixtures for version control.

Usage:
    python tests/fixtures/sanitize_fixtures.py tests/fixtures/temp
    python tests/fixtures/sanitize_fixtures.py capture.json --output-dir sanitized
    python tests/fixtures/sanitize_fixtures.py captures/ --replace 'secret=redacted'

Input files are never modified. Directory inputs are searched recursively for
JSON files, and outputs retain their relative paths under ``output/`` by default.
``--replace OLD=NEW`` applies a literal substring replacement to JSON string
values before automatic session, workspace, and local-path sanitization. When
OLD exactly matches a workspace path or an unambiguous label, NEW replaces every
representation of that workspace. If OLD starts with ``/``, the replacement keeps
that leading slash unless NEW already starts with one. Repeat the option for more
replacements. Session IDs retain their original pseudonym scheme. Readable
project labels are assigned consistently across the batch. Workspace paths
remain distinct even when their labels match; record-local labels follow their
workspace path. Narrative titles and descriptions are redacted; task categories,
complexity classifications, and obfuscated task-group labels are retained.
Existing output files are never overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import cast

type JsonValue = (
    bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None
)

SESSION_FIELDS = {"session", "sessionid", "sessions", "sessionids"}
WORKSPACE_FIELDS = {
    "workspace",
    "workspaces",
    "workspacelabel",
    "workspacelabels",
    "workspacepath",
    "project",
    "projects",
    "projectname",
    "projectnames",
    "cwd",
    "workingdirectory",
}
SUMMARY_FIELDS = {"title", "description"}
PUBLIC_FIELDS = {
    "client",
    "model",
    "modelid",
    "modelsused",
    "provider",
    "providerid",
    "source",
    "version",
    "tokscaleversion",
    "matchedkey",
    "kind",
    "groupby",
    "taskcategory",
    "complexity",
}
ROLLOUT_SESSION_ID = re.compile(
    r"^(rollout-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})-"
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
    re.IGNORECASE,
)
LOCAL_PATH = re.compile(
    r"(?<![\w:/\\])(?:~/[^\s\"'<>`]+|"
    r"[A-Za-z]:[\\/][^\s\"'<>`]+|"
    r"\\\\[^\s\"'<>`]+|/[^\s\"'<>`]+)"
)
PROJECT_LABELS = (
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "zeta",
    "eta",
    "theta",
    "iota",
    "kappa",
    "lambda",
    "mu",
    "nu",
    "xi",
    "omicron",
    "pi",
    "rho",
    "sigma",
    "tau",
    "upsilon",
    "phi",
    "chi",
    "psi",
    "omega",
)


def _normalized_field_name(name: str) -> str:
    """Return a case- and punctuation-insensitive JSON field name."""
    return "".join(character for character in name.casefold() if character.isalnum())


def _walk(
    value: JsonValue, key: str | None = None
) -> Iterator[tuple[str | None, JsonValue]]:
    """Yield each JSON value together with its containing object key."""
    if isinstance(value, dict):
        for child_key, child in value.items():
            yield child_key, child
            yield from _walk(child, child_key)
    elif isinstance(value, list):
        for child in value:
            yield key, child
            yield from _walk(child, key)


def _collect_sensitive_values(
    documents: list[JsonValue],
) -> tuple[set[str], set[str]]:
    """Collect session IDs and workspace names shared across the input batch."""
    session_ids: set[str] = set()
    workspace_values: set[str] = set()
    for document in documents:
        for key, value in _walk(document):
            if key is None or not isinstance(value, str):
                continue
            normalized_key = _normalized_field_name(key)
            if normalized_key in SESSION_FIELDS and value:
                session_ids.add(value)
            if normalized_key in WORKSPACE_FIELDS and value.strip():
                workspace_values.add(value.strip())
    return session_ids, workspace_values


def _digest_text(value: str, length: int) -> str:
    """Return a deterministic hexadecimal pseudonym of the requested length."""
    chunks: list[str] = []
    counter = 0
    while sum(map(len, chunks)) < length:
        seed = f"usagebassoon-fixture-v1:{counter}:{value}".encode()
        chunks.append(hashlib.sha256(seed).hexdigest())
        counter += 1
    return "".join(chunks)[:length]


def _pseudonymize_session_id(session_id: str) -> str:
    """Return a stable pseudonym while preserving familiar tokscale ID shapes."""
    rollout_match = ROLLOUT_SESSION_ID.fullmatch(session_id)
    if rollout_match is not None:
        token = _digest_text(session_id, 32)
        return (
            f"{rollout_match.group(1)}-{token[:8]}-{token[8:12]}-"
            f"{token[12:16]}-{token[16:20]}-{token[20:32]}"
        )
    if session_id.startswith("ses_"):
        suffix = session_id[4:]
        return f"ses_{_digest_text(session_id, len(suffix))}"
    if session_id.startswith("session_"):
        suffix = session_id[len("session_") :]
        return f"session_{_digest_text(session_id, max(16, len(suffix)))}"
    return f"session_{_digest_text(session_id, 16)}"


def _project_label(index: int) -> str:
    """Return a readable stable label for one distinct workspace."""
    if index < len(PROJECT_LABELS):
        return f"project-{PROJECT_LABELS[index]}"
    return f"project-{index + 1}"


def _workspace_identity(value: str) -> str:
    """Normalize full workspace paths without merging POSIX path casing."""
    stripped = value.strip().rstrip("/\\")
    if re.match(r"^[A-Za-z]:[\\/]", stripped) or stripped.startswith("\\\\"):
        return stripped.replace("\\", "/").casefold()
    return stripped or value.strip()


def _is_workspace_path(value: str) -> bool:
    """Return whether a workspace value contains path separators."""
    return "/" in value or "\\" in value


def _strings(value: JsonValue) -> Iterator[str]:
    """Yield strings from one field, retaining array membership."""
    if isinstance(value, str):
        if value.strip():
            yield value.strip()
    elif isinstance(value, list):
        for child in value:
            yield from _strings(child)


def _record_workspaces(record: dict[str, JsonValue]) -> set[str]:
    """Collect workspace fields directly attached to one JSON record."""
    return {
        text
        for key, value in record.items()
        if _normalized_field_name(key) in WORKSPACE_FIELDS
        for text in _strings(value)
    }


def _workspace_records(documents: list[JsonValue]) -> Iterator[dict[str, JsonValue]]:
    """Yield records that can associate a workspace path with its labels."""
    for document in documents:
        if isinstance(document, dict):
            yield document
        for _key, value in _walk(document):
            if isinstance(value, dict):
                yield value


def _workspace_replacements(
    workspace_values: set[str],
    extra_replacements: list[tuple[str, str]],
    *,
    documents: list[JsonValue] | None = None,
) -> dict[str, str]:
    """Link unambiguous labels to full paths and assign readable project names."""
    aliases: dict[str, set[str]] = {}
    for value in sorted(workspace_values):
        if _is_workspace_path(value):
            basename = re.split(r"[/\\]", value.rstrip("/\\"))[-1]
            if basename:
                aliases.setdefault(basename, set()).add(_workspace_identity(value))
    for record in _workspace_records(documents or []):
        values = _record_workspaces(record)
        paths = {
            _workspace_identity(value) for value in values if _is_workspace_path(value)
        }
        if len(paths) == 1:
            for value in values:
                if not _is_workspace_path(value):
                    aliases.setdefault(value, set()).update(paths)
    identities: dict[str, str] = {}
    for value in workspace_values | aliases.keys():
        targets = aliases.get(value, set())
        identities[value] = (
            next(iter(targets))
            if not _is_workspace_path(value) and len(targets) == 1
            else _workspace_identity(value)
        )
    labels = {
        identity: _project_label(index)
        for index, identity in enumerate(sorted(set(identities.values())))
    }
    for original, replacement in extra_replacements:
        if original in identities:
            if len(aliases.get(original, set())) > 1:
                raise ValueError("workspace replacement matches an ambiguous label")
            labels[identities[original]] = replacement
    replacements: dict[str, str] = {}
    for value, identity in sorted(identities.items()):
        label = labels[identity]
        path_replacement = f"/{label.lstrip('/')}" if value.startswith("/") else label
        replacements[value] = path_replacement
        if "\\" in value:
            replacements.setdefault(value.replace("\\", "/"), path_replacement)
    return replacements


def _record_replacements(
    record: dict[str, JsonValue], replacements: dict[str, str]
) -> dict[str, str]:
    """Use a record's path to disambiguate project labels shared by other paths."""
    values = _record_workspaces(record)
    paths = sorted(value for value in values if _is_workspace_path(value))
    if len({_workspace_identity(value) for value in paths}) != 1:
        return replacements
    label = replacements[paths[0]].lstrip("/")
    return replacements | {
        value: label for value in values if not _is_workspace_path(value)
    }


def _parse_replacement(value: str) -> tuple[str, str]:
    """Parse one command-line OLD=NEW replacement."""
    old, separator, new = value.partition("=")
    if not separator or not old:
        raise argparse.ArgumentTypeError(
            "replacement must be OLD=NEW with a nonempty OLD"
        )
    return old, new


def _collect_json_files(input_path: Path) -> tuple[list[Path], bool]:
    """Return JSON files to process and whether the input is a directory."""
    if input_path.is_file():
        if input_path.suffix.casefold() != ".json":
            raise ValueError(f"input file must have a .json extension: {input_path}")
        return [input_path], False
    if not input_path.is_dir():
        raise ValueError(
            f"input path does not exist or is not a file or directory: {input_path}"
        )
    files = sorted(
        path
        for path in input_path.rglob("*")
        if path.is_file() and path.suffix.casefold() == ".json"
    )
    if not files:
        raise ValueError(f"no JSON files found under input directory: {input_path}")
    return files, True


def _load_json(path: Path) -> JsonValue:
    """Read one JSON fixture."""
    return cast(JsonValue, json.loads(path.read_text(encoding="utf-8")))


def _local_path_substitute(match: re.Match[str]) -> str:
    """Replace a local filesystem path and retain punctuation after it."""
    path = match.group(0)
    trimmed = path.rstrip(".,;:!?)]}")
    return f"<local-path>{path[len(trimmed) :]}"


def _sanitize_text(
    value: str,
    session_ids: dict[str, str],
    workspace_replacements: dict[str, str],
    extra_replacements: list[tuple[str, str]],
) -> str:
    """Replace known identifiers and redact local filesystem paths."""
    value = _apply_extra_replacements(value, extra_replacements)
    for original in sorted(session_ids, key=len, reverse=True):
        pseudonym = session_ids[original]
        value = value.replace(original, pseudonym)
    originals = sorted(
        sorted(filter(None, workspace_replacements)), key=len, reverse=True
    )
    if originals:
        alternatives = "|".join(re.escape(original) for original in originals)
        pattern = re.compile(rf"(?<![\w.-])(?:{alternatives})(?![\w-])", re.IGNORECASE)
        folded_replacements: dict[str, str] = {}
        for original in originals:
            folded_replacements.setdefault(
                original.casefold(), workspace_replacements[original]
            )

        def substitute_workspace(match: re.Match[str]) -> str:
            """Replace original names once, preserving generated project names."""
            matched = match.group(0)
            return workspace_replacements.get(
                matched, folded_replacements[matched.casefold()]
            )

        value = pattern.sub(substitute_workspace, value)
    protected_paths = {
        replacement
        for replacement in workspace_replacements.values()
        if replacement.startswith("/")
    }

    def redact_path(match: re.Match[str]) -> str:
        """Keep generated project paths while redacting other filesystem paths."""
        if match.group(0).rstrip(".,;:!?)]}") in protected_paths:
            return match.group(0)
        return _local_path_substitute(match)

    return LOCAL_PATH.sub(redact_path, value)


def _apply_extra_replacements(
    value: str, extra_replacements: list[tuple[str, str]]
) -> str:
    """Apply user-specified literal replacements in their command-line order."""
    for original, replacement in extra_replacements:
        if original.startswith("/") and replacement and not replacement.startswith("/"):
            replacement = f"/{replacement}"
        value = value.replace(original, replacement)
    return value


def _sanitize_value(
    value: JsonValue,
    key: str | None,
    session_ids: dict[str, str],
    workspace_replacements: dict[str, str],
    extra_replacements: list[tuple[str, str]],
) -> JsonValue:
    """Sanitize strings recursively while preserving JSON structure and values."""
    if isinstance(value, dict):
        record_replacements = _record_replacements(value, workspace_replacements)
        for child_key, child in value.items():
            value[child_key] = _sanitize_value(
                child,
                child_key,
                session_ids,
                (
                    record_replacements
                    if _normalized_field_name(child_key) in WORKSPACE_FIELDS
                    else workspace_replacements
                ),
                extra_replacements,
            )
        return value
    if isinstance(value, list):
        for index, child in enumerate(value):
            value[index] = _sanitize_value(
                child, key, session_ids, workspace_replacements, extra_replacements
            )
        return value
    if isinstance(value, str):
        normalized_key = _normalized_field_name(key) if key is not None else ""
        if normalized_key in SUMMARY_FIELDS:
            return "<redacted>" if value else value
        if normalized_key in SESSION_FIELDS:
            if not value:
                return value
            replaced_value = _apply_extra_replacements(value, extra_replacements)
            if replaced_value != value:
                return _sanitize_text(replaced_value, {}, workspace_replacements, [])
            return session_ids.get(value, _pseudonymize_session_id(value))
        if normalized_key in WORKSPACE_FIELDS and value.strip():
            return workspace_replacements[value.strip()]
        if normalized_key in PUBLIC_FIELDS:
            return _apply_extra_replacements(value, extra_replacements)
        return _sanitize_text(
            value, session_ids, workspace_replacements, extra_replacements
        )
    return value


def _output_path(
    source: Path,
    input_root: Path,
    is_directory: bool,
    output_directory: Path,
) -> Path:
    """Choose an output path while retaining directory-relative names."""
    if is_directory:
        return output_directory / source.relative_to(input_root)
    return output_directory / source.name


def run(
    input_path: Path,
    output_directory: Path,
    extra_replacements: list[tuple[str, str]],
) -> int:
    """Sanitize one JSON file or every JSON file under a directory."""
    resolved_input = input_path.expanduser().resolve()
    resolved_output = output_directory.expanduser().resolve()
    files, is_directory = _collect_json_files(resolved_input)
    if is_directory and resolved_output.is_relative_to(resolved_input):
        raise ValueError("output directory must be outside the input directory")

    documents = [_load_json(path) for path in files]
    original_session_ids, workspace_values = _collect_sensitive_values(documents)
    session_ids = {
        value: _pseudonymize_session_id(value) for value in sorted(original_session_ids)
    }
    workspace_replacements = _workspace_replacements(
        workspace_values, extra_replacements, documents=documents
    )
    output_paths = [
        _output_path(path, resolved_input, is_directory, resolved_output)
        for path in files
    ]
    if any(
        output_path.resolve() == source
        for output_path, source in zip(output_paths, files, strict=True)
    ):
        raise ValueError("output path would overwrite an input fixture")
    for destination in output_paths:
        if not destination.resolve().is_relative_to(resolved_output):
            raise ValueError("output path escapes the output directory")
        if destination.exists() or destination.is_symlink():
            raise ValueError(f"output fixture already exists: {destination}")

    for source, document, destination in zip(
        files, documents, output_paths, strict=True
    ):
        sanitized = _sanitize_value(
            document,
            None,
            session_ids,
            workspace_replacements,
            extra_replacements,
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("x", encoding="utf-8") as output:
            output.write(json.dumps(sanitized, indent=2, ensure_ascii=False) + "\n")
        print(f"{source} -> {destination}")
    print(
        f"sanitized {len(files)} fixture(s); "
        f"session IDs pseudonymized: {len(session_ids)}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse command-line arguments and run fixture sanitization."""
    parser = argparse.ArgumentParser(description="Sanitize tokscale JSON fixtures.")
    parser.add_argument(
        "input",
        type=Path,
        help="one JSON fixture file or a directory searched recursively for JSON files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="write sanitized files here (default: ./output; inputs are never changed)",
    )
    parser.add_argument(
        "--replace",
        action="append",
        default=[],
        type=_parse_replacement,
        metavar="OLD=NEW",
        help="replace an additional sensitive string everywhere; may be repeated",
    )
    arguments = parser.parse_args(argv)
    try:
        return run(arguments.input, arguments.output_dir, arguments.replace)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"sanitize_fixtures.py: error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
