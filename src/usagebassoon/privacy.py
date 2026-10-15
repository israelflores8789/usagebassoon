# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""privacy.py — Output-only credential redaction and sharing obfuscation."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath, PureWindowsPath
from urllib.parse import quote, quote_plus

import pyarrow as pa

_PSEUDONYM_PREFIXES: Mapping[str, str] = {
    "host": "host",
    "hostname": "host",
    "issue_key": "session",
    "machine_id": "host",
    "session_id": "session",
    "session_label": "session",
    "tag": "tag",
    "workspace": "workspace",
    "workspace_label": "workspace",
}
_REDACTED_COLUMNS = frozenset({"note"})
_CREDENTIAL_MARKERS = (
    "TOKEN",
    "PASSWORD",
    "PASSWD",
    "KEY",
    "SECRET",
    "AUTH",
    "CREDENTIAL",
)
_URI_CREDENTIAL_PATTERN = re.compile(r"(\w+://)[^\s/@]+@")
_QUERY_SECRET_PATTERN = re.compile(
    r"[?&;](?P<name>[\w-]+)=[^\s&#;\"']*",
    re.IGNORECASE,
)
_ENV_ASSIGNMENT_PATTERN = re.compile(
    r"(?P<prefix>[\"']?\b(?P<name>(?=[\w-]*(?:"
    + "|".join(_CREDENTIAL_MARKERS)
    + r"))[A-Za-z_][A-Za-z0-9_-]*)[\"']?\s*[=:]\s*)"
    r"(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\s,;{}\[\]\"']+)",
    re.IGNORECASE,
)
_AUTHORIZATION_PATTERN = re.compile(
    r"\b(?:Bearer|Basic)\s+[^\s\"',;}\]]+", re.IGNORECASE
)
_WINDOWS_PATH_PATTERN = re.compile(
    r"(?<![\w:])(?:"
    r"(?P<quote>['\"])(?:[A-Za-z]:[\\/]|\\\\[^\\/\r\n]+[\\/])"
    r"[^\r\n]*?(?P=quote)"
    r"|"
    r"(?:[A-Za-z]:[\\/]|\\\\[^\\/\s]+[\\/])"
    r"(?:[^\\/:;,)\"'<>|?*\[\]\r\n]+[\\/])*"
    r"[^\\/:;,)\"'<>|?*\[\]\r\n]*?\.[A-Za-z0-9]{1,16}"
    r"(?=(?:\[\d+\])?[:;,)'\"\s]|$)"
    r"|"
    r"(?:[A-Za-z]:[\\/]|\\\\[^\\/\s]+[\\/][^\s,:;)]+)[^\s,:;)]*"
    r")"
)
_POSIX_PATH_PATTERN = re.compile(r"(?<!\w)(?:~|/)[^\s,:;)]*")
_DOCTOR_POSIX_PATH_PATTERN = re.compile(
    r"(?<![\w/:])(?:(?P<quote>['\"])(?:~|/[^'\"\r\n]*)(?P=quote)|(?:~|/)[^\s,:;)]*)"
)
_GCP_SERVICE_ACCOUNT_PATTERN = re.compile(
    r"(?P<account>[A-Za-z0-9._+-]+)@[A-Za-z0-9-]+(?P<domain>\.iam\.gserviceaccount\.com)\b",
    re.IGNORECASE,
)
_DOCTOR_EMAIL_PATTERN = re.compile(
    r"(?P<account>[A-Za-z0-9._%+-]+)@"
    r"(?P<domain>(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z]{2,63})"
    r"(?![A-Za-z0-9.-])"
)
_PUBLIC_EMAIL_DOMAINS = frozenset(
    {
        "aol.com",
        "gmail.com",
        "googlemail.com",
        "hotmail.com",
        "icloud.com",
        "live.com",
        "mac.com",
        "me.com",
        "msn.com",
        "outlook.com",
        "proton.me",
        "protonmail.com",
        "yahoo.com",
    }
)
_DOCTOR_AUTH_METHODS = frozenset(
    {
        "ADC",
        "ADC from a credentials file",
        "configured service-account credentials file",
        "explicit credentials",
        "explicit token",
        "injected client",
        "local file permissions",
        "MOTHERDUCK_TOKEN (environment)",
        "MOTHERDUCK_TOKEN (not set)",
        "unavailable",
    }
)
_APPLICATION_PATH_PREFIXES = (
    ("appdata", "local"),
    ("appdata", "locallow"),
    ("appdata", "roaming"),
    (".config",),
    (".local", "share"),
    ("library", "application support"),
)
_USER_DIRECTORY_PARENTS = frozenset({"home", "users", "documents and settings"})
_PUBLIC_USER_DIRECTORIES = frozenset({"all users", "default", "default user", "public"})
_PROJECT_PARENT_DIRECTORIES = frozenset(
    {
        "code",
        "dev",
        "documents",
        "project",
        "projects",
        "repo",
        "repos",
        "src",
        "work",
        "workspace",
        "workspaces",
    }
)
_COMMON_PATH_DIRECTORIES = frozenset(
    {
        ".cache",
        ".config",
        ".local",
        "appdata",
        "application support",
        "applications",
        "bin",
        "desktop",
        "dev",
        "documents",
        "downloads",
        "etc",
        "home",
        "library",
        "lib",
        "lib64",
        "local",
        "media",
        "mnt",
        "opt",
        "program files",
        "program files (x86)",
        "programdata",
        "public",
        "run",
        "sbin",
        "share",
        "src",
        "system32",
        "temp",
        "tmp",
        "usr",
        "users",
        "var",
        "volumes",
        "windows",
        "workspace",
        "workspaces",
    }
)


def _label(prefix: str, index: int) -> str:
    """Return a readable deterministic label for an output-local index.

    Args:
        prefix: Semantic type of the identifier.
        index: One-based value index within its identifier type.

    Returns:
        A share-safe label such as ``session-alpha``.
    """
    alphabet = "alpha bravo charlie delta echo foxtrot golf hotel india juliet"
    words = alphabet.split()
    if index <= len(words):
        return f"{prefix}-{words[index - 1]}"
    return f"{prefix}-{index}"


def _sanitize_text(value: str, replacements: Mapping[str, str]) -> str:
    """Redact credentials and paths embedded in an arbitrary string.

    Args:
        value: Potentially sensitive free text.
        replacements: Raw known host or machine values mapped to pseudonyms.

    Returns:
        Share-safe free text.
    """
    sanitized = redact_credentials(value)
    for private_value in sorted(replacements, key=len, reverse=True):
        if private_value:
            sanitized = sanitized.replace(private_value, replacements[private_value])
    sanitized = _WINDOWS_PATH_PATTERN.sub("<path>", sanitized)
    return _POSIX_PATH_PATTERN.sub("<path>", sanitized)


def _redact_environment_secret(match: re.Match[str]) -> str:
    """Redact one environment-style assignment when its name suggests a secret.

    Args:
        match: Parsed environment-style assignment.

    Returns:
        Original assignment or a redacted value.
    """
    name = match["name"]
    value = match[0][len(match["prefix"]) :].strip().strip("\"'")
    if name.casefold() == "auth":
        line_end = match.string.find("\n", match.end())
        suffix_end = len(match.string) if line_end < 0 else line_end
        suffix = match.string[match.end() : suffix_end]
        complete_value = f"{value}{suffix}".strip().strip("\"'")
        if complete_value in _DOCTOR_AUTH_METHODS:
            return match[0]
    if _credential_name(name):
        return f"{match['prefix']}<redacted>"
    return match[0]


def _credential_name(name: str) -> bool:
    """Return whether a field or environment name denotes authentication material."""
    return any(token in name.upper() for token in _CREDENTIAL_MARKERS)


def _redact_query_secret(match: re.Match[str]) -> str:
    """Redact a credential query parameter while preserving ordinary parameters."""
    return "<redacted-query>" if _credential_name(match["name"]) else match[0]


def credential_values(
    environ: Mapping[str, str], *, environment_names: Iterable[str] = ()
) -> tuple[str, ...]:
    """Select active credentials, including explicitly supplied child variables.

    Args:
        environ: Active process environment.
        environment_names: Additional potentially sensitive variable names.

    Returns:
        Nonempty values requiring exact redaction even when echoed without labels.
    """
    names = set(environment_names)
    return tuple(
        value
        for name, value in environ.items()
        if value and (name in names or _credential_name(name))
    )


def redact_credentials(value: str, *, known_values: Iterable[str] = ()) -> str:
    """Redact authentication material while preserving diagnostic locations.

    Args:
        value: Rendered text, including exception chains or child diagnostics.
        known_values: Active credentials that may appear without a field label.

    Returns:
        Credential-safe text with paths and ordinary diagnostic context retained.
    """
    variants: set[str] = set()
    for secret in known_values:
        if secret:
            variants.update((secret, quote(secret, safe=""), quote_plus(secret)))
            variants.add(json.dumps(secret)[1:-1])
            variants.add(repr(secret)[1:-1])
    sanitized = value
    for secret in sorted(variants, key=len, reverse=True):
        sanitized = sanitized.replace(secret, "<redacted>")
    sanitized = _URI_CREDENTIAL_PATTERN.sub(r"\1<credentials>@", sanitized)
    sanitized = _QUERY_SECRET_PATTERN.sub(_redact_query_secret, sanitized)
    sanitized = _AUTHORIZATION_PATTERN.sub("<redacted-authorization>", sanitized)
    return _ENV_ASSIGNMENT_PATTERN.sub(_redact_environment_secret, sanitized)


def _sanitize_value(value: object, replacements: Mapping[str, str]) -> object:
    """Sanitize arbitrary nested text while preserving its container shape.

    Args:
        value: Value from a shareable Arrow record.
        replacements: Raw known host or machine values mapped to pseudonyms.

    Returns:
        A value with embedded sensitive text removed.
    """
    if isinstance(value, str):
        return _sanitize_text(value, replacements)
    if isinstance(value, list):
        return [_sanitize_value(item, replacements) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_value(item, replacements) for item in value)
    if isinstance(value, dict):
        return {key: _sanitize_value(item, replacements) for key, item in value.items()}
    return value


def sanitize_table(table: pa.Table) -> pa.Table:
    """Obfuscate known personal columns without changing data relationships.

    Values retain a stable pseudonym within this one result table. Diagnostic
    columns remain intact except ``issue_key`` values that carry a session id
    for a models/report reconciliation issue.

    Args:
        table: Raw Arrow result table from a known UsageBassoon relation.

    Returns:
        A table with share-safe identifiers, redacted notes, and sanitized
        embedded credentials or filesystem paths.
    """
    records = table.to_pylist()
    mappings: dict[str, dict[str, str]] = {}
    for record in records:
        check_name = record.get("check_name")
        for column, prefix in _PSEUDONYM_PREFIXES.items():
            value = record.get(column)
            if value is None or not isinstance(value, str):
                continue
            if column == "issue_key" and check_name not in {
                "models_report_session_set",
                "models_report_metric",
                "models_report_cost",
            }:
                continue
            if column == "issue_key" and "/" in value:
                client, session_id = value.split("/", maxsplit=1)
                mapping = mappings.setdefault(prefix, {})
                label = mapping.setdefault(
                    session_id,
                    _label(prefix, len(mapping) + 1),
                )
                record[column] = f"{client}/{label}"
                continue
            mapping = mappings.setdefault(prefix, {})
            record[column] = mapping.setdefault(value, _label(prefix, len(mapping) + 1))
    host_replacements = mappings.get("host", {})
    for record in records:
        for column, value in record.items():
            if column in _REDACTED_COLUMNS and value is not None:
                record[column] = "[redacted]"
            else:
                record[column] = _sanitize_value(value, host_replacements)
    return pa.Table.from_pylist(records, schema=table.schema)


def _sanitize_doctor_path(value: str, *, preserve_filename: bool = True) -> str:
    """Preserve path structure while masking recognizable user and project names.

    Args:
        value: A Windows, POSIX, or relative filesystem path.
        preserve_filename: Whether to keep the final path component as a filename.

    Returns:
        A readable path with known user and project directories replaced.
    """
    if value.startswith("//storage.googleapis.com/"):
        return value
    windows_path = PureWindowsPath(value)
    path = windows_path if windows_path.drive or "\\" in value else PurePosixPath(value)
    parts = list(path.parts[1:] if path.anchor else path.parts)
    try:
        home_name = Path.home().name.casefold()
    except (OSError, RuntimeError):
        home_name = ""
    try:
        project_name = Path.cwd().name.casefold()
    except OSError:
        project_name = ""

    sanitized_parts: list[str] = []
    for index, part in enumerate(parts):
        normalized = part.casefold()
        previous = parts[index - 1].casefold() if index else ""
        is_last = index == len(parts) - 1
        replacement: str | None = None
        ancestors = tuple(component.casefold() for component in parts[:index])
        in_application_data = any(
            ancestors[start : start + len(prefix)] == prefix
            for prefix in _APPLICATION_PATH_PREFIXES
            for start in range(len(ancestors))
        )

        is_user_directory = bool(home_name and normalized == home_name) or (
            previous in _USER_DIRECTORY_PARENTS
            and normalized not in _PUBLIC_USER_DIRECTORIES
        )
        is_project_directory = bool(
            project_name
            and normalized == project_name
            and normalized not in _COMMON_PATH_DIRECTORIES
            and not in_application_data
        ) or (
            previous in _PROJECT_PARENT_DIRECTORIES
            and normalized not in _COMMON_PATH_DIRECTORIES
            and (not is_last or not preserve_filename)
        )

        if is_user_directory:
            replacement = "<user>"
        elif is_project_directory:
            replacement = "<project>"

        sanitized_parts.append(replacement or part)

    if isinstance(path, PureWindowsPath) and path.drive.startswith("\\\\"):
        anchor = "\\\\<host>\\<share>\\"
        return anchor + "\\".join(sanitized_parts)
    return str(type(path)(path.anchor, *sanitized_parts))


def _sanitize_doctor_path_match(match: re.Match[str]) -> str:
    """Sanitize one path match while retaining any surrounding quotes."""
    matched = match[0]
    if len(matched) >= 2 and matched[0] in "'\"" and matched[-1] == matched[0]:
        quote = matched[0]
        path = matched[1:-1]
        preserve_filename = not path.endswith(("/", "\\"))
        sanitized_path = _sanitize_doctor_path(
            path,
            preserve_filename=preserve_filename,
        )
        return f"{quote}{sanitized_path}{quote}"
    return _sanitize_doctor_path(
        matched,
        preserve_filename=not matched.endswith(("/", "\\")),
    )


def sanitize_doctor_text(
    value: str,
    *,
    config_path: str | None,
    database: str | None,
) -> str:
    """Keep path context while masking users and projects and redacting secrets.

    Args:
        value: One doctor display string.
        config_path: Resolved configuration file path, when available.
        database: Configured database path or public database/dataset name.

    Returns:
        A display-safe variant of ``value``.
    """

    def sanitize_part(part: str) -> str:
        """Redact credentials and preserve useful path components."""
        if database and any(separator in database for separator in ("/", "\\")):
            flags = re.IGNORECASE if PureWindowsPath(database).drive else 0
            part = re.sub(re.escape(database), "<path>", part, flags=flags)
        sanitized = redact_credentials(part)
        sanitized = _WINDOWS_PATH_PATTERN.sub(_sanitize_doctor_path_match, sanitized)
        sanitized = _DOCTOR_POSIX_PATH_PATTERN.sub(
            _sanitize_doctor_path_match, sanitized
        )
        sanitized = _GCP_SERVICE_ACCOUNT_PATTERN.sub(
            r"\g<account>@<project-id>\g<domain>", sanitized
        )
        return _DOCTOR_EMAIL_PATTERN.sub(_redact_doctor_email, sanitized)

    if not config_path:
        return sanitize_part(value)
    windows_config_path = PureWindowsPath(config_path).drive != ""
    flags = re.IGNORECASE if windows_config_path else 0
    config_pattern = re.compile(re.escape(config_path), flags)
    sanitized_config_path = redact_credentials(_sanitize_doctor_path(config_path))
    return sanitized_config_path.join(
        sanitize_part(part) for part in config_pattern.split(value)
    )


def google_doctor_principal(credentials: object | None) -> str:
    """Describe a Google credential principal for sanitized doctor output.

    Args:
        credentials: Resolved Google Auth credentials, when available.

    Returns:
        The effective service account, user account, external identity, or an
        unavailable marker. Email masking is applied when doctor renders output.
    """
    if credentials is None:
        return "unavailable"

    service_account_email = getattr(credentials, "service_account_email", None)
    if isinstance(service_account_email, str) and "@" in service_account_email:
        return f"service account {service_account_email}"

    account = getattr(credentials, "account", None)
    if isinstance(account, str):
        return (
            f"user account {account}"
            if "@" in account
            else "user account (email unavailable)"
        )

    module = type(credentials).__module__
    if any(
        marker in module for marker in ("external_account", "identity_pool", ".aws")
    ):
        return "external identity (email unavailable)"
    if "compute_engine" in module or "impersonated_credentials" in module:
        return "service account (email unavailable)"
    return "principal unavailable"


def _redact_doctor_email(match: re.Match[str]) -> str:
    """Keep only a short handle prefix and allow-listed public email domains."""
    account = match.group("account")
    visible_count = 2 if len(account) > 4 else 1 if len(account) > 2 else 0
    visible = f"{account[:visible_count]}..." if visible_count else "..."
    domain = match.group("domain")
    safe_domain = domain.casefold()
    if safe_domain not in _PUBLIC_EMAIL_DOMAINS:
        safe_domain = "<domain>"
    return f"{visible}@{safe_domain}"
