# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""contracts.py — Versioned ingest contracts and tolerant schema-drift detection."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Literal, cast

from usagebassoon.json_types import JsonValue

PAYLOAD_KINDS = ("models", "graph", "pricing", "report")
CONTRACT_FORMAT_VERSION = 1
JSON_TYPES = frozenset({"null", "bool", "int", "float", "str", "array", "object"})

type PayloadKind = Literal["models", "graph", "pricing", "report"]
type JsonType = Literal["null", "bool", "int", "float", "str", "array", "object"]
type Presence = tuple[frozenset[JsonType], int, int]


@dataclass(frozen=True, slots=True)
class ContractEntry:
    """One expected JSON path and its permitted types."""

    path: str
    expected_types: tuple[JsonType, ...]
    required: bool


@dataclass(frozen=True, slots=True)
class PayloadContract:
    """The pinned expected shape of one tokscale payload kind."""

    payload_kind: PayloadKind
    tokscale_version: str
    entries: tuple[ContractEntry, ...]
    format_version: int = CONTRACT_FORMAT_VERSION


@dataclass(frozen=True, slots=True)
class ContractDrift:
    """One schema deviation detected on a collection run."""

    drift_id: str
    run_id: str
    detected_at: datetime
    payload_kind: PayloadKind
    drift_kind: Literal["unknown_field", "missing_field", "type_change"]
    path: str
    detail: str
    tokscale_ver: str


@dataclass(frozen=True, slots=True)
class ContractValidation:
    """Combined drift outcome across one raw collection payload set."""

    events: tuple[ContractDrift, ...]
    fatal: bool


class ContractValidationError(ValueError):
    """Raised when raw payloads violate a required contract field."""

    def __init__(self, validation: ContractValidation) -> None:
        """Initialize the error with the fatal validation result.

        Args:
            validation: Validation result containing one or more fatal events.
        """
        self.validation = validation
        super().__init__(
            "required tokscale contract fields changed; "
            "inspect validation.events for details"
        )


def _payload_kind(value: str) -> PayloadKind:
    """Validate and narrow a payload kind.

    Args:
        value: Candidate payload kind.

    Returns:
        The validated payload kind.

    Raises:
        ValueError: If the kind is unsupported.
    """
    if value not in PAYLOAD_KINDS:
        raise ValueError(f"unsupported payload kind {value!r}")
    return cast(PayloadKind, value)


def _json_type(value: JsonValue) -> JsonType:
    """Map a decoded JSON value to a compact type label.

    Args:
        value: Decoded JSON value.

    Returns:
        JSON type label.

    Raises:
        ValueError: If the value is not representable in JSON.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    raise ValueError(f"value is not decoded JSON: {value!r}")


def _presence(value: JsonValue, prefix: str = "") -> dict[str, Presence]:
    """Collect path types and per-array-member requiredness.

    Args:
        value: Decoded JSON payload.
        prefix: Recursion prefix.

    Returns:
        Path mapping of permitted types, members present, and member count.
    """
    occurrences: dict[str, list[tuple[JsonType, bool]]] = {}

    def add(path: str, json_type: JsonType, present: bool) -> None:
        occurrences.setdefault(path, []).append((json_type, present))

    def collect_member(
        node: JsonValue,
        current: str,
        fields: dict[str, frozenset[JsonType]],
    ) -> None:
        """Collect one array member's descendant fields."""
        if isinstance(node, dict):
            for name, child in node.items():
                path = f"{current}.{name}" if current else name
                types = fields.get(path, frozenset())
                fields[path] = types | {_json_type(child)}
                collect_member(child, path, fields)
        elif isinstance(node, list):
            for child in node:
                collect_member(child, f"{current}[]", fields)

    def walk(node: JsonValue, current: str) -> None:
        if isinstance(node, dict):
            for name, child in node.items():
                path = f"{current}.{name}" if current else name
                add(path, _json_type(child), True)
                walk(child, path)
        elif isinstance(node, list):
            members: list[dict[str, frozenset[JsonType]]] = []
            for child in node:
                fields: dict[str, frozenset[JsonType]] = {}
                collect_member(child, f"{current}[]", fields)
                members.append(fields)
            paths = set().union(*(set(fields) for fields in members))
            for path in paths:
                types = frozenset().union(
                    *(fields.get(path, frozenset()) for fields in members)
                )
                add(
                    path,
                    sorted(types)[0],
                    all(path in fields for fields in members),
                )
                for extra_type in sorted(types)[1:]:
                    add(path, extra_type, all(path in fields for fields in members))

    walk(value, prefix)
    return {
        path: (
            frozenset(json_type for json_type, _ in entries),
            sum(1 for _, present in entries if present),
            len(entries),
        )
        for path, entries in occurrences.items()
    }


def build_contract(
    payload_kind: str,
    payload: JsonValue,
    tokscale_version: str,
    *,
    optional_paths: frozenset[str] = frozenset(),
) -> PayloadContract:
    """Build a versioned contract from an observed sanitized payload.

    Args:
        payload_kind: models, graph, pricing, or report.
        payload: Decoded payload used as the contract source.
        tokscale_version: Toks scale version that produced the payload.
        optional_paths: Paths explicitly exempt from requiredness.

    Returns:
        Pinned payload contract.
    """
    entries = tuple(
        ContractEntry(
            path=path,
            expected_types=tuple(sorted(types)),
            required=path not in optional_paths and present == total,
        )
        for path, (types, present, total) in sorted(_presence(payload).items())
    )
    return PayloadContract(_payload_kind(payload_kind), tokscale_version, entries)


def diff_contract(
    contract: PayloadContract,
    observed: JsonValue,
    *,
    run_id: str,
    sequence_start: int = 0,
    detected_at: datetime | None = None,
) -> ContractValidation:
    """Compare one observed raw payload with its pinned contract.

    Unknown fields are additive drift and remain non-fatal. A required field
    that is absent, missing from an array member, null when non-null is
    required, or has an unexpected type is fatal.

    Args:
        contract: Pinned contract for one payload kind.
        observed: Newly decoded payload.
        run_id: Owning collection run id.
        sequence_start: Prior event count for globally unique drift ids.
        detected_at: Shared collection detection timestamp when available.

    Returns:
        Drift events and whether a required contract violation occurred.
    """
    observed_paths = _presence(observed)
    expected = {entry.path: entry for entry in contract.entries}
    events: list[ContractDrift] = []
    fatal = False
    now = detected_at or datetime.now(UTC)

    def make_event(
        kind: Literal["unknown_field", "missing_field", "type_change"],
        path: str,
        detail: str,
    ) -> ContractDrift:
        event_id = sequence_start + len(events) + 1
        return ContractDrift(
            drift_id=f"{run_id}-d{event_id:04d}",
            run_id=run_id,
            detected_at=now,
            payload_kind=contract.payload_kind,
            drift_kind=kind,
            path=path,
            detail=detail,
            tokscale_ver=contract.tokscale_version,
        )

    for path, entry in expected.items():
        observed_entry = observed_paths.get(path)
        if observed_entry is None:
            if entry.required:
                events.append(
                    make_event(
                        "missing_field",
                        path,
                        f"required types {', '.join(entry.expected_types)}; absent",
                    )
                )
                fatal = True
            continue
        observed_types, present, total = observed_entry
        if entry.required and present < total:
            events.append(
                make_event(
                    "missing_field",
                    path,
                    f"present on {present}/{total} array members",
                )
            )
            fatal = True
            continue
        unexpected = observed_types - frozenset(entry.expected_types)
        if unexpected:
            events.append(
                make_event(
                    "type_change",
                    path,
                    f"expected {', '.join(entry.expected_types)}; "
                    f"got {', '.join(sorted(observed_types))}",
                )
            )
            if entry.required:
                fatal = True
    for path, observed_entry in observed_paths.items():
        if path not in expected:
            events.append(
                make_event(
                    "unknown_field",
                    path,
                    f"types {', '.join(sorted(observed_entry[0]))}; tolerated",
                )
            )
    return ContractValidation(tuple(events), fatal)


def load_contract(path: Path) -> PayloadContract:
    """Load a persisted contract JSON file.

    Args:
        path: Contract file path.

    Returns:
        Parsed and validated payload contract.

    Raises:
        ValueError: If the persisted format is unsupported or malformed.
    """
    decoded = cast(JsonValue, json.loads(path.read_text()))
    if not isinstance(decoded, dict):
        raise ValueError(f"contract {path} must be a JSON object")
    if decoded.get("format_version") != CONTRACT_FORMAT_VERSION:
        raise ValueError(f"unsupported contract format in {path}")
    kind = decoded.get("payload_kind")
    version = decoded.get("tokscale_version")
    entries = decoded.get("entries")
    if (
        not isinstance(kind, str)
        or not isinstance(version, str)
        or not isinstance(entries, list)
    ):
        raise ValueError(f"contract {path} is missing required metadata")
    parsed_entries: list[ContractEntry] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError(f"contract {path} contains a malformed entry")
        path_value = entry.get("path")
        types_value = entry.get("types")
        required = entry.get("required")
        if (
            not isinstance(path_value, str)
            or not isinstance(types_value, list)
            or not all(isinstance(item, str) for item in types_value)
            or not isinstance(required, bool)
        ):
            raise ValueError(f"contract {path} contains invalid entry values")
        if not types_value or any(item not in JSON_TYPES for item in types_value):
            raise ValueError(f"contract {path} contains unsupported JSON types")
        parsed_entries.append(
            ContractEntry(
                path_value,
                tuple(cast(JsonType, item) for item in types_value),
                required,
            )
        )
    return PayloadContract(_payload_kind(kind), version, tuple(parsed_entries))


def save_contract(contract: PayloadContract, path: Path) -> Path:
    """Write one portable contract JSON file.

    Args:
        contract: Contract to persist.
        path: Target JSON path.

    Returns:
        Written path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "format_version": contract.format_version,
                "payload_kind": contract.payload_kind,
                "tokscale_version": contract.tokscale_version,
                "entries": [
                    {
                        "path": entry.path,
                        "types": list(entry.expected_types),
                        "required": entry.required,
                    }
                    for entry in contract.entries
                ],
            },
            indent=2,
        )
        + "\n"
    )
    return path


def load_shipped_contracts() -> dict[PayloadKind, PayloadContract]:
    """Load every versioned contract distributed with usagebassoon.

    Returns:
        Contracts keyed by payload kind.

    Raises:
        ValueError: If a required contract is missing or has mismatched metadata.
    """
    root = resources.files("usagebassoon").joinpath("contracts")
    contracts: dict[PayloadKind, PayloadContract] = {}
    for kind in PAYLOAD_KINDS:
        resource = root.joinpath(f"{kind}.json")
        if not resource.is_file():
            raise ValueError(f"missing shipped contract for {kind}")
        with resources.as_file(resource) as path:
            contract = load_contract(path)
        if contract.payload_kind != kind:
            raise ValueError(f"contract metadata does not match file {kind}.json")
        contracts[contract.payload_kind] = contract
    return contracts


def validate_payloads(
    payloads: Mapping[str, Sequence[JsonValue]],
    *,
    run_id: str,
    contracts: Mapping[PayloadKind, PayloadContract] | None = None,
    detected_at: datetime | None = None,
) -> ContractValidation:
    """Validate every raw collection payload before parser invocation.

    Args:
        payloads: One or more raw payloads per supported kind. Pricing may
            contain one payload per resolved model.
        run_id: Owning collection run id.
        contracts: Explicit contracts for tests or custom deployments.
        detected_at: Shared drift timestamp when available.

    Returns:
        Aggregated drift events and fatal status.

    Raises:
        ValueError: If a supported payload is absent or an unknown kind appears.
    """
    expected = contracts if contracts is not None else load_shipped_contracts()
    unknown = set(payloads) - set(PAYLOAD_KINDS)
    if unknown:
        raise ValueError(f"unknown payload kinds: {sorted(unknown)}")
    events: list[ContractDrift] = []
    fatal = False
    for kind in PAYLOAD_KINDS:
        observations = payloads.get(kind)
        if observations is None or not observations:
            raise ValueError(f"missing raw payload for {kind}")
        contract = expected[_payload_kind(kind)]
        for observed in observations:
            result = diff_contract(
                contract,
                observed,
                run_id=run_id,
                sequence_start=len(events),
                detected_at=detected_at,
            )
            events.extend(result.events)
            fatal = fatal or result.fatal
    return ContractValidation(tuple(events), fatal)
