# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""collection_policy.py — Cross-domain policy overrides for collected records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class CollectionPolicy:
    """Override selected fields for records in one collection domain.

    Attributes:
        domain: Normalized collection domain, such as ``price_versions``.
        when: Field/value pairs that must all match the record.
        overrides: Fields to replace when the policy matches.
        skip_if: Field/value pairs that preserve the input record when all match.
    """

    domain: str
    when: Mapping[str, object]
    overrides: Mapping[str, object]
    skip_if: Mapping[str, object]


_COLLECTION_POLICIES = (
    CollectionPolicy(
        domain="price_versions",
        when=MappingProxyType({"model": "codex-auto-review"}),
        overrides=MappingProxyType(
            {
                "source": "UsageBassoon",
                "matched_key": None,
                "match_kind": "policy_free",
                "price_input_per_token": 0.0,
                "price_output_per_token": 0.0,
                "price_cache_read_per_token": 0.0,
                "price_cache_write_per_token": 0.0,
            }
        ),
        skip_if=MappingProxyType({"source": "Custom"}),
    ),
)


def apply_collection_policy(
    domain: str, record: Mapping[str, object]
) -> dict[str, object]:
    """Apply matching policy overrides to one normalized collection record.

    Args:
        domain: Normalized collection domain containing the record.
        record: Normalized fields to evaluate against registered policies.

    Returns:
        A copy of the record with all matching policy overrides applied.
    """
    updated = dict(record)
    for policy in _COLLECTION_POLICIES:
        if policy.domain != domain or not all(
            record.get(field) == value for field, value in policy.when.items()
        ):
            continue
        if policy.skip_if and all(
            record.get(field) == value for field, value in policy.skip_if.items()
        ):
            continue
        updated.update(policy.overrides)
    return updated
