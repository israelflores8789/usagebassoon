# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""bigquery_compaction.py — Install the versioned nightly BigQuery Scheduled Query."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from importlib import resources
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from google.cloud import bigquery_datatransfer

from google.auth.credentials import Credentials


class BigQueryBackendCompaction(Protocol):
    """Metadata and SQL binding needed to install a dataset schedule."""

    project: str
    dataset: str
    location: str
    _credentials: Credentials | None

    def _qualify_view_sql(self, sql: str) -> str:
        """Bind the packaged maintenance SQL to this dataset."""


def install_compaction(
    backend: BigQueryBackendCompaction, *, enabled: bool | None = True
) -> str:
    """Create or update this dataset's nightly transaction during explicit init."""
    from google.cloud import bigquery_datatransfer
    from google.protobuf.field_mask_pb2 import FieldMask

    sql = (
        resources.files("usagebassoon.sql.bigquery")
        .joinpath("compaction.sql")
        .read_text()
    )
    query = backend._qualify_view_sql(sql)
    display_name = f"UsageBassoon nightly compaction: {backend.dataset}"
    parent = f"projects/{backend.project}/locations/{backend.location.lower()}"
    with bigquery_datatransfer.DataTransferServiceClient(
        credentials=backend._credentials
    ) as client:
        matches = [
            config
            for config in client.list_transfer_configs(parent=parent)
            if config.display_name == display_name
            and config.data_source_id == "scheduled_query"
        ]
        if len(matches) > 1:
            raise RuntimeError("multiple UsageBassoon compaction schedules exist")
        disabled = (
            matches[0].disabled if enabled is None and matches else enabled is False
        )
        if matches:
            config = matches[0]
            if (
                cast(Mapping[str, str], config.params).get("query") == query
                and config.schedule == "every day 02:00"
                and config.disabled == disabled
            ):
                return config.name
            config = bigquery_datatransfer.TransferConfig(
                name=config.name,
                params={"query": query},
                schedule="every day 02:00",
                disabled=disabled,
            )
            config = client.update_transfer_config(
                transfer_config=config,
                update_mask=FieldMask(paths=["params", "schedule", "disabled"]),
            )
        else:
            config = bigquery_datatransfer.TransferConfig(
                display_name=display_name,
                data_source_id="scheduled_query",
                params={"query": query},
                schedule="every day 02:00",
                disabled=disabled,
            )
            email = getattr(backend._credentials, "service_account_email", "")
            request = bigquery_datatransfer.CreateTransferConfigRequest(
                parent=parent,
                transfer_config=config,
                service_account_name=email if isinstance(email, str) else "",
            )
            config = client.create_transfer_config(request=request)
        return config.name


def _matching_schedules(
    client: bigquery_datatransfer.DataTransferServiceClient,
    parent: str,
    display_name: str,
    remaining: Callable[[], float],
) -> list[bigquery_datatransfer.TransferConfig]:
    """Fetch each page with a fresh remaining operation budget."""
    matches: list[bigquery_datatransfer.TransferConfig] = []
    token = ""
    while True:
        pager = client.list_transfer_configs(
            request={"parent": parent, "page_token": token},
            retry=None,
            timeout=remaining(),
        )
        page = next(iter(pager.pages))
        remaining()
        matches.extend(
            config
            for config in page.transfer_configs
            if config.display_name == display_name
            and config.data_source_id == "scheduled_query"
        )
        token = page.next_page_token
        if not token:
            return matches


def pause_compaction(
    backend: BigQueryBackendCompaction, *, timeout: float = 60.0
) -> None:
    """Disable matching maintenance and await active transfer runs with a bound."""
    from google.cloud import bigquery_datatransfer
    from google.protobuf.field_mask_pb2 import FieldMask

    deadline = time.monotonic() + timeout

    def remaining() -> float:
        """Apply one operation budget to discovery, updates and polling."""
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise RuntimeError(
                "scheduled compaction shutdown deadline exceeded; "
                "inspect maintenance state and retry restore"
            )
        return budget

    parent = f"projects/{backend.project}/locations/{backend.location.lower()}"
    display_name = f"UsageBassoon nightly compaction: {backend.dataset}"
    logging.getLogger("usagebassoon").info("Disabling scheduled compaction...")
    with bigquery_datatransfer.DataTransferServiceClient(
        credentials=backend._credentials
    ) as client:
        matches = _matching_schedules(client, parent, display_name, remaining)
        if len(matches) > 1:
            raise RuntimeError(
                "multiple UsageBassoon compaction schedules exist; "
                "resolve them before restore"
            )
        if not matches:
            return
        config = matches[0]
        if not config.disabled:
            client.update_transfer_config(
                transfer_config=bigquery_datatransfer.TransferConfig(
                    name=config.name, disabled=True
                ),
                update_mask=FieldMask(paths=["disabled"]),
                retry=None,
                timeout=remaining(),
            )
        while True:
            token = ""
            active = False
            while True:
                pager = client.list_transfer_runs(
                    request={
                        "parent": config.name,
                        "states": [
                            bigquery_datatransfer.TransferState.PENDING,
                            bigquery_datatransfer.TransferState.RUNNING,
                        ],
                        "page_token": token,
                    },
                    retry=None,
                    timeout=remaining(),
                )
                page = next(iter(pager.pages))
                remaining()
                active = bool(page.transfer_runs)
                token = page.next_page_token
                if active or not token:
                    break
            if not active:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "scheduled compaction is disabled but a run remains active; "
                    "wait and retry restore"
                )
            time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))


def compaction_status(
    backend: BigQueryBackendCompaction, *, timeout: float = 30.0
) -> tuple[bool, str]:
    """Inspect the destination's schedule without changing its enabled state."""
    from google.cloud import bigquery_datatransfer

    deadline = time.monotonic() + timeout

    def remaining() -> float:
        """Bound optional maintenance-state reporting as well as shutdown."""
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise RuntimeError("maintenance status deadline exceeded")
        return budget

    parent = f"projects/{backend.project}/locations/{backend.location.lower()}"
    display_name = f"UsageBassoon nightly compaction: {backend.dataset}"
    with bigquery_datatransfer.DataTransferServiceClient(
        credentials=backend._credentials
    ) as client:
        matches = _matching_schedules(client, parent, display_name, remaining)
        if len(matches) != 1:
            return (
                False,
                f"expected one UsageBassoon compaction schedule; found {len(matches)}",
            )
        config = matches[0]
        state = (
            "disabled; run bassoon init after recovery verification"
            if config.disabled
            else "enabled at 02:00 UTC"
        )
        return not config.disabled, f"nightly compaction {state}"
