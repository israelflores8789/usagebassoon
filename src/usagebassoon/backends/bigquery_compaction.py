# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""bigquery_compaction.py — Install the versioned nightly BigQuery Scheduled Query."""

import logging
import time
from collections.abc import Mapping
from importlib import resources
from typing import Protocol, cast

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


def pause_compaction(
    backend: BigQueryBackendCompaction, *, timeout: float = 60.0
) -> None:
    """Disable matching maintenance and await active transfer runs with a bound."""
    from google.cloud import bigquery_datatransfer
    from google.protobuf.field_mask_pb2 import FieldMask

    parent = f"projects/{backend.project}/locations/{backend.location.lower()}"
    display_name = f"UsageBassoon nightly compaction: {backend.dataset}"
    logging.getLogger("usagebassoon").info("Disabling scheduled compaction...")
    with bigquery_datatransfer.DataTransferServiceClient(
        credentials=backend._credentials
    ) as client:
        matches = [
            c
            for c in client.list_transfer_configs(parent=parent)
            if c.display_name == display_name and c.data_source_id == "scheduled_query"
        ]
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
            )
        deadline = time.monotonic() + timeout
        while True:
            active = list(
                client.list_transfer_runs(
                    request={
                        "parent": config.name,
                        "states": [
                            bigquery_datatransfer.TransferState.PENDING,
                            bigquery_datatransfer.TransferState.RUNNING,
                        ],
                    }
                )
            )
            if not active:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "scheduled compaction is disabled but a run remains active; "
                    "wait and retry restore"
                )
            time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))


def compaction_status(backend: BigQueryBackendCompaction) -> tuple[bool, str]:
    """Inspect the destination's schedule without changing its enabled state."""
    from google.cloud import bigquery_datatransfer

    parent = f"projects/{backend.project}/locations/{backend.location.lower()}"
    display_name = f"UsageBassoon nightly compaction: {backend.dataset}"
    with bigquery_datatransfer.DataTransferServiceClient(
        credentials=backend._credentials
    ) as client:
        matches = [
            c
            for c in client.list_transfer_configs(parent=parent)
            if c.display_name == display_name and c.data_source_id == "scheduled_query"
        ]
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
