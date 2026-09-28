# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""compaction.py — Install the versioned nightly BigQuery Scheduled Query."""

from collections.abc import Mapping
from importlib import resources
from typing import Protocol, cast

from google.auth.credentials import Credentials


class CompactionBackend(Protocol):
    """Metadata and SQL binding needed to install a dataset schedule."""

    project: str
    dataset: str
    location: str
    _credentials: Credentials | None

    def _qualify_view_sql(self, sql: str) -> str:
        """Bind the packaged maintenance SQL to this dataset."""


def install_compaction(backend: CompactionBackend) -> str:
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
        if matches:
            config = matches[0]
            if (
                cast(Mapping[str, str], config.params).get("query") == query
                and config.schedule == "every day 02:00"
            ):
                return config.name
            config = bigquery_datatransfer.TransferConfig(
                name=config.name, params={"query": query}, schedule="every day 02:00"
            )
            config = client.update_transfer_config(
                transfer_config=config,
                update_mask=FieldMask(paths=["params", "schedule"]),
            )
        else:
            config = bigquery_datatransfer.TransferConfig(
                display_name=display_name,
                data_source_id="scheduled_query",
                params={"query": query},
                schedule="every day 02:00",
            )
            email = getattr(backend._credentials, "service_account_email", "")
            request = bigquery_datatransfer.CreateTransferConfigRequest(
                parent=parent,
                transfer_config=config,
                service_account_name=email if isinstance(email, str) else "",
            )
            config = client.create_transfer_config(request=request)
        return config.name
