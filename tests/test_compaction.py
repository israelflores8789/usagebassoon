# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_compaction.py — Nightly schedule provisioning and update behavior."""

from typing import cast
from unittest.mock import MagicMock

import pytest
from google.auth.crypt import Signer
from google.cloud import bigquery, bigquery_datatransfer
from google.oauth2.service_account import Credentials

from usagebassoon.backends.bigquery import BigQueryBackend
from usagebassoon.compaction import install_compaction


def test_nightly_schedule_create_reuse_and_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the service account, reuse matching schedules, and update changed SQL."""
    credentials = Credentials(
        signer=cast(Signer, MagicMock(spec=Signer)),
        service_account_email="collector@example.iam.gserviceaccount.com",
        token_uri="https://oauth2.googleapis.com/token",
    )
    backend = BigQueryBackend(
        "usagebassoon-test",
        "usagebassoon_it",
        credentials=credentials,
        client=cast(bigquery.Client, MagicMock(spec=bigquery.Client)),
    )
    client = MagicMock(spec=bigquery_datatransfer.DataTransferServiceClient)
    client.__enter__.return_value = client
    empty_configs: list[bigquery_datatransfer.TransferConfig] = []
    client.list_transfer_configs.return_value = empty_configs
    client.create_transfer_config.return_value = bigquery_datatransfer.TransferConfig(
        name="projects/1/locations/us/transferConfigs/1"
    )
    factory = MagicMock(return_value=client)
    monkeypatch.setattr(bigquery_datatransfer, "DataTransferServiceClient", factory)
    name = install_compaction(backend)
    factory.assert_called_with(credentials=credentials)
    request = client.create_transfer_config.call_args.kwargs["request"]
    assert request.parent == "projects/usagebassoon-test/locations/us"
    assert request.service_account_name == credentials.service_account_email
    assert request.transfer_config.schedule == "every day 02:00"
    assert "BEGIN TRANSACTION" in request.transfer_config.params["query"]
    assert (
        "`usagebassoon-test.usagebassoon_it.raw_daily_stats`"
        in (request.transfer_config.params["query"])
    )
    existing = bigquery_datatransfer.TransferConfig(request.transfer_config)
    existing.name = name
    client.list_transfer_configs.return_value = [existing]
    assert install_compaction(backend) == name
    client.create_transfer_config.assert_called_once()
    client.update_transfer_config.assert_not_called()
    existing = bigquery_datatransfer.TransferConfig(
        name=name,
        display_name=existing.display_name,
        data_source_id="scheduled_query",
        params={"query": "old SQL"},
    )
    client.list_transfer_configs.return_value = [existing]
    client.update_transfer_config.return_value = existing
    assert install_compaction(backend) == name
    update = client.update_transfer_config.call_args.kwargs
    assert list(update["update_mask"].paths) == ["params", "schedule"]
    assert (
        update["transfer_config"].params["query"]
        == request.transfer_config.params["query"]
    )
    client.list_transfer_configs.return_value = [existing, existing]
    with pytest.raises(RuntimeError, match="multiple UsageBassoon"):
        install_compaction(backend)
