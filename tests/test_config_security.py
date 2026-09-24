# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_config_security.py — Security-related configuration validation tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from usagebassoon.config import ConfigurationError, ConfigurationManager

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def test_security_configuration_is_loaded_with_safe_defaults(tmp_path: Path) -> None:
    """Load explicit tokscale environment and BigQuery cost controls."""
    path = tmp_path / "config.toml"
    path.write_text(
        f'source_id = "{SOURCE_ID}"\n'
        'backend = "bigquery"\n'
        "\n[tokscale]\n"
        'env = ["TOKSCALE_CUSTOM_AUTH"]\n'
        'timeout = "120s"\n'
        "max_stdout_bytes = 1024\n"
        "max_stderr_bytes = 512\n"
        "\n[schedule]\n"
        'interval = "30m"\n'
        "\n[bigquery]\n"
        'project = "usagebassoon-test"\n'
        'dataset = "usagebassoon_it"\n'
        'location = "us-central1"\n'
        "maximum_bytes_billed = 1048576\n"
    )

    configuration = ConfigurationManager(path).load()

    assert configuration.tokscale_env == ("TOKSCALE_CUSTOM_AUTH",)
    assert configuration.tokscale_timeout_seconds == 120.0
    assert configuration.tokscale_max_stdout_bytes == 1024
    assert configuration.tokscale_max_stderr_bytes == 512
    assert configuration.schedule.interval == "30m"
    assert configuration.bigquery is not None
    assert configuration.bigquery.maximum_bytes_billed == 1_048_576
    assert configuration.bigquery.timeout_seconds == 120.0


@pytest.mark.parametrize(
    "table, message",
    [
        ('[tokscale]\nenv = ["BAD-NAME"]\n', "invalid variable name"),
        (
            '[bigquery]\nproject = "usagebassoon-test"\n'
            'dataset = "usagebassoon_it"\nlocation = "US`"\n',
            "location",
        ),
    ],
)
def test_security_configuration_rejects_unsafe_values(
    tmp_path: Path,
    table: str,
    message: str,
) -> None:
    """Reject malformed environment names and SQL-interpolated locations."""
    path = tmp_path / "config.toml"
    valid_bigquery_settings = (
        ""
        if "[bigquery]" in table
        else '\n[bigquery]\nproject = "usagebassoon-test"\n'
        'dataset = "usagebassoon_it"\n'
    )
    path.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "bigquery"\n'
        f"{valid_bigquery_settings}\n{table}"
    )

    with pytest.raises(ConfigurationError, match=message):
        ConfigurationManager(path).load()
