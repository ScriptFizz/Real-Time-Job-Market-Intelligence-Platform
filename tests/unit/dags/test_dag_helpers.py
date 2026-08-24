from datetime import UTC, datetime

import pytest

from job_plat.dags.dag_helpers import build_cli_command


def test_build_cli_command_uses_logical_date_and_environment():
    command = build_cli_command(
        "silver",
        {
            "logical_date": datetime(2025, 3, 2, 4, 30, tzinfo=UTC),
            "params": {"env": "prod"},
        },
    )

    assert command == [
        "-m",
        "job_plat.cli",
        "silver",
        "--execution-date",
        "2025-03-02T04:30:00+00:00",
        "--env",
        "prod",
    ]


def test_build_cli_command_rejects_naive_logical_date():
    with pytest.raises(ValueError, match="timezone-aware"):
        build_cli_command(
            "silver",
            {
                "logical_date": datetime(2025, 3, 2),
                "params": {"env": "dev"},
            },
        )
