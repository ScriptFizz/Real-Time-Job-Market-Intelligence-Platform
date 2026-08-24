from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from job_plat.context.contexts import BronzeContext
from job_plat.pipeline.stages.data.bronze_stage import BronzeStage


def test_bronze_stage_rejects_missing_country():
    context = BronzeContext(
        execution_date=datetime.now(UTC),
        root_path="/tmp/job-platform",
        query="data engineer",
        country=None,
        location="Rome",
    )

    stage = BronzeStage(
        bronze_ctx=context,
        storage=MagicMock(),
        connector=MagicMock(),
    )

    with pytest.raises(
        ValueError,
        match="Country must not be empty",
    ):
        stage.validate_config()


def test_bronze_stage_builds_run_from_validate_config():
    execution_date = datetime(2025, 3, 2, tzinfo=UTC)

    context = BronzeContext(
        execution_date=execution_date,
        root_path="/tmp/job-platform",
        query="data engineer",
        country="it",
        location="Rome",
    )

    connector = MagicMock()
    connector.name = "adzuna"

    stage = BronzeStage(
        bronze_ctx=context,
        storage=MagicMock(),
        connector=connector,
    )

    run = stage.create_context()

    assert run.query == "data engineer"
    assert run.country == "it"
    assert run.location == "Rome"
    assert run.source == "adzuna"
    assert run.execution_date == execution_date


def test_bronze_stage_requires_execution_date():
    context = BronzeContext(
        execution_date=None,
        root_path="/tmp/job-platform",
        query="data engineer",
        country="it",
        location="Rome",
    )

    connector = MagicMock()
    connector.name = "adzuna"

    stage = BronzeStage(
        bronze_ctx=context,
        storage=MagicMock(),
        connector=connector,
    )

    with pytest.raises(
        ValueError,
        match="requires execution_date",
    ):
        stage.create_context()
