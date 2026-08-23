from datetime import UTC, datetime, timedelta

import pytest

from job_plat.transformations.silver.validation.quality_checks import (
    check_freshness,
)


def test_freshness_accepts_recent_rows(spark):
    dataframe = spark.createDataFrame(
        [(datetime.now(UTC) - timedelta(hours=1),)],
        ["scraped_at"],
    )

    check_freshness(dataframe, max_hours=24)


def test_freshness_rejects_old_rows(spark):
    dataframe = spark.createDataFrame(
        [(datetime.now(UTC) - timedelta(hours=25),)],
        ["scraped_at"],
    )

    with pytest.raises(ValueError, match="older than 24 hours"):
        check_freshness(dataframe, max_hours=24)


def test_freshness_rejects_invalid_threshold(spark):
    dataframe = spark.createDataFrame(
        [(datetime.now(UTC),)],
        ["scraped_at"],
    )

    with pytest.raises(ValueError, match="greater than zero"):
        check_freshness(dataframe, max_hours=0)
