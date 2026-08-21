from datetime import UTC, datetime

import pytest

from job_plat.cli import resolve_execution_date


def test_execution_date_defaults_to_current_utc_time():
    before = datetime.now(UTC)

    result = resolve_execution_date(None)

    after = datetime.now(UTC)

    assert before <= result <= after
    assert result.tzinfo is UTC


def test_execution_date_parses_zulu_time():
    result = resolve_execution_date("2026-08-21T10:30:00Z")

    assert result == datetime(2026, 8, 21, 10, 30, tzinfo=UTC)


def test_execution_date_normalizes_offset_to_utc():
    result = resolve_execution_date("2026-08-21T12:30:00+02:00")

    assert result == datetime(
        2026,
        8,
        21,
        10,
        30,
        tzinfo=UTC,
    )


def test_execution_date_rejects_naive_datetime():
    with pytest.raises(
        ValueError,
        match="must include a timezone offset",
    ):
        resolve_execution_date("2026-08-21T10:30:00")
