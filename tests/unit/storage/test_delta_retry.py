import pytest

from job_plat.storage.delta_retry import run_with_delta_retry


class ConcurrentWriteException(RuntimeError):
    pass


def test_retries_recognized_delta_conflict():
    calls = 0

    def operation():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConcurrentWriteException("conflicting Delta commit")
        return "committed"

    result = run_with_delta_retry(
        operation,
        max_attempts=2,
        initial_delay_seconds=0,
    )

    assert result == "committed"
    assert calls == 2


def test_does_not_retry_non_conflict():
    calls = 0

    def operation():
        nonlocal calls
        calls += 1
        raise ValueError("invalid data")

    with pytest.raises(ValueError, match="invalid data"):
        run_with_delta_retry(
            operation,
            max_attempts=3,
            initial_delay_seconds=0,
        )

    assert calls == 1
