import time
from collections.abc import Callable
from typing import TypeVar

ResultT = TypeVar("ResultT")

DELTA_CONFLICT_MARKERS = (
    "ConcurrentAppendException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentDeleteReadException",
    "ConcurrentTransactionException",
    "ConcurrentWriteException",
    "MetadataChangedException",
    "ProtocolChangedException",
)


def is_delta_conflict(error: BaseException) -> bool:
    current: BaseException | None = error
    visited: set[int] = set()

    while current is not None and id(current) not in visited:
        visited.add(id(current))
        details = f"{type(current).__name__}: {current}"
        if any(marker in details for marker in DELTA_CONFLICT_MARKERS):
            return True

        current = current.__cause__ or current.__context__

    return False


def run_with_delta_retry(
    operation: Callable[[], ResultT],
    *,
    max_attempts: int = 3,
    initial_delay_seconds: float = 0.05,
) -> ResultT:
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least one")

    for attempt_number in range(1, max_attempts + 1):
        try:
            return operation()
        except Exception as error:
            if not is_delta_conflict(error) or attempt_number == max_attempts:
                raise

            time.sleep(initial_delay_seconds * (2 ** (attempt_number - 1)))

    raise AssertionError("Delta retry loop exhausted without returning or raising")
