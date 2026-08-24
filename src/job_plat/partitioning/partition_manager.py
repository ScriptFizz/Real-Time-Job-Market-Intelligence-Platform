from collections.abc import Iterable
from datetime import date

from job_plat.partitioning.state_store import StateStore


class PartitionManager:
    def __init__(self, state_store: StateStore):
        self.state_store = state_store

    def get_processed(self, stage_name: str) -> set[date]:
        state = self.state_store.load()
        values = state.get(stage_name, [])
        return {date.fromisoformat(v) for v in values}

    def mark_processed(self, stage_name: str, partitions: Iterable[date]) -> None:
        state = self.state_store.load()
        existing = set(state.get(stage_name, []))
        new_values = {p.isoformat() for p in partitions}
        state[stage_name] = sorted(existing | new_values)
        self.state_store.save(state)
