import json
from collections.abc import Iterable
from datetime import date
from pathlib import Path


class PartitionManager:
    def __init__(self, metadata_path: str | Path):
        self.metadata_path = Path(metadata_path)
        self.metadata_filepath = self.metadata_path / "partitions_metadata.json"

        if not self.metadata_filepath.exists():
            self.metadata_filepath.write_text("{}")

    def _load(self) -> dict[str, list[str]]:
        return json.loads(self.metadata_filepath.read_text())

    def _save(self, state: dict[str, list[str]]) -> None:
        self.metadata_filepath.write_text(json.dumps(state, indent=2))

    def get_processed(self, stage_name: str) -> set[date]:
        state = self._load()
        values = state.get(stage_name, [])
        return {date.fromisoformat(v) for v in values}

    def mark_processed(self, stage_name: str, partitions: Iterable[date]) -> None:
        state = self._load()
        existing = set(state.get(stage_name, []))
        new_values = {p.isoformat() for p in partitions}
        state[stage_name] = sorted(existing.union(new_values))
        self._save(state)
