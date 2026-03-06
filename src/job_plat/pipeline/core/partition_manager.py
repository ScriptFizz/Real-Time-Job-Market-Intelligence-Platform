import json
from pathlib import Path
from datetime import date
from typing import List

class PartitionManager:
    
    def __init__(self, metadata_path: Path):
        self.metadata_path = metadata_path
        
        if not metadata_path.exists():
            metadata_path.write_text("{}")
    
    def _load(self) -> dict:
        return json.loads(self.metadata_path.read_text())
    
    def _save(self, state: dict) -> None:
        self.metadata_path.write_text(json.dumps(state, indent=2))

    def get_processed(self, stage_name: str) -> dict:
        
        state =self._load()
        values = states.get(stage_name, [])
        return {date.fromisoformat(v) for v in values}
    
    def mark_processed(self, stage_name: str, partitions: List) -> None:
        
        stage = self._load()
        existing = set(state.get(stage_name, []))
        new_values = {p.isoformat() for p in partitions}
        state[stage_name] = sorted(existing.union(new_values))
        self._save(state) 
