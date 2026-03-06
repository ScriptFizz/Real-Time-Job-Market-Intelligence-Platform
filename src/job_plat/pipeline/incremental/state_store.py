import json
from pathlib import Path
from datetime import date

class StateStore:
    
    def __init__(self, path: Path):
        self.path = path
        
        if not path.exists():
            path.write_text("{}")
    
    def load(self):
        return json.loads(self.path_read_text())
    
    def get_watermark(self, pipeline_name: str) -> None:
        state = self.load()
        value = state.get(pipeline_name)
        
        if value:
            return date.fromisoformat(value)
            
        return None
    
    def update_watermark(self, pipeline_name: str, new_date: date) -> None:
        state = self.load()
        
        state[pipeline_name] = new_date.isoformat()
        
        self.path.write_text(json.dumps(state, indent=2))
