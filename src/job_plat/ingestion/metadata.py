from dataclasses import dataclass
from datetime import datetime
import uuid
import json
from pathlib import Path
from job_plat.context.contexts import StageExecutionContext

# @dataclass(kw_only=True)
# class StageExecutionContext:
    # stage: str
    # pipeline_version: str
    # run_id: str = str(uuid.uuid4())
    # started_at: datetime = datetime.utcnow()


@dataclass(kw_only=True)
class IngestionRun(StageExecutionContext):
    stage: str = "bronze"
    source: str
    query: str
    country: str
    location: str


def write_metadata(
    path: Path, 
    run: IngestionRun, 
    row_count: int,
    filename: str = "_metadata.json"
    ):
    
    metadata = {
        "run_id": run.run_id,
        "source": run.source,
        "query": run.query,
        "location": run.location,
        "started_at": run.started_at.isoformat(),
        "pipeline_version": run.pipeline_version,
        "row_count": row_count,
    }
    
    path.mkdir(parents=True, exist_ok=True)
    
    with open(path / filename, "w") as f:
        json.dump(metadata, f, indent=2)

