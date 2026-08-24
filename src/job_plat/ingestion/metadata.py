from dataclasses import dataclass
from datetime import datetime
from typing import Any

from job_plat.context.contexts import StageExecutionContext
from job_plat.storage.paths import join_storage_path
from job_plat.storage.storages import Storage


@dataclass(kw_only=True)
class IngestionRun(StageExecutionContext):
    stage: str = "bronze"
    source: str
    query: str
    country: str
    location: str
    execution_date: datetime


def build_ingestion_metadata(
    *,
    run: IngestionRun,
    row_count: int,
    schema_error_count: int = 0,
) -> dict[str, Any]:
    if row_count < 0:
        raise ValueError("row_count must not be negative")
    if schema_error_count < 0:
        raise ValueError("schema_error_count must not be negative")

    return {
        "run_id": run.run_id,
        "source": run.source,
        "query": run.query,
        "country": run.country,
        "location": run.location,
        "execution_date": run.execution_date.isoformat(),
        "ingestion_date": run.execution_date.date().isoformat(),
        "started_at": run.started_at.isoformat(),
        "pipeline_version": run.pipeline_version,
        "row_count": row_count,
        "schema_error_count": schema_error_count,
    }


def write_metadata(
    *,
    storage: Storage,
    path: str,
    run: IngestionRun,
    row_count: int,
    schema_error_count: int = 0,
    filename: str = "_metadata.json",
) -> None:
    metadata_path = join_storage_path(
        path,
        filename,
    )

    storage.write_json(
        build_ingestion_metadata(
            run=run,
            row_count=row_count,
            schema_error_count=schema_error_count,
        ),
        metadata_path,
    )
