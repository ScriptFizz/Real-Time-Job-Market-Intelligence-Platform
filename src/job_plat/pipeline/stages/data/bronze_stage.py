from collections.abc import Iterator
from pathlib import Path

from job_plat.config.logconfig import ContextLogger
from job_plat.context.contexts import BronzeContext
from job_plat.ingestion.connectors import JobConnector
from job_plat.ingestion.metadata import IngestionRun, write_metadata
from job_plat.ingestion.search_criteria import JobSearchCriteria
from job_plat.pipeline.core.base_source_stage import BaseSourceStage
from job_plat.storage.storages import Storage


class BronzeStage(BaseSourceStage):
    def __init__(
        self, bronze_ctx: BronzeContext, storage: Storage, connector: JobConnector
    ):
        super().__init__(storage=storage)
        self.bronze_ctx = bronze_ctx
        self.connector = connector

    def validate_config(self) -> None:
        self._validate_search_config()
        # missing = []
        # if not self.bronze_ctx.query:
        #     missing.append("Query must not be empty")
        # if not self.bronze_ctx.location:
        #     missing.append("Location must not be empty")
        # if missing:
        #     raise ValueError(", ".join(missing))

    def create_context(self) -> IngestionRun:
        query, country, location = self._validate_search_config()
        run_context = IngestionRun(
            source=self.connector.name,
            query=query,
            country=country,
            location=location,
            pipeline_version="1.0.0",
        )
        return run_context

    def _enrich_with_ingestion_metadata(
        self, records: Iterator[dict], run: IngestionRun
    ) -> Iterator[dict]:
        for record in records:
            yield {
                "ingestion_metadata": {
                    "run_id": run.run_id,
                    "source": run.source,
                    "query": run.query,
                    "location": run.location,
                    "started_at": run.started_at.isoformat(),
                    "canonical_schema_version": "1.0.0",
                },
                "raw_payload": record,
                "payload": self.connector.normalize(record).model_dump(),
            }

    def produce(self, run: IngestionRun, logger: ContextLogger) -> int:
        logger.info(
            "bronze_run_started",
            extra={"source": run.source},
        )

        criteria = JobSearchCriteria(
            query=run.query,
            country=run.country,
            location=run.location,
        )

        raw_stream = self.connector.fetch(
            criteria=criteria,
        )

        enriched_stream = self._enrich_with_ingestion_metadata(
            records=raw_stream,
            run=run,
        )

        base_path = (
            Path(self.bronze_ctx.root_path)
            / "bronze"
            / "jobs"
            / f"ingestion_date={run.started_at.date()}"
            / f"source={run.source}"
            / f"run_id={run.run_id}"
        )

        data_path = base_path / "part-000.jsonl"

        row_count = self.storage.write_jsonl(
            records=enriched_stream,
            path=str(data_path),
        )

        write_metadata(path=base_path, run=run, row_count=row_count)

        # Save runs metadata in metadata registry

        runs_dir = Path(self.bronze_ctx.root_path) / "_runs"

        runs_dir.mkdir(parents=True, exist_ok=True)
        write_metadata(
            path=runs_dir, run=run, row_count=row_count, filename=f"{run.run_id}.json"
        )

        # Log bronze run stats

        logger.info(
            "bronze_run_completed",
            extra={
                "source": run.source,
                "row_count": row_count,
            },
        )
        return row_count

    def _validate_search_config(self) -> tuple[str, str, str]:
        query = self.bronze_ctx.query
        country = self.bronze_ctx.country
        location = self.bronze_ctx.location

        if query and country and location:
            return query, country, location

        missing = []

        if not query:
            missing.append("Query must not be empty")
        if not country:
            missing.append("Country must not be empty")
        if not location:
            missing.append("Location must not be empty")

        raise ValueError(", ".join(missing))
