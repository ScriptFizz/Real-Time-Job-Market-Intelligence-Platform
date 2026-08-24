from collections.abc import Iterator

from job_plat.config.logconfig import ContextLogger
from job_plat.context.contexts import BronzeContext
from job_plat.ingestion.connectors import JobConnector
from job_plat.ingestion.metadata import IngestionRun, write_metadata
from job_plat.ingestion.search_criteria import JobSearchCriteria
from job_plat.pipeline.core.base_source_stage import BaseSourceStage
from job_plat.storage.paths import join_storage_path
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

    def create_context(self) -> IngestionRun:
        query, country, location = self._validate_search_config()
        execution_date = self.bronze_ctx.execution_date
        if execution_date is None:
            raise ValueError(
                "BronzeStage requires execution_date for deterministic partition identity"
            )
        run_context = IngestionRun(
            source=self.connector.name,
            query=query,
            country=country,
            location=location,
            execution_date=execution_date,
            pipeline_version="1.0.0",
        )
        return run_context

    def _enrich_with_ingestion_metadata(
        self, records: Iterator[dict], run: IngestionRun
    ) -> Iterator[dict]:
        for record in records:
            normalized = self.connector.normalize_with_accounting(record)
            if normalized is None:
                continue

            yield {
                "ingestion_metadata": {
                    "run_id": run.run_id,
                    "source": run.source,
                    "query": run.query,
                    "location": run.location,
                    "execution_date": run.execution_date.isoformat(),
                    "ingestion_date": run.execution_date.date().isoformat(),
                    "started_at": run.started_at.isoformat(),
                    "canonical_schema_version": "1.0.0",
                },
                "raw_payload": record,
                "payload": normalized.model_dump(),
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

        base_path = join_storage_path(
            self.bronze_ctx.root_path,
            "bronze",
            "jobs",
            f"ingestion_date={run.execution_date.date().isoformat()}",
            f"source={run.source}",
            f"run_id={run.run_id}",
        )

        data_path = join_storage_path(
            base_path,
            "part-000.jsonl",
        )

        row_count = self.storage.write_jsonl(
            records=enriched_stream,
            path=data_path,
        )
        schema_error_count = self.connector.schema_error_count

        write_metadata(
            storage=self.storage,
            path=base_path,
            run=run,
            row_count=row_count,
            schema_error_count=schema_error_count,
        )

        # Save runs metadata in metadata registry

        runs_path = join_storage_path(
            self.bronze_ctx.root_path,
            "_runs",
        )

        write_metadata(
            storage=self.storage,
            path=runs_path,
            run=run,
            row_count=row_count,
            schema_error_count=schema_error_count,
            filename=f"{run.run_id}.json",
        )

        # Log bronze run stats

        logger.info(
            "bronze_run_completed",
            extra={
                "source": run.source,
                "row_count": row_count,
                "schema_error_count": schema_error_count,
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
