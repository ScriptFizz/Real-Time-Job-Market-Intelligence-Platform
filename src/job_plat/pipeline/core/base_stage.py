import logging
import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar

from job_plat.config.logconfig import ContextLogger
from job_plat.context.contexts import SparkStageContext, StageExecutionContext
from job_plat.observability.run_manifest import RunManifestWriter
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.partitioning.processing_ledger import ProcessingAttempt
from job_plat.pipeline.core.read_strategy import (
    IncrementalReadStrategy,
    PartitionBatch,
    ReadResult,
    ReadStrategy,
    StageInputs,
)
from job_plat.pipeline.datasets.data_contracts import (
    collect_dataset_quality_metrics,
    validate_dataset_contract,
)
from job_plat.pipeline.datasets.dataset_definitions import DatasetDef
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.schemas.output_schemas import StageOutput
from job_plat.utils.helpers import StageSkip

ContextT = TypeVar("ContextT", bound=SparkStageContext)
OutputT = TypeVar("OutputT", bound=StageOutput)

Metrics = dict[str, Any]


class BaseStage(ABC, Generic[ContextT, OutputT]):
    STAGE_NAME: str
    INPUT_MAP: dict[str, type[DatasetDef]]
    OUTPUT_TYPE: type[OutputT]
    READ_STRATEGY: ReadStrategy = IncrementalReadStrategy()
    ALLOW_EMPTY_OUTPUTS = False

    def __init__(
        self,
        datasets: DatasetRegistry,
        partition_manager: PartitionManager,
        ctx: ContextT,
    ):
        self.spark = ctx.spark
        self.datasets = datasets
        self.partition_manager = partition_manager
        self.ctx = ctx
        self._base_logger = logging.getLogger(
            f"pipeline.{self.__module__}.{self.__class__.__name__}"
        )

    # ----------------------------

    def execute(self) -> None:
        run_context = self.create_context()
        self.logger = ContextLogger(
            self._base_logger,
            {
                "run_id": run_context.run_id,
                "stage": run_context.stage,
                "batch_id": None,
                "partition_batch": [],
                "model_id": None,
                "model_version": self._configured_model_version(),
            },
        )

        start = time.time()
        attempt: ProcessingAttempt | None = None
        input_observability: dict[str, Any] = {}
        output_observability: dict[str, Any] = {}
        stage_metrics: Metrics = {}
        model_identity: dict[str, Any] = {}
        manifest = RunManifestWriter(
            storage=self.datasets.list()[0].storage,
            metadata_path=self.partition_manager.metadata_path,
        )

        self.logger.info("stage_started")
        manifest.write(
            run_id=run_context.run_id,
            stage=run_context.stage,
            status="started",
            started_at=run_context.started_at,
            batch_id=None,
            partitions=[],
        )

        try:
            self.validate_inputs()
            read_result = self.read()
            attempt = self.partition_manager.start_attempt(
                stage_name=self.STAGE_NAME,
                partitions=read_result.batch.partitions,
                attempt_id=run_context.run_id,
                started_at=run_context.started_at,
            )
            batch_id = attempt.batch_id if attempt is not None else None
            partition_batch = [
                partition.isoformat() for partition in read_result.batch.partitions
            ]
            self.logger.bind(
                batch_id=batch_id,
                partition_batch=partition_batch,
            )
            input_observability = self._collect_input_observability(read_result.inputs)
            self.logger.info("stage_inputs", extra={"datasets": input_observability})

            outputs = self.transform(read_result.inputs)
            self.validate_outputs(outputs)
            output_observability = self._collect_output_observability(outputs)
            model_identity = self._extract_model_identity(outputs)
            self.logger.bind(
                model_id=model_identity.get("model_id"),
                model_version=model_identity.get(
                    "model_version", self._configured_model_version()
                ),
            )
            self.logger.info("stage_outputs", extra={"datasets": output_observability})

            stage_metrics = {
                "rejected_record_count": 0,
                "dead_letter_count": 0,
                **self.compute_metrics(outputs),
            }
            if stage_metrics:
                self.logger.info(
                    "stage_metrics",
                    extra={"stage": self.STAGE_NAME, **stage_metrics},
                )
                self.evaluate_metrics(stage_metrics)
            self.write(
                outputs=outputs,
                batch=read_result.batch,
            )
            self.acknowledge(read_result.batch, attempt)

            duration = round(time.time() - start, 2)
            self.logger.info(
                "stage_completed",
                extra={"duration_seconds": duration},
            )

            is_no_op = bool(output_observability) and all(
                dataset_metrics.get("row_count") == 0
                for dataset_metrics in output_observability.values()
            )
            if is_no_op:
                self.logger.info("stage_no_op", extra={"reason": "empty_outputs"})

            manifest.write(
                run_id=run_context.run_id,
                stage=run_context.stage,
                status="no_op" if is_no_op else "committed",
                started_at=run_context.started_at,
                finished_at=datetime.now(UTC),
                batch_id=batch_id,
                partitions=partition_batch,
                inputs=input_observability,
                outputs=output_observability,
                metrics=stage_metrics,
                model_identity=model_identity,
            )

        except StageSkip as error:
            if attempt is not None:
                self._mark_attempt_failed(attempt, error)
            self.logger.info("stage_skipped", extra={"reason": str(error)})
            manifest.write(
                run_id=run_context.run_id,
                stage=run_context.stage,
                status="no_op" if attempt is None else "failed",
                started_at=run_context.started_at,
                finished_at=datetime.now(UTC),
                batch_id=attempt.batch_id if attempt else None,
                partitions=(
                    [partition.isoformat() for partition in attempt.partitions]
                    if attempt
                    else []
                ),
                inputs=input_observability,
                outputs=output_observability,
                metrics=stage_metrics,
                failure=(
                    None
                    if attempt is None
                    else {
                        "type": type(error).__name__,
                        "message": str(error)[:2000],
                    }
                ),
            )

        except Exception as error:
            duration = round(time.time() - start, 2)

            self._mark_attempt_failed(attempt, error)

            self.logger.error(
                "stage_failed", extra={"duration_seconds": duration}, exc_info=True
            )
            manifest.write(
                run_id=run_context.run_id,
                stage=run_context.stage,
                status="failed",
                started_at=run_context.started_at,
                finished_at=datetime.now(UTC),
                batch_id=attempt.batch_id if attempt else None,
                partitions=(
                    [partition.isoformat() for partition in attempt.partitions]
                    if attempt
                    else []
                ),
                inputs=input_observability,
                outputs=output_observability,
                metrics=stage_metrics,
                model_identity=model_identity,
                failure={
                    "type": type(error).__name__,
                    "message": str(error)[:2000],
                },
            )
            raise

    def _mark_attempt_failed(
        self,
        attempt: ProcessingAttempt | None,
        error: BaseException,
    ) -> None:
        if attempt is None:
            return

        try:
            self.partition_manager.mark_failed(attempt, error)
        except Exception:
            self.logger.error("processing_ledger_update_failed", exc_info=True)

    # ---------------------
    # READ
    # ---------------------

    def read(self) -> ReadResult:
        datasets = {
            name: self.datasets.get(dataset_definition)
            for name, dataset_definition in self.INPUT_MAP.items()
        }

        return self.READ_STRATEGY.read(
            stage=self,
            datasets=datasets,
            execution_date=self.ctx.execution_date,
        )

    # ------------------------
    # WRITE
    # ------------------------

    def write(
        self,
        outputs: OutputT,
        batch: PartitionBatch,
    ) -> None:
        if not outputs:
            self.logger.info("No outputs to write", extra={"stage": self.STAGE_NAME})
            return

        dataset_map = outputs.__class__.dataset_map()
        write_strategy = {}

        for field_name, df in vars(outputs).items():
            dataset_cls = dataset_map[field_name]
            dataset = self.datasets.get(dataset_cls)
            validate_dataset_contract(
                df,
                dataset_name=dataset_cls.NAME,
                required_columns=dataset_cls.REQUIRED_COLUMNS,
                expected_types=dataset_cls.EXPECTED_TYPES,
                non_null_columns=dataset_cls.NON_NULL_COLUMNS,
                unique_keys=dataset_cls.UNIQUE_KEYS,
            )
            dataset.write(
                df,
                expected_partitions=batch.partitions,
            )
            write_strategy[field_name] = dataset.write_mode

        self.logger.info("write_strategy", extra=write_strategy)

    def _collect_input_observability(
        self, inputs: StageInputs
    ) -> dict[str, dict[str, int]]:
        return {
            name: {"row_count": dataframe.count()}
            for name, dataframe in inputs.items()
            if dataframe is not None
        }

    def _collect_output_observability(
        self, outputs: OutputT
    ) -> dict[str, dict[str, Any]]:
        dataset_map = outputs.__class__.dataset_map()
        return {
            field_name: collect_dataset_quality_metrics(
                dataframe,
                non_null_columns=dataset_map[field_name].NON_NULL_COLUMNS,
                freshness_column=dataset_map[field_name].FRESHNESS_COLUMN,
                reference_time=self.ctx.execution_date,
            ).as_dict()
            for field_name, dataframe in vars(outputs).items()
        }

    @staticmethod
    def _extract_model_identity(outputs: OutputT) -> dict[str, Any]:
        for dataframe in vars(outputs).values():
            identity_columns = [
                name
                for name in ("model_id", "model_version")
                if name in dataframe.columns
            ]
            if not identity_columns:
                continue
            rows = dataframe.select(*identity_columns).distinct().limit(2).collect()
            if len(rows) == 1:
                return rows[0].asDict(recursive=True)
        return {}

    def _configured_model_version(self) -> str | None:
        return getattr(
            self.ctx,
            "model_version",
            getattr(self.ctx, "embedding_model_version", None),
        )

    # ------------------------
    # ACKNOWLEDGE
    # -----------------------

    def acknowledge(
        self,
        batch: PartitionBatch,
        attempt: ProcessingAttempt | None,
    ) -> None:
        if batch.is_empty:
            return

        if attempt is None:
            raise RuntimeError(
                "Cannot acknowledge a partition batch without an attempt"
            )

        self.partition_manager.mark_processed(attempt)

        self.logger.info(
            "partition_batch_processed",
            extra={
                "partitions": [partition.isoformat() for partition in batch.partitions]
            },
        )

    # ------------------------
    # VALIDATION IO
    # ------------------------

    def validate_inputs(self) -> None:
        missing = []
        for dataset_cls in self.INPUT_MAP.values():
            ds = self.datasets.get(dataset_cls)
            if not ds.storage.exists(str(ds.path)):
                missing.append(str(ds.path))

        if missing:
            raise FileNotFoundError(f"Missing input dataset(s): {', '.join(missing)}")

    def validate_outputs(self, outputs: OutputT) -> None:
        if not isinstance(outputs, self.OUTPUT_TYPE):
            raise TypeError(
                f"{self.STAGE_NAME}: expected output {self.OUTPUT_TYPE.__name__}, "
                f"got {type(outputs).__name__}"
            )
        for name, df in vars(outputs).items():
            if df is None:
                raise ValueError(f"{self.STAGE_NAME}: output '{name}' is None")

            if not self.ALLOW_EMPTY_OUTPUTS and df.limit(1).count() == 0:
                raise ValueError(f"{self.STAGE_NAME}: output '{name}' is empty")

    # --------------------
    # OPTIONAL METHODS
    # --------------------

    def compute_metrics(self, _outputs: OutputT) -> Metrics:
        return {}

    def evaluate_metrics(self, _metrics: Metrics) -> None:  # noqa: B027
        pass

    # ---------------------
    # REQUIRED METHODS
    # ---------------------

    @abstractmethod
    def transform(self, inputs: StageInputs) -> OutputT:
        raise NotImplementedError

    @abstractmethod
    def create_context(self) -> StageExecutionContext:
        pass
