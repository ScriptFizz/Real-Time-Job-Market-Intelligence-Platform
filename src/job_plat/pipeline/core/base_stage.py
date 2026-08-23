import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generic, TypeVar

from job_plat.config.logconfig import ContextLogger
from job_plat.context.contexts import SparkStageContext, StageExecutionContext
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.core.read_strategy import (
    IncrementalReadStrategy,
    PartitionBatch,
    ReadResult,
    ReadStrategy,
    StageInputs,
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
        self.validate_inputs()

        run_context = self.create_context()
        self.logger = ContextLogger(
            self._base_logger,
            {
                "run_id": run_context.run_id,
                "stage": run_context.stage,
            },
        )

        start = time.time()

        self.logger.info("stage_started")

        try:
            read_result = self.read()
            outputs = self.transform(read_result.inputs)
            self.validate_outputs(outputs)
            metrics = self.compute_metrics(outputs)
            if metrics:
                self.logger.info(
                    "stage_metrics", extra={"stage": self.STAGE_NAME, **metrics}
                )
                self.evaluate_metrics(metrics)
            self.write(
                outputs=outputs,
                batch=read_result.batch,
            )
            self.acknowledge(read_result.batch)

            duration = round(time.time() - start, 2)
            self.logger.info(
                "stage_completed",
                extra={"duration_seconds": duration},
            )

        except StageSkip as e:
            self.logger.info("stage_skipped", extra={"reason": str(e)})

        except Exception:
            duration = round(time.time() - start, 2)

            self.logger.error(
                "stage_failed", extra={"duration_seconds": duration}, exc_info=True
            )
            raise

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
            dataset.write(
                df,
                expected_partitions=batch.partitions,
            )
            write_strategy[field_name] = dataset.write_mode

        self.logger.info("write_strategy", extra=write_strategy)

    # ------------------------
    # ACKNOWLEDGE
    # -----------------------

    def acknowledge(
        self,
        batch: PartitionBatch,
    ) -> None:
        if batch.is_empty:
            return

        self.partition_manager.mark_processed(
            stage_name=self.STAGE_NAME,
            partitions=batch.partitions,
        )

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
            if not Path(ds.path).exists():
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

            if df.limit(1).count() == 0:
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
