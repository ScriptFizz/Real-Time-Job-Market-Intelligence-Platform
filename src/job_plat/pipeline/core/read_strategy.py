from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.datasets.dataset import Dataset

StageInputs = dict[str, DataFrame | None]


@dataclass(frozen=True)
class PartitionBatch:
    partitions: tuple[date, ...]

    @property
    def is_empty(self) -> bool:
        return not self.partitions


@dataclass
class ReadResult:
    inputs: StageInputs
    batch: PartitionBatch


class ReadableStage(Protocol):
    STAGE_NAME: str
    spark: SparkSession
    partition_manager: PartitionManager


class ReadStrategy(ABC):
    @abstractmethod
    def read(
        self,
        *,
        stage: ReadableStage,
        datasets: Mapping[str, Dataset],
        execution_date: datetime | None,
    ) -> ReadResult:
        raise NotImplementedError


class IncrementalReadStrategy(ReadStrategy):
    def read(
        self,
        *,
        stage: ReadableStage,
        datasets: Mapping[str, Dataset],
        execution_date: datetime | None,
    ) -> ReadResult:
        del execution_date

        partitioned_datasets = {
            name: dataset
            for name, dataset in datasets.items()
            if dataset.partition_columns
        }

        if partitioned_datasets:
            processed = stage.partition_manager.get_processed(
                stage_name=stage.STAGE_NAME
            )

            available_sets = [
                set(dataset.list_partitions()) - processed
                for dataset in partitioned_datasets.values()
            ]

            common_partitions = set.intersection(*available_sets)

            batch = PartitionBatch(partitions=tuple(sorted(common_partitions)))

            if batch.is_empty:
                return ReadResult(
                    inputs={name: None for name in datasets},
                    batch=batch,
                )
        else:
            batch = PartitionBatch(partitions=())

        inputs: StageInputs = {}

        for name, dataset in datasets.items():
            if dataset.partition_columns:
                inputs[name] = dataset.read_partitions(
                    spark=stage.spark, partitions=list(batch.partitions)
                )
            else:
                inputs[name] = dataset.read_all(spark=stage.spark)

        return ReadResult(
            inputs=inputs,
            batch=batch,
        )


class TimeWindowReadStrategy(ReadStrategy):
    def __init__(self, window_days: int):
        if window_days <= 0:
            raise ValueError("window_days must be greater than zero")
        self.window_days = window_days

    def read(
        self,
        *,
        stage: ReadableStage,
        datasets: Mapping[str, Dataset],
        execution_date: datetime | None,
    ) -> ReadResult:
        window_end = execution_date if execution_date is not None else datetime.now(UTC)

        window_start = window_end - timedelta(days=self.window_days)

        inputs: StageInputs = {}

        for name, dataset in datasets.items():
            dataframe = dataset.read_all(spark=stage.spark)

            if dataset.time_window_column is not None:
                time_column = col(dataset.time_window_column)

                dataframe = dataframe.filter(
                    (time_column >= window_start) & (time_column < window_end)
                )

            inputs[name] = dataframe

        return ReadResult(
            inputs=inputs,
            batch=PartitionBatch(partitions=()),
        )
