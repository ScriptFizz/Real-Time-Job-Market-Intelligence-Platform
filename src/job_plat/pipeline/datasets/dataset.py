from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date

from job_plat.pipeline.datasets.dataset_definitions import (
    WriteMode,
)
from job_plat.storage.storages import Storage


@dataclass
class Dataset:
    name: str
    path: str
    storage: Storage
    partition_columns: list[str] = field(default_factory=lambda: ["ingestion_date"])
    time_window_column: str | None = field(default_factory=lambda: None)
    write_mode: WriteMode = "append"
    file_format: Literal["parquet", "jsonl"] = "parquet"

    def read_all(
        self,
        spark: SparkSession,
    ) -> DataFrame:
        base_path = str(self.path)

        if self.file_format == "parquet":
            return self.storage.read_parquet(
                spark=spark,
                base_path=base_path,
                paths=[base_path],
            )

        if self.file_format == "jsonl":
            return self.storage.read_jsonl(
                spark=spark,
                base_path=base_path,
                paths=[base_path],
            )

        raise ValueError(f"Unsupported format {self.file_format}")

    def list_partitions(self) -> list[date]:
        if not self.partition_columns:
            return []

        partition_col = self.partition_columns[0]

        pattern = f"{partition_col}=*"
        dirs = self.storage.list_dirs(path=self.path, pattern=pattern)
        partitions = []

        for p in dirs:
            value = Path(p).name.split("=")[1]
            partitions.append(date.fromisoformat(value))

        return sorted(set(partitions))

    def read_partitions(
        self,
        spark: SparkSession,
        partitions: list[date] | None = None,
        filters: list[date] | None = None,
    ) -> DataFrame:
        if not self.partition_columns:
            return self.read_all(spark)

        partition_col = self.partition_columns[0]

        if not partitions and not filters:
            raise ValueError(f"No partitions to read for dataset {self.name}")

        base_path = str(self.path)

        if partitions:
            paths = [f"{base_path}/{partition_col}={p}" for p in partitions]

            if self.file_format == "parquet":
                return self.storage.read_parquet(
                    spark=spark, base_path=base_path, paths=paths
                )

            if self.file_format == "jsonl":
                return self.storage.read_jsonl(
                    spark=spark,
                    base_path=base_path,
                    paths=paths,
                )

            raise ValueError(f"Unsupported format {self.file_format}")

        if filters is None:
            raise ValueError(f"No partitions to read for dataset {self.name}")

        dataframe = self.read_all(spark)

        return dataframe.filter(col(partition_col).isin(filters))

    def write(
        self,
        df: DataFrame,
        mode: WriteMode | None = None,
        expected_partitions: tuple[date, ...] | None = None,
    ) -> None:
        actual_mode = mode or self.write_mode

        partition_cols = self.partition_columns if self.partition_columns else None

        dynamic_partition_overwrite = actual_mode == "replace_partitions"

        if dynamic_partition_overwrite:
            if not partition_cols:
                raise ValueError(
                    f"Dataset {self.name} cannot replace "
                    "partitions because it is not partitioned"
                )

            if not expected_partitions:
                raise ValueError(
                    f"Dataset {self.name} requires an explicit "
                    "partition batch for partition replacement"
                )

            self._validate_output_partitions(
                df=df,
                expected_partitions=expected_partitions,
            )
            storage_mode = "overwrite"
        else:
            storage_mode = actual_mode

        if self.file_format == "parquet":
            self.storage.write_parquet(
                df=df,
                path=str(self.path),
                partition_cols=partition_cols,
                mode=storage_mode,
                dynamic_partition_overwrite=(dynamic_partition_overwrite),
            )
        elif self.file_format == "jsonl":
            self.storage.write_dataframe_json(
                df=df,
                path=str(self.path),
                partition_cols=partition_cols,
                mode=storage_mode,
                dynamic_partition_overwrite=(dynamic_partition_overwrite),
            )
        else:
            raise ValueError(f"Unsupported format {self.file_format}")

    def _validate_output_partitions(
        self,
        df: DataFrame,
        expected_partitions: tuple[date, ...],
    ) -> None:
        partition_column = self.partition_columns[0]

        rows = (
            df.select(to_date(col(partition_column)).alias("partition_date"))
            .distinct()
            .collect()
        )

        actual_partitions = {row["partition_date"] for row in rows}

        expected = set(expected_partitions)

        unexpected = actual_partitions - expected

        if unexpected:
            raise ValueError(
                f"Dataset {self.name} produced partitions "
                f"outside the active batch: {sorted(unexpected)}"
            )
