from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.storage.storages import Storage


@dataclass
class Dataset:
    name: str
    path: str
    storage: Storage
    partition_columns: list[str] = field(default_factory=lambda: ["ingestion_date"])
    time_window_column: str | None = field(default_factory=lambda: None)
    write_mode: Literal["append", "overwrite"] = "append"
    file_format: Literal["parquet", "jsonl"] = "parquet"

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
            if self.file_format == "parquet":
                return spark.read.parquet(str(self.path))
            else:
                return spark.read.option("multiline", False).json(str(self.path))

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

        if self.file_format == "parquet":
            dataframe = spark.read.parquet(base_path)
        elif self.file_format == "jsonl":
            dataframe = spark.read.option("multiLine", False).json(base_path)
        else:
            raise ValueError(f"Unsupported format {self.file_format}")

        return dataframe.filter(col(partition_col).isin(filters))

    def write(
        self, df: DataFrame, mode: Literal["append", "overwrite"] | None = None
    ) -> None:
        actual_mode = mode or self.write_mode

        partition_cols = self.partition_columns if self.partition_columns else None

        if self.file_format == "parquet":
            self.storage.write_parquet(
                df=df,
                path=str(self.path),
                partition_cols=partition_cols,
                mode=actual_mode,
            )
        elif self.file_format == "jsonl":
            self.storage.write_dataframe_json(
                df=df,
                path=str(self.path),
                partition_cols=partition_cols,
                mode=actual_mode,
            )
        else:
            raise ValueError(f"Unsupported format {self.file_format}")

    def get_available_partitions(
        self, partition_manager: PartitionManager, stage_name: str
    ) -> list[date]:
        available = self.list_partitions()
        processed = partition_manager.get_processed(stage_name=stage_name)
        return sorted(set(available) - set(processed))
