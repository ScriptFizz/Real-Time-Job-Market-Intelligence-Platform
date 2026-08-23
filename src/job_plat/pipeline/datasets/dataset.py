from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Literal

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, lit, row_number, to_date

from job_plat.pipeline.datasets.dataset_definitions import MergeOrder, WriteMode
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
    merge_keys: tuple[str, ...] = ()
    merge_order_column: str | None = None
    merge_order: MergeOrder = "desc"

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

        if actual_mode == "merge":
            self._merge_and_overwrite(df)
            return

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

    def _merge_and_overwrite(
        self,
        incoming: DataFrame,
    ) -> None:
        if self.partition_columns:
            raise ValueError(
                f"Dataset {self.name} cannot use merge mode because it is partitioned"
            )

        if self.file_format != "parquet":
            raise ValueError(
                f"Dataset {self.name} supports merge mode only for Parquet"
            )

        if not self.merge_keys:
            raise ValueError(f"Dataset {self.name} requires merge keys")

        missing_keys = set(self.merge_keys) - set(incoming.columns)

        if missing_keys:
            raise ValueError(
                f"Dataset {self.name} is missing merge keys: {sorted(missing_keys)}"
            )

        if (
            self.merge_order_column is not None
            and self.merge_order_column not in incoming.columns
        ):
            raise ValueError(
                f"Dataset {self.name} is missing merge "
                f"order column: {self.merge_order_column}"
            )

        if not self.storage.exists(str(self.path)):
            self.storage.write_parquet(
                df=incoming,
                path=str(self.path),
                mode="overwrite",
                partition_cols=None,
            )
            return

        existing = self.read_all(spark=incoming.sparkSession)

        priority_column = "__job_plat_merge_priority"
        row_number_column = "__job_plat_row_number"

        combined = existing.withColumn(priority_column, lit(0)).unionByName(
            incoming.withColumn(
                priority_column,
                lit(1),
            )
        )

        order_columns = []

        if self.merge_order_column is not None:
            ordering = col(self.merge_order_column)

            if self.merge_order == "asc":
                order_columns.append(ordering.asc_nulls_last())
            else:
                order_columns.append(ordering.desc_nulls_last())

        # On equal ordering values, the incoming row wins.
        order_columns.append(col(priority_column).desc())

        window = Window.partitionBy(*self.merge_keys).orderBy(*order_columns)

        merged = (
            combined.withColumn(
                row_number_column,
                row_number().over(window),
            )
            .filter(col(row_number_column) == 1)
            .drop(
                row_number_column,
                priority_column,
            )
        )

        materialized = merged.localCheckpoint(eager=True)

        try:
            self.storage.write_parquet(
                df=materialized,
                path=str(self.path),
                mode="overwrite",
                partition_cols=None,
            )
        finally:
            materialized.unpersist()

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
