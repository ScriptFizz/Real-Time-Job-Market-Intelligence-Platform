from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import col, lit, row_number, to_date

from job_plat.pipeline.datasets.dataset_definitions import (
    FileFormat,
    MergeOrder,
    WriteMode,
)
from job_plat.storage.delta_retry import run_with_delta_retry
from job_plat.storage.storages import Storage


@dataclass
class Dataset:
    name: str
    path: str
    storage: Storage
    partition_columns: list[str] = field(default_factory=lambda: ["ingestion_date"])
    time_window_column: str | None = field(default_factory=lambda: None)
    write_mode: WriteMode = "append"
    file_format: FileFormat = "parquet"
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

        if self.file_format == "delta":
            return spark.read.format("delta").load(base_path)

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

            if self.file_format == "delta":
                return self.read_all(spark).filter(col(partition_col).isin(partitions))

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
            self._merge_delta(df)
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

        if self.file_format == "delta" and dynamic_partition_overwrite:
            self._replace_delta_partitions(
                dataframe=df,
                expected_partitions=expected_partitions,
            )
        elif self.file_format == "delta":
            writer = df.write.format("delta").mode(storage_mode)

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            writer.save(str(self.path))
        elif self.file_format == "parquet":
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

    def _merge_delta(
        self,
        incoming: DataFrame,
    ) -> None:
        if self.partition_columns:
            raise ValueError(
                f"Dataset {self.name} cannot use merge mode because it is partitioned"
            )

        if self.file_format != "delta":
            raise ValueError(f"Dataset {self.name} supports merge mode only for Delta")

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

        source = self._deduplicate_merge_source(incoming)
        spark = incoming.sparkSession
        path = str(self.path)

        if not DeltaTable.isDeltaTable(spark, path):
            if self.storage.exists(path):
                raise ValueError(
                    f"Dataset {self.name} exists at {path} but is not a Delta table"
                )

            try:
                source.write.format("delta").mode("errorifexists").save(path)
                return
            except Exception:
                # A concurrent writer may have initialized the table.
                if not DeltaTable.isDeltaTable(spark, path):
                    raise

        run_with_delta_retry(
            lambda: self._execute_delta_merge(
                source=source,
                spark=spark,
                path=path,
            )
        )

    def _execute_delta_merge(
        self,
        *,
        source: DataFrame,
        spark: SparkSession,
        path: str,
    ) -> None:
        target = DeltaTable.forPath(spark, path)
        merger = (
            target.alias("target")
            .merge(
                source.alias("source"),
                self._merge_key_condition(
                    target_alias="target",
                    source_alias="source",
                ),
            )
            .withSchemaEvolution()
        )

        matched_condition = self._matched_update_condition(
            target_alias="target",
            source_alias="source",
        )
        if matched_condition is None:
            merger = merger.whenMatchedUpdateAll()
        else:
            merger = merger.whenMatchedUpdateAll(condition=matched_condition)

        merger.whenNotMatchedInsertAll().execute()

    def _deduplicate_merge_source(self, incoming: DataFrame) -> DataFrame:
        order_columns: list[Column] = []

        if self.merge_order_column is not None:
            ordering = col(self.merge_order_column)
            if self.merge_order == "asc":
                order_columns.append(ordering.asc_nulls_last())
            else:
                order_columns.append(ordering.desc_nulls_last())

        tie_breaker_columns = sorted(
            set(incoming.columns) - set(self.merge_keys) - {self.merge_order_column}
        )
        order_columns.extend(
            col(column_name).cast("string").desc_nulls_last()
            for column_name in tie_breaker_columns
        )

        if not order_columns:
            order_columns.append(lit(1))

        row_number_column = "__job_plat_merge_row_number"
        window = Window.partitionBy(*self.merge_keys).orderBy(*order_columns)

        return (
            incoming.withColumn(row_number_column, row_number().over(window))
            .filter(col(row_number_column) == 1)
            .drop(row_number_column)
        )

    def _merge_key_condition(
        self,
        target_alias: str,
        source_alias: str,
    ) -> Column:
        condition = col(f"{target_alias}.{self.merge_keys[0]}").eqNullSafe(
            col(f"{source_alias}.{self.merge_keys[0]}")
        )

        for key in self.merge_keys[1:]:
            condition = condition & col(f"{target_alias}.{key}").eqNullSafe(
                col(f"{source_alias}.{key}")
            )

        return condition

    def _matched_update_condition(
        self,
        target_alias: str,
        source_alias: str,
    ) -> Column | None:
        if self.merge_order_column is None:
            return None

        target_order = col(f"{target_alias}.{self.merge_order_column}")
        source_order = col(f"{source_alias}.{self.merge_order_column}")
        both_null = target_order.isNull() & source_order.isNull()

        if self.merge_order == "asc":
            preferred = source_order <= target_order
        else:
            preferred = source_order >= target_order

        return both_null | (
            source_order.isNotNull() & (target_order.isNull() | preferred)
        )

    def _replace_delta_partitions(
        self,
        dataframe: DataFrame,
        expected_partitions: tuple[date, ...] | None,
    ) -> None:
        if expected_partitions is None:
            raise ValueError(
                f"Dataset {self.name} requires an explicit partition batch"
            )

        partition_column = self.partition_columns[0]
        partition_predicate = " OR ".join(
            f"`{partition_column}` = DATE '{partition.isoformat()}'"
            for partition in expected_partitions
        )

        run_with_delta_retry(
            lambda: (
                dataframe.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", partition_predicate)
                .partitionBy(*self.partition_columns)
                .save(str(self.path))
            )
        )

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
