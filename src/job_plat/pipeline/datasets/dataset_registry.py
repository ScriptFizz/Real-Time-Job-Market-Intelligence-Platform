from collections.abc import Sequence
from pathlib import Path

from job_plat.pipeline.datasets.dataset import Dataset
from job_plat.pipeline.datasets.dataset_definitions import DatasetDef
from job_plat.storage.storages import Storage


class DatasetRegistry:
    def __init__(
        self,
        root: str | Path,
        storage: Storage,
        dataset_defs: Sequence[type[DatasetDef]],
    ):
        self._datasets: dict[type[DatasetDef], Dataset] = {}
        root_text = str(root).rstrip("/")
        for dataset_def in dataset_defs:
            relative_path = dataset_def.RELATIVE_PATH.lstrip("/")
            dataset_path = f"{root_text}/{relative_path}"
            dataset = Dataset(
                name=dataset_def.NAME,
                path=dataset_path,
                storage=storage,
                partition_columns=getattr(
                    dataset_def, "PARTITION_COLUMNS", ["ingestion_date"]
                ),
                time_window_column=getattr(dataset_def, "TIME_WINDOW_COLUMN", ""),
                write_mode=getattr(dataset_def, "WRITE_MODE", "append"),
                file_format=getattr(dataset_def, "FILE_FORMAT", "parquet"),
            )
            self._datasets[dataset_def] = dataset

    def get(self, dataset_cls: type[DatasetDef]) -> Dataset:
        return self._datasets[dataset_cls]

    def list(self) -> list[Dataset]:
        return list(self._datasets.values())
