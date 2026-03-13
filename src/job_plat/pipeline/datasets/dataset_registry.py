from pathlib import Path
from job_plat.storage.storages import Storage
from job_plat.pipeline.datasets.dataset import Dataset


class DatasetRegistry:
    
    def __init__(
        self,
        root: Path,
        storage: Storage,
        dataset_defs: list[type]
        ):
            
        self._datasets = {}
        for ds in dataset_defs:
            dataset = Dataset(
                name = ds.NAME,
                path= Path(root) / ds.RELATIVE_PATH,
                storage=storage,
                partition_columns=getattr(ds, "PARTITION_COLUMNS", ["ingestion_date"]),
                time_window_column=getattr(ds, "TIME_WINDOW_COLUMN", ""),
                write_mode=getattr(ds, "WRITE_MODE", "append"),
                file_format=getattr(ds, "FILE_FORMAT", "parquet")
            )
            self._datasets[ds] = dataset
    
    def get(self, dataset_cls: type) -> Dataset:
        return self._datasets[dataset_cls]
    
    def list(self) -> list:
        return list(self._datasets.values())
