# from typing import Any
# import json

# class RawStorage:
    # def save(self, data: Any | None = None, filename: str | None = None) -> None:
        # raise NotImplementedError


# class LocalStorage(RawStorage):
    # def save(self, data: Any | None = None, filename: str | None = None) -> None:
        # with open(filename, "w") as f:
            # json.dump(data, f)

# class GCSStorage(RawStorage):
    # def save(self, data: Any | None = None, filename: str | None = None) -> None:
        # raise NotImplementedError


# def get_storage(config) -> RawStorage:
    # if config["storage"]["type"] == "local":
        # return LocalStorage()
    # else:
        # return GCSStorage()


from abc import ABC, abstractmethod
from typing import Dict
import os
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
#from ... import IngestionRun

class Storage(ABC):
    
    @abstractmethod
    def write_dataframe(
        self,
        df,
        path: str,
        mode: str,
        partition_cols: list[str] | None = None
    ) -> None:
        pass 
    
    @abstractmethod
    def write_jsonl(
        self,
        records,
        path: str
    ) -> int:
        pass


# class LocalStorage(Storage):
    
    # def write_dataframe(self, df, path, mode, partition_cols=None):
        # writer = df.write.mode(mode)
        # if partition_cols:
            # writer = writer.partitionBy(*partition_cols)
        # writer.parquet(path)
    
    # def write_jsonl(self, records, path):
        # count = 0
        # with open(path, "w", encoding="utf-8") as f:
            # for record in records:
                # f.write(json.dumps(record) + "\n")
                # count += 1
        # return count

class LocalStorage(Storage):
    
    def write_dataframe(self, df, path, mode, partition_cols=None):
        writer = df.write.mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(path)
    
    def write_jsonl(self, records, path: Path) -> int: 
        
        path.parent.mkdir(parents=True, exist_ok=True)
        
        count = 0
        with NamedTemporaryFile("w", delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)
            
            for record in records:
                tmp.write(json.dumps(record) + "\n")
                count += 1
        
        os.replace(tmp_path, path)
         
        return count

def get_storage(storage_type: str | None) -> Storage:
    
    #storage_config = config["storage"]["type"]
    if not storage_type:
        raise ValueError("Storage settings not configured.")
        
    if storage_type == "local":
        return LocalStorage()
    else:
        return GCStorage()
