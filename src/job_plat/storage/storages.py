import json
import shutil
from abc import ABC, abstractmethod
from collections.abc import Iterable
from fnmatch import fnmatchcase
from importlib import import_module
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from pyspark.sql import DataFrame, SparkSession


class Storage(ABC):
    @abstractmethod
    def read_parquet(
        self,
        spark: SparkSession,
        base_path: str,
        paths: list[str],
    ) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def read_jsonl(
        self,
        spark: SparkSession,
        base_path: str,
        paths: list[str],
    ) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def write_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        partition_cols: list[str] | None = None,
        dynamic_partition_overwrite: bool = False,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def write_dataframe_json(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        partition_cols: list[str] | None = None,
        dynamic_partition_overwrite: bool = False,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def write_jsonl(self, records: Iterable[dict[str, Any]], path: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def list_dirs(self, path: str, pattern: str) -> list[str]:
        """
        Return sorted, unique, immediate child directories whose
        names match pattern.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Return whether path identifies an existing object or a
        non-empty dataset prefix.
        """
        raise NotImplementedError

    @abstractmethod
    def read_json(self, path: str) -> dict[str, Any] | None:
        """Read one JSON object, or return None when it does not exist."""
        raise NotImplementedError

    @abstractmethod
    def write_json(
        self,
        payload: dict[str, Any],
        path: str,
    ) -> None:
        """Write one JSON object."""
        raise NotImplementedError


class LocalStorage(Storage):
    def read_parquet(
        self, spark: SparkSession, base_path: str, paths: list[str]
    ) -> DataFrame:
        return spark.read.option("basePath", base_path).parquet(*paths)

    def write_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        partition_cols: list[str] | None = None,
        dynamic_partition_overwrite: bool = False,
    ) -> None:
        writer = df.write

        if dynamic_partition_overwrite:
            writer = writer.option(
                "partitionOverwriteMode",
                "dynamic",
            )

        writer = writer.mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(path)

    def read_jsonl(
        self, spark: SparkSession, base_path: str, paths: list[str]
    ) -> DataFrame:
        return (
            spark.read.option("basePath", base_path)
            .option("multiLine", False)
            .json(paths)
        )

    def write_jsonl(self, records: Iterable[dict[str, Any]], path: str) -> int:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        count = 0
        with NamedTemporaryFile("w", delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)

            for record in records:
                tmp.write(json.dumps(record) + "\n")
                count += 1

        shutil.move(str(tmp_path), str(destination))

        return count

    def write_dataframe_json(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        partition_cols: list[str] | None = None,
        dynamic_partition_overwrite: bool = False,
    ) -> None:
        writer = df.write.option("compression", "none")

        if dynamic_partition_overwrite:
            writer = writer.option(
                "partitionOverwriteMode",
                "dynamic",
            )

        writer = writer.mode(mode)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.json(path)

    def list_dirs(self, path: str, pattern: str) -> list[str]:
        return sorted(
            {
                str(candidate)
                for candidate in Path(path).glob(pattern)
                if candidate.is_dir()
            }
        )

    def exists(self, path: str) -> bool:
        return Path(path).exists()

    def read_json(self, path: str) -> dict[str, Any] | None:
        source = Path(path)

        if not source.exists():
            return None

        payload = json.loads(source.read_text(encoding="utf-8"))

        if not isinstance(payload, dict):
            raise ValueError(f"Expected JSON object at {path}")

        return payload

    def write_json(
        self,
        payload: dict[str, Any],
        path: str,
    ) -> None:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        temporary_path: Path | None = None
        try:
            with NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=destination.parent,
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                json.dump(payload, temporary, indent=2, sort_keys=True)
                temporary.flush()

            temporary_path.replace(destination)
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()


###############
### GCS Storage
################


class GCStorage(Storage):
    def __init__(self):
        try:
            storage_module = import_module("google.cloud.storage")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "GCS support is not installed. "
                "Install it with `poetry install --with cloud`."
            ) from exc
        self.client = storage_module.Client()

    @staticmethod
    def _split_gcs_path(path: str) -> tuple[str, str]:
        if not path.startswith("gs://"):
            raise ValueError("GCStorage requires gs:// path")

        _, rest = path.split("gs://", 1)
        bucket_name, separator, object_path = rest.partition("/")

        if not bucket_name:
            raise ValueError("GCS path must include a bucket")

        if not separator or not object_path.strip("/"):
            raise ValueError("GCS path must include a bucket and object path")

        return bucket_name, object_path.strip("/")

    def read_parquet(
        self, spark: SparkSession, base_path: str, paths: list[str]
    ) -> DataFrame:
        return spark.read.option("basePath", base_path).parquet(*paths)

    def write_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        partition_cols: list[str] | None = None,
        dynamic_partition_overwrite: bool = False,
    ) -> None:
        writer = df.write

        if dynamic_partition_overwrite:
            writer = writer.option(
                "partitionOverwriteMode",
                "dynamic",
            )

        writer = writer.mode(mode)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.parquet(path)

    def read_jsonl(
        self, spark: SparkSession, base_path: str, paths: list[str]
    ) -> DataFrame:
        return (
            spark.read.option("basePath", base_path)
            .option("multiLine", False)
            .json(paths)
        )

    def write_dataframe_json(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        partition_cols: list[str] | None = None,
        dynamic_partition_overwrite: bool = False,
    ) -> None:
        writer = df.write.option("compression", "none")

        if dynamic_partition_overwrite:
            writer = writer.option(
                "partitionOverwriteMode",
                "dynamic",
            )

        writer = writer.mode(mode)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.json(path)

    def write_jsonl(self, records: Iterable[dict[str, Any]], path: str) -> int:
        bucket_name, blob_path = self._split_gcs_path(path)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        count = 0
        lines = []

        for record in records:
            lines.append(json.dumps(record))
            count += 1

        blob.upload_from_string("\n".join(lines))

        return count

    def list_dirs(self, path: str, pattern: str) -> list[str]:
        bucket_name, object_path = self._split_gcs_path(path)
        bucket = self.client.bucket(bucket_name)
        parent_prefix = object_path.rstrip("/") + "/"

        blob_iterator = self.client.list_blobs(
            bucket,
            prefix=parent_prefix,
            delimiter="/",
        )

        matching_prefixes: set[str] = set()

        for page in blob_iterator.pages:
            for child_prefix in page.prefixes:
                normalized_prefix = child_prefix.rstrip("/")
                child_name = normalized_prefix.rsplit("/", 1)[-1]

                if fnmatchcase(child_name, pattern):
                    matching_prefixes.add(f"gs://{bucket_name}/{normalized_prefix}")

        return sorted(matching_prefixes)

    def exists(self, path: str) -> bool:
        bucket_name, object_path = self._split_gcs_path(path)
        bucket = self.client.bucket(bucket_name)
        exact_blob = bucket.blob(object_path)

        if exact_blob.exists(client=self.client):
            return True

        normalized_prefix = object_path.rstrip("/") + "/"

        blobs = self.client.list_blobs(
            bucket,
            prefix=normalized_prefix,
            max_results=1,
        )

        return next(iter(blobs), None) is not None

    def _resolve_blob(self, path: str):
        bucket_name, blob_path = self._split_gcs_path(path)
        bucket = self.client.bucket(bucket_name)
        return bucket.blob(blob_path)

    def read_json(self, path: str) -> dict[str, Any] | None:
        blob = self._resolve_blob(path)

        if not blob.exists(client=self.client):
            return None

        payload = json.loads(blob.download_as_text())

        if not isinstance(payload, dict):
            raise ValueError(f"Expected JSON object at {path}")

        return payload

    def write_json(
        self,
        payload: dict[str, Any],
        path: str,
    ) -> None:
        blob = self._resolve_blob(path)
        blob.upload_from_string(
            json.dumps(payload, indent=2, sort_keys=True),
            content_type="application/json",
        )


def get_storage(storage_type: str | None) -> Storage:
    # storage_config = config["storage"]["type"]
    if not storage_type:
        raise ValueError("Storage settings not configured.")

    if storage_type == "local":
        return LocalStorage()
    elif storage_type == "gcs":
        return GCStorage()
    else:
        raise ValueError(f"Type of storage {storage_type} is not recognized")
