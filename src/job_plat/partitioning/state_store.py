from abc import ABC, abstractmethod
from typing import TypeAlias

from job_plat.storage.paths import join_storage_path
from job_plat.storage.storages import GCStorage, LocalStorage, Storage

PartitionState: TypeAlias = dict[str, list[str]]


class StateStore(ABC):
    @abstractmethod
    def load(self) -> PartitionState:
        raise NotImplementedError

    @abstractmethod
    def save(self, state: PartitionState) -> None:
        raise NotImplementedError


class StorageStateStore(StateStore):
    def __init__(
        self,
        storage: Storage,
        metadata_path: str,
    ):
        self.storage = storage
        self.state_path = join_storage_path(
            metadata_path,
            "partitions_metadata.json",
        )

    def load(self) -> PartitionState:
        payload = self.storage.read_json(self.state_path)

        if payload is None:
            return {}

        state: PartitionState = {}

        for stage_name, values in payload.items():
            if not isinstance(stage_name, str):
                raise ValueError("Partition-state keys must be strings")

            if not isinstance(values, list) or not all(
                isinstance(value, str) for value in values
            ):
                raise ValueError(f"Invalid partition state for stage {stage_name!r}")

            state[stage_name] = values

        return state

    def save(self, state: PartitionState) -> None:
        self.storage.write_json(state, self.state_path)


class LocalStateStore(StorageStateStore):
    def __init__(self, metadata_path: str):
        super().__init__(storage=LocalStorage(), metadata_path=metadata_path)


class GCSStateStore(StorageStateStore):
    def __init__(
        self,
        storage: GCStorage,
        metadata_path: str,
    ):
        super().__init__(
            storage=storage,
            metadata_path=metadata_path,
        )


def get_state_store(
    *,
    storage_type: str,
    metadata_path: str,
    storage: Storage,
) -> StateStore:
    if storage_type == "local":
        return LocalStateStore(metadata_path)

    if storage_type == "gcs":
        if not isinstance(storage, GCStorage):
            raise TypeError("GCS state requires a GCStorage instance")

        return GCSStateStore(
            storage=storage,
            metadata_path=metadata_path,
        )

    raise ValueError(f"Unsupported state storage type: {storage_type}")
