from job_plat.storage.paths import join_storage_path
from job_plat.storage.storages import Storage


def assert_storage_discovery_contract(
    *,
    storage: Storage,
    root: str,
) -> None:
    partition_path = join_storage_path(
        root,
        "ingestion_date=2025-03-01",
    )
    metadata_path = join_storage_path(
        partition_path,
        "_metadata.json",
    )

    assert not storage.exists(partition_path)

    storage.write_json(
        {"row_count": 1},
        metadata_path,
    )

    assert storage.exists(partition_path)
    assert storage.read_json(metadata_path) == {"row_count": 1}
    assert storage.list_dirs(
        path=root,
        pattern="ingestion_date=*",
    ) == [partition_path]
