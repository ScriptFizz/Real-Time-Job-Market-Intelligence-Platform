import pytest

from job_plat.storage import storages
from job_plat.storage.storages import LocalStorage


def test_gcs_storage_explains_missing_optional_dependency(monkeypatch):
    def missing_module(name):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(storages, "import_module", missing_module)

    with pytest.raises(
        RuntimeError,
        match="poetry install --with cloud",
    ):
        storages.GCStorage()


def test_local_storage_list_dirs_returns_materialized_strings(tmp_path):
    first = tmp_path / "ingestion_date=2025-03-01"
    second = tmp_path / "ingestion_date=2025-03-02"
    first.mkdir()
    second.mkdir()

    results = LocalStorage().list_dirs(
        path=str(tmp_path),
        pattern="ingestion_date=*",
    )

    assert results == [str(first), str(second)]
    assert all(isinstance(result, str) for result in results)
