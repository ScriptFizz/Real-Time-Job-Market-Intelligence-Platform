import pytest

from job_plat.storage import storages


def test_gcs_storage_explains_missing_optional_dependency(monkeypatch):
    def missing_module(name):
        raise ModuleNotFoundError(name)
    
    monkeypatch.setattr(storages, "import_module", missing_module)

    with pytest.raises(
        RuntimeError,
        match="poetry install --with cloud",
    ):
        storages.GCStorage()