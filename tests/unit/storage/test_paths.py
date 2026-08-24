import pytest

from job_plat.storage.paths import join_storage_path


@pytest.mark.parametrize(
    ("base", "parts", "expected"),
    [
        (
            "/tmp/job-platform",
            ("metadata", "state.json"),
            "/tmp/job-platform/metadata/state.json",
        ),
        (
            "gs://job-pipeline",
            ("metadata", "state.json"),
            "gs://job-pipeline/metadata/state.json",
        ),
        (
            "gs://job-pipeline/",
            ("/metadata/", "/state.json"),
            "gs://job-pipeline/metadata/state.json",
        ),
    ],
)
def test_join_storage_path(base, parts, expected):
    assert join_storage_path(base, *parts) == expected


def test_join_storage_path_rejects_empty_base():
    with pytest.raises(ValueError, match="must not be empty"):
        join_storage_path("", "state.json")
