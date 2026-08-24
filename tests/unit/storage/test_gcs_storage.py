import pytest
from fixtures.storage_contract import assert_storage_discovery_contract

from job_plat.storage.storages import GCStorage


class FakeBlob:
    def __init__(self, client, bucket_name, name):
        self.client = client
        self.bucket_name = bucket_name
        self.name = name

    def exists(self, client):
        assert client is self.client
        return (self.bucket_name, self.name) in self.client.objects

    def upload_from_string(self, data, content_type=None):
        del content_type
        self.client.objects[(self.bucket_name, self.name)] = data

    def download_as_text(self):
        return self.client.objects[(self.bucket_name, self.name)]


class FakeBucket:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def blob(self, name):
        return FakeBlob(
            client=self.client,
            bucket_name=self.name,
            name=name,
        )


class FakePage:
    def __init__(self, prefixes):
        self.prefixes = prefixes


class FakeBlobIterator:
    def __init__(self, blobs, prefixes=()):
        self._blobs = blobs
        self.pages = [FakePage(prefixes)]

    def __iter__(self):
        return iter(self._blobs)


class FakeGCSClient:
    def __init__(self):
        self.objects = {}

    def bucket(self, name):
        return FakeBucket(self, name)

    def list_blobs(
        self,
        bucket,
        *,
        prefix,
        max_results=None,
        delimiter=None,
    ):
        matching_names = sorted(
            name
            for bucket_name, name in self.objects
            if bucket_name == bucket.name and name.startswith(prefix)
        )

        if delimiter is None:
            blobs = [
                FakeBlob(
                    client=self,
                    bucket_name=bucket.name,
                    name=name,
                )
                for name in matching_names[:max_results]
            ]
            return FakeBlobIterator(blobs)

        child_prefixes = set()

        for name in matching_names:
            remainder = name[len(prefix) :]

            if delimiter in remainder:
                child = remainder.split(delimiter, 1)[0]
                child_prefixes.add(f"{prefix}{child}{delimiter}")

        return FakeBlobIterator(
            blobs=[],
            prefixes=sorted(child_prefixes),
        )


def build_gcs_storage(client):
    storage = object.__new__(GCStorage)
    storage.client = client
    return storage


def test_gcs_exists_supports_exact_object():
    client = FakeGCSClient()
    storage = build_gcs_storage(client)

    storage.write_json(
        {"status": "complete"},
        "gs://job-platform/metadata/state.json",
    )

    assert storage.exists("gs://job-platform/metadata/state.json")


def test_gcs_exists_supports_dataset_prefix():
    client = FakeGCSClient()
    storage = build_gcs_storage(client)

    storage.write_json(
        {"row_count": 1},
        ("gs://job-platform/silver/jobs/ingestion_date=2025-03-01/_metadata.json"),
    )

    assert storage.exists("gs://job-platform/silver/jobs")


def test_gcs_exists_returns_false_for_missing_path():
    storage = build_gcs_storage(FakeGCSClient())

    assert not storage.exists("gs://job-platform/missing/dataset")


def test_gcs_list_dirs_returns_unique_immediate_prefixes():
    client = FakeGCSClient()
    storage = build_gcs_storage(client)

    paths = [
        (
            "gs://job-platform/bronze/jobs/"
            "ingestion_date=2025-03-02/"
            "source=adzuna/part-000.jsonl"
        ),
        (
            "gs://job-platform/bronze/jobs/"
            "ingestion_date=2025-03-01/"
            "source=adzuna/part-000.jsonl"
        ),
        (
            "gs://job-platform/bronze/jobs/"
            "ingestion_date=2025-03-01/"
            "source=usajobs/part-000.jsonl"
        ),
        ("gs://job-platform/bronze/jobs/unexpected=value/part-000.jsonl"),
    ]

    for path in paths:
        bucket_name, blob_name = storage._split_gcs_path(path)
        client.objects[(bucket_name, blob_name)] = "{}"

    assert storage.list_dirs(
        path="gs://job-platform/bronze/jobs",
        pattern="ingestion_date=*",
    ) == [
        "gs://job-platform/bronze/jobs/ingestion_date=2025-03-01",
        "gs://job-platform/bronze/jobs/ingestion_date=2025-03-02",
    ]


def test_gcs_satisfies_storage_discovery_contract():
    storage = build_gcs_storage(FakeGCSClient())

    assert_storage_discovery_contract(
        storage=storage,
        root="gs://job-platform/bronze/jobs",
    )


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/local-path",
        "gs://",
        "gs://job-platform",
        "gs://job-platform/",
    ],
)
def test_gcs_rejects_invalid_object_path(path):
    storage = build_gcs_storage(FakeGCSClient())

    with pytest.raises(ValueError):
        storage.exists(path)
