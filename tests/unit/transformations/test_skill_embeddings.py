from datetime import UTC, datetime

import numpy as np
import pytest

from job_plat.transformations.feature.embeddings.build_skill_embeddings import (
    build_skill_embeddings,
)

GENERATED_AT = datetime(2025, 3, 2, tzinfo=UTC)


class FakeEncoder:
    def encode(self, skills, batch_size=128, show_progress_bar=True):
        del batch_size
        del show_progress_bar
        return np.ones((len(skills), 384))


def test_embedding_dimension(gold_dim_skills_data, spark, monkeypatch):
    monkeypatch.setattr(
        "job_plat.transformations.feature.embeddings.build_skill_embeddings.SentenceTransformer",
        lambda _name: FakeEncoder(),
    )

    result = build_skill_embeddings(
        dim_skills_df=gold_dim_skills_data, spark=spark, generated_at=GENERATED_AT
    )

    row = result.first()
    assert len(row.embedding) == 384
    assert row.embedding_dim == 384


def test_embeddings_are_batched_and_model_is_loaded_once(spark, monkeypatch):
    dataframe = spark.createDataFrame(
        [(f"skill-{index}", f"skill {index}") for index in range(5)],
        ["skill_id", "skills"],
    )
    encoder = FakeEncoder()
    encoded_batch_sizes: list[int] = []

    def encode(skills, batch_size=128, show_progress_bar=True):
        del batch_size, show_progress_bar
        encoded_batch_sizes.append(len(skills))
        return np.ones((len(skills), 4))

    encoder.encode = encode
    model_loads: list[str] = []

    def load_model(name):
        model_loads.append(name)
        return encoder

    monkeypatch.setattr(
        "job_plat.transformations.feature.embeddings.build_skill_embeddings."
        "SentenceTransformer",
        load_model,
    )

    result = build_skill_embeddings(
        dataframe,
        spark,
        generated_at=GENERATED_AT,
        batch_size=2,
    )

    assert result.count() == 5
    assert encoded_batch_sizes == [2, 2, 1]
    assert model_loads == ["all-MiniLM-L6-v2"]


def test_embedding_cardinality_guard_fails_before_loading_model(
    spark,
    monkeypatch,
):
    dataframe = spark.createDataFrame(
        [("skill-1", "one"), ("skill-2", "two")],
        ["skill_id", "skills"],
    )
    model_loader = pytest.fail
    monkeypatch.setattr(
        "job_plat.transformations.feature.embeddings.build_skill_embeddings."
        "SentenceTransformer",
        model_loader,
    )

    with pytest.raises(ValueError, match="driver limit"):
        build_skill_embeddings(
            dataframe,
            spark,
            generated_at=GENERATED_AT,
            max_driver_skills=1,
        )


def test_only_missing_skill_version_keys_are_embedded(spark, monkeypatch):
    dataframe = spark.createDataFrame(
        [("skill-1", "one"), ("skill-2", "two")],
        ["skill_id", "skills"],
    )
    existing = spark.createDataFrame(
        [("skill-1", "v1")],
        ["skill_id", "model_version"],
    )
    encoded_skills: list[str] = []
    encoder = FakeEncoder()

    def encode(skills, batch_size=128, show_progress_bar=True):
        del batch_size, show_progress_bar
        encoded_skills.extend(skills)
        return np.ones((len(skills), 4))

    encoder.encode = encode
    monkeypatch.setattr(
        "job_plat.transformations.feature.embeddings.build_skill_embeddings."
        "SentenceTransformer",
        lambda _name: encoder,
    )

    result = build_skill_embeddings(
        dataframe,
        spark,
        generated_at=GENERATED_AT,
        existing_embeddings_df=existing,
        model_version="v1",
    )

    assert [row.skill_id for row in result.collect()] == ["skill-2"]
    assert encoded_skills == ["two"]
