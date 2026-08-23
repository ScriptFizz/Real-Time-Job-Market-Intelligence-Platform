from datetime import UTC, datetime

import numpy as np

from job_plat.transformations.feature.embeddings.build_skill_embeddings import (
    build_skill_embeddings,
)

GENERATED_AT = datetime(2025, 3, 2, tzinfo=UTC)


class FakeEncoder:
    def encode(self, skills, show_progress_bar=True):
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
