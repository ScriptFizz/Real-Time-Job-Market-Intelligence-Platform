from job_plat.transformations.feature.embeddings.embedding_skill_normalizer import (
    EmbeddingSkillNormalizer,
)


def test_embedding_skill_normalizer_handles_empty_input():
    normalizer = object.__new__(EmbeddingSkillNormalizer)

    assert normalizer.normalize([]) == []


def test_choose_canonical_uses_shortest_lowercase_alias():
    normalizer = object.__new__(EmbeddingSkillNormalizer)

    result = normalizer._choose_canonical(["Apache Spark", "SPARK", "PySpark"])

    assert result == "spark"
