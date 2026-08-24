from collections.abc import Sequence
from datetime import datetime
from typing import Any

import numpy as np
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from sentence_transformers import SentenceTransformer

SKILL_EMBEDDING_SCHEMA = StructType(
    [
        StructField("skill_id", StringType(), False),
        StructField("skills", StringType(), False),
        StructField("embedding", ArrayType(DoubleType(), False), False),
        StructField("embedding_dim", IntegerType(), False),
        StructField("model_name", StringType(), False),
        StructField("model_version", StringType(), False),
        StructField("model_provider", StringType(), False),
        StructField("generated_at", TimestampType(), False),
        StructField("is_active", BooleanType(), False),
    ]
)


def _batches(values: Sequence[str], batch_size: int) -> list[Sequence[str]]:
    return [
        values[index : index + batch_size]
        for index in range(0, len(values), batch_size)
    ]


def build_skill_embeddings(
    dim_skills_df: DataFrame,
    spark: SparkSession,
    *,
    generated_at: datetime,
    existing_embeddings_df: DataFrame | None = None,
    model_name: str = "all-MiniLM-L6-v2",
    model_version: str = "v1",
    model_provider: str = "sentence-transformers",
    batch_size: int = 128,
    max_driver_skills: int = 50_000,
) -> DataFrame:
    """Embed only missing skill/version keys with a bounded driver workload."""
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if max_driver_skills <= 0:
        raise ValueError("max_driver_skills must be greater than zero")

    candidates = dim_skills_df.select("skill_id", "skills").dropDuplicates(["skill_id"])
    if existing_embeddings_df is not None:
        existing_keys = (
            existing_embeddings_df.filter(F.col("model_version") == model_version)
            .select("skill_id")
            .distinct()
        )
        candidates = candidates.join(existing_keys, "skill_id", "left_anti")

    rows = candidates.orderBy("skill_id").limit(max_driver_skills + 1).collect()
    if len(rows) > max_driver_skills:
        raise ValueError(
            "Skill embedding cardinality exceeds the configured driver limit: "
            f"max_driver_skills={max_driver_skills}"
        )
    if not rows:
        return spark.createDataFrame([], SKILL_EMBEDDING_SCHEMA)

    model = SentenceTransformer(model_name)
    skill_names = [str(row.skills) for row in rows]
    encoded_batches: list[np.ndarray[Any, Any]] = []
    for batch in _batches(skill_names, batch_size):
        encoded = np.asarray(
            model.encode(
                list(batch),
                batch_size=batch_size,
                show_progress_bar=False,
            ),
            dtype=float,
        )
        if encoded.ndim != 2 or encoded.shape[0] != len(batch):
            raise ValueError("Embedding model returned an unexpected output shape")
        encoded_batches.append(encoded)

    embeddings = np.concatenate(encoded_batches, axis=0)
    embedding_dim = int(embeddings.shape[1])
    records = [
        (
            str(row.skill_id),
            str(row.skills),
            embedding.tolist(),
            embedding_dim,
            model_name,
            model_version,
            model_provider,
            generated_at,
            True,
        )
        for row, embedding in zip(rows, embeddings, strict=True)
    ]
    return spark.createDataFrame(records, SKILL_EMBEDDING_SCHEMA)
