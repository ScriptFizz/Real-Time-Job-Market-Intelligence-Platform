import pytest
from job_plat.utils.helpers import assert_df_equality
from pyspark.sql import SparkSession
from job_plat.transformations.gold.v1_analytics.build_dimensions import build_dim_jobs, build_dim_skills
from pyspark.sql.functions import (
    col,
    sha2
)


def test_build_dim_jobs(spark):
    
    input_data = [
        (123, "linkedin", "data engineer", "ACME", "Berlin", "2026-03-05", "2026-03-03", "desc1"),
        (456, "indeed", "ml engineer", "Google", "Austin", "2026-03-06", "2026-03-02", "desc2"),
        (123, "linkedin", "data engineer", "ACME", "Berlin", "2026-03-08", "2026-03-05", "desc3"),
    ]

    df = spark.createDataFrame(
        input_data, 
        ["job_id", "source", "job_title", "company", "location", "ingestion_date", "posted_at", "description"]
        )
    
    result_df = build_dim_jobs(df)

    expected_data = [
        (123, "linkedin", "data engineer", "ACME", "Berlin", "2026-03-05", "2026-03-03", len("desc1")),
        (456, "indeed", "ml engineer", "Google", "Austin", "2026-03-06", "2026-03-02", len("desc2")),
    ]
    
    expected_df = spark.createDataFrame(
        expected_data,
        ["job_id","source","job_title","company","location","ingestion_date","posted_at","description_length"]
    )
    
    assert_df_equality(result_df, expected_df)


@pytest.mark.parametrize("description,expected_len",
    [
        ("12345", 5),
        ("", 0),
        ("12345678901234567", 17)
    ]
)
def test_description_length(spark, description, expected_len):
    
    df = spark.createDataFrame(
        [(1, "linkedin", "title", "comp", "loc", "2026-01-01", "2026-01-01", description)],
        ["job_id","source","job_title","company","location","ingestion_date","posted_at","description"]
    ) 
    
    result =  build_dim_jobs(df).first() 
    
    assert result["description_length"] == expected_len 
    
def test_build_dim_skills(spark):
    
    input_data = [
        ("gcp", "2026-02-03"),
        ("python", "2026-11-04"),
        ("gcp", "2026-03-06"),
        ("docker", "2026-02-02")
    ]
    
    df = spark.createDataFrame(input_data, ["skills", "ingestion_date"])
    
    result_df = build_dim_skills(df)
    
    expected_data = [
        ("gcp", "2026-02-03"),
        ("python", "2026-11-04"),
        ("docker", "2026-02-02")
    ]

    expected_df = spark.createDataFrame(expected_data, ["skills", "ingestion_date"])\
        .withColumn("skill_id", sha2(col("skills"), 256))
    
    assert_df_equality(result_df, expected_df)
