import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)


@pytest.fixture
def job_bronze_df(spark):

    schema = StructType([
        StructField(
            "payload",
            StructType([
                StructField("source", StringType(), True),
                StructField("source_job_id", IntegerType(), True),
                StructField("job_title_raw", StringType(), True),
                StructField("company_raw", StringType(), True),
                StructField("location_raw", StringType(), True),
                StructField("description_raw", StringType(), True),
                StructField("url", StringType(), True),
                StructField("employment_type_raw", StringType(), True),
                StructField("contract_type_raw", StringType(), True),
                StructField("salary_min_raw", IntegerType(), True),
                StructField("salary_max_raw", IntegerType(), True),
                StructField("currency_raw", StringType(), True),
                StructField("posted_at_raw", StringType(), True),
            ])
        ),
        StructField("ingestion_date", StringType(), True),
        StructField("run_id", IntegerType(), True),
        StructField(
            "ingestion_metadata",
            StructType([
                StructField("started_at", StringType(), True)
            ])
        )
    ])

    data = [
        (
            {
                "source": "linkedin",
                "source_job_id": 2,
                "job_title_raw": "  Data Engineer ",
                "company_raw": " ACME ",
                "location_raw": " Berlin",
                "description_raw": " a    python with  docker  and sql ",
                "url": "http://myurl.co",
                "employment_type_raw": "full-time",
                "contract_type_raw": "indeterminate",
                "salary_min_raw": 12000,
                "salary_max_raw": 19000,
                "currency_raw": "euro",
                "posted_at_raw": "2026-03-01"
            },
            "2026-03-01",
            123,
            {"started_at": "2026-03-01T10:00:00+00:00"}
        )
    ]

    return spark.createDataFrame(data, schema)

    
@pytest.fixture
def silver_jobs_df(spark):
    
    data = [
        (1, "linkedin", "data engineer", "ACME", "Berlin", "2026-03-01", "2026-03-01", "desc one"),
        (1, "linkedin", "data engineer", "ACME", "Berlin", "2026-03-02", "2026-03-02", "desc two"),
        (2, "indeed", "ml engineer", "Google", "Austin", "2026-03-01", "2026-03-01", "desc three"),
    ]
    
    columns = [
        "job_id",
        "source",
        "job_title",
        "company",
        "location",
        "ingestion_date",
        "posted_at",
        "description",
    ]
    
    return spark.createDataFrame(data, columns)


@pytest.fixture
def silver_job_skills_df(spark):

    data = [
        (1, "python", 0.95, "2026-03-01T10:00:00", "2026-03-01", "2026-03-01"),
        (1, "spark", 0.90, "2026-03-01T10:00:00", "2026-03-01", "2026-03-01"),
        (2, "python", 0.80, "2026-03-02T10:00:00", "2026-03-02", "2026-03-02"),
        (2, "docker", 0.85, "2026-03-02T10:00:00", "2026-03-02", "2026-03-02"),
    ]

    columns = [
        "job_id",
        "skills",
        "skill_confidence",
        "processed_at",
        "ingestion_date",
        "posted_at",
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def gold_dim_skills_df(spark):
    data = [
         (1, "python", "2026-03-01"),
        (2, "spark", "2026-03-01"),
        (3, "aws", "2026-03-02"),
        (4, "docker", "2026-03-02")
    ]

    columns = ["skill_id", "skills", "ingestion_date"]
    
    return spark.createDataFrame(data, columns)
