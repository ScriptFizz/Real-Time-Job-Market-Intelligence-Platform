import pytest
from job_plat.utils.helpers import assert_df_equality
from pyspark.sql import SparkSession
from job_plat.transformations.silver.cleaning.clean_jobs import clean_jobs, deduplicate_jobs

@pytest.mark.parametrize(
    "input_row, expected",
    [(
        {
            "job_title_raw": "  Data Engineer  ",
            "description_raw": " a    description with  spaces ",
            "company_raw": " ACME ",
            "location_raw": " Berlin"
        },
        {
            "job_title": "data engineer",
            "company": "ACME",
            "description": " a description with spaces",
            "location": "Berlin"
        }
    ),
    (
        {
            "job_title_raw": "  ML sciENtist  ",
            "description_raw": " a    DEscriptIon wiTh  spaCes aND capitalization ",
            "company_raw": " Datalyst ",
            "location_raw": " Los Angeles"
        },
        {
            "job_title": "ml scientist",
            "company": "Datalyst",
            "location": "Los Angeles"
        }
    )
    
    
    ]
)
def test_clean_jobs(spark, input_row, expected):
    
    data = [(
        input_row["job_title_raw"],
        input_row["description_raw"],
        input_row["company_raw"],
        input_row["location_raw"],
        "2024-01-01T10:00:00+00:00",
        "linkedin",
        "123",
        "http://example.com"
    )]

    df = spark.createDataFrame(data, [
        "job_title_raw",
        "description_raw",
        "company_raw",
        "location_raw",
        "scraped_at",
        "source",
        "job_id",
        "url"
    ])
    
    result = clean_jobs(df)
    row = result.first() #result.collect()[0]
    
    assert row["job_title"] == expected["job_title"]
    assert row["description"] == expected["description"]
    assert row["company"] == expected["company"]
    assert row["location"] == expected["location"]
    assert row["scraped_at"] is not None



def test_deduplicate_jobs(spark):
    
    input_data = [
        ("indeed", 1, "2026-03-02"),
        ("indeed", 1, "2026-03-05"),
        ("linkedin", 1, "2026-03-02"),
        ("linkedin", 1, "2026-03-10")
    ]
    
    df = spark.createDataFrame(input_data, ["source", "job_id", "ingested_at"])
    
    result_df = deduplicate_jobs(df)
    
    expected_data = [
        ("indeed", 1, "2026-03-02"),
        ("linkedin", 1, "2026-03-02")
    ]
    
    expected_df = spark.createDataFrame(expected_data, ["source", "job_id", "ingested_at"])
    
    assert_df_equality(result_df, expected_df)
