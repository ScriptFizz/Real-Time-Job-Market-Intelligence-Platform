import pytest
from pyspark.sql import SparkSession
from job_plat.silver.cleaning.clean_jobs import clean_jobs

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("test")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_clean_jobs(spark):
    
    data = [(
        "  Data Engineer ",
        " a   description with  many    spaces",
        "  ACME  ",
        "  Berlin  ",
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
    rows = result.collect()
    
    assert rows[0]["job_title"] == "data engineer"
    assert rows[0]["description"] == " a description with many spaces"
    assert rows[0]["company"] == "ACME"
    assert rows[0]["location"] == "Berlin"
    assert rows[0]["scraped_at"] is not None
