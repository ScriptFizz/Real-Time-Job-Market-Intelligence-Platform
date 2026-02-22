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
    
    data = [("  Data Engineer ", " a   description with  many    spaces", )]
    df = spark.createDataFrame(data, ["job_title_raw", "description_raw"])
    
    result = clean_jobs(df)
    roes = result.collect()
    
    assert rows[0]["title"] == "Data Engineer"
    assert rows[0]["description"] == "a description with many spaces"
