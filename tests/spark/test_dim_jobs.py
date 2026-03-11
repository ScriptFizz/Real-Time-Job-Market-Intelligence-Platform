import pytest
from pyspark.sql import SparkSession
from job_plat.transformations.gold.v1_analytics.build_dimensions import build_dim_jobs

@pytest.mark.parametrize(
    "input_row", "expected",
    [(
        {
            "job_id": "  123  ",
            "source": "linkedin",
            "job_title": "data engineer ",
            "company": "ACME",
            "location": "Berlin",
            "ingestion_date": "05/03/2026",
            "posted_at": "03/03/2026",
            "description": "this is not an empty description"
        },
        {
            "job_id": "  123  ",
            "source": " a    description with  spaces ",
            "job_title": " ACME ",
            "company": "ACME",
            "location": "Berlin",
            "ingestion_date": ,
            "posted_at" "",
            "description_length": len("this is not an empty description")
        }
    ),
    (
        {
            "job_title_raw": "  ML sciENtist  ",
            "description_raw": " a    DEscriptIon wiTh  spaCes aND capitalization ",
            "company_raw": " Datalyst ",
            "description": " a description with spaces and capitalization",
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
def test_build_dim_jobs(spark, input_row, expected):
    
    data = [(
            input_row["job_id"],
            input_row["source"],
            input_row["job_title"],
            input_row["company"],
            input_row["location"],
            input_row["ingestion_date"],
            input_row["posted_at"],
            input_row["description"]
    )]

    df = spark.createDataFrame(data, [
            "job_id",
            "source",
            "job_title",
            "company",
            "location",
            "ingestion_date",
            "posted_at",
            "description"
    ])
    
    result = clean_jobs(df)
    rows = result.collect()
    
    assert row["job_title"] == expected["job_title"]
    assert row["description"] == expected["description"]
    assert row["company"] == expected["company"]
    assert row["location"] == expected["location"]
    assert row["scraped_at"] is not None
