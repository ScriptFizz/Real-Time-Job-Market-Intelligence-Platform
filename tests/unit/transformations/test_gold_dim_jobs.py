import pytest
from job_plat.utils.helpers import assert_df_equality
from pyspark.sql import SparkSession
from job_plat.transformations.gold.v1_analytics.build_dimensions import build_dim_jobs, build_dim_skills


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
    
    df = spar.createDataFrame(
        [(1, "linkedin", "title", "comp", "loc", "2026-01-01", "2026-01-01", description)],
        ["job_id","source","job_title","company","location","ingestion_date","posted_at","description"]
    ) 
    
    result = build_dim_jobs(df).collect()[0]
    
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

    expected_df = spark.createDataFrame(expected_data ["skills", "ingestion_date"])\
        .withColumn("skill_id", sha2(col("skills"), 256))
    
    assert_df_equality(result_df, expected_df)




################################


# @pytest.mark.parametrize(
    # "input_rows", "expected",
    # [(
        # [{
            # "job_id": 123,
            # "source": "linkedin",
            # "job_title": "data engineer ",
            # "company": "ACME",
            # "location": "Berlin",
            # "ingestion_date": "05/03/2026",
            # "posted_at": "03/03/2026",
            # "description": "this is not an empty description"
        # },
        # {
        
            # "job_id": 456,
            # "source": "indeed",
            # "job_title": "ml engineer ",
            # "company": "Google",
            # "location": "Austin",
            # "ingestion_date": "06/03/2026",
            # "posted_at": "02/03/2026",
            # "description": "this is not an empty description"
        # },
        # {
            # "job_id": 123,
            # "source": "linkedin",
            # "job_title": "data engineer",
            # "company": "ACME S.p.a.",
            # "location": "Berlin",
            # "ingestion_date": "08/03/2026",
            # "posted_at": "05/03/2026",
            # "description": "this is not an empty description too"
        # }
        
        # ],
        # [
        # {
            # "job_id": 123,
            # "source": " a    description with  spaces ",
            # "job_title": " ACME ",
            # "company": "ACME",
            # "location": "Berlin",
            # "ingestion_date": ,
            # "posted_at" "",
            # "description_length": len("this is not an empty description")
        # },
        # {
        
            # "job_id": 456,
            # "source": "indeed",
            # "job_title": "ml engineer ",
            # "company": "Google",
            # "location": "Austin",
            # "ingestion_date": "06/03/2026",
            # "posted_at": "02/03/2026",
            # "description": "this is not an empty description"
        # }
        # ]
    # )
    
    # ]
# )
# def test_build_dim_jobs(spark, input_rows, expected):
    
    # data = [(
            # row["job_id"],
            # row["source"],
            # row["job_title"],
            # row["company"],
            # row["location"],
            # row["ingestion_date"],
            # row["posted_at"],
            # row["description"]
    # ) for row in input_rows]

    # df = spark.createDataFrame(data, [
            # "job_id",
            # "source",
            # "job_title",
            # "company",
            # "location",
            # "ingestion_date",
            # "posted_at",
            # "description"
    # ])
    
    # result = build_dim_jobs(df)
    # rows = result.collect()
    
    # assert len(rows) == 2
    # assert all(x >= 0 for x in rows["description_length"])
