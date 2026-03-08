from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType, StringType, MapType, FloatType
)

from job_plat.transformations.silver.enrichment.extract_skills import (
    extract_skills,
    normalize_skills,
    skill_confidence
)

extract_skills_udf = udf(
    extract_skills, 
    ArrayType(StringType())
    )

normalize_skills_udf = udf(
    normalize_skills, 
    ArrayType(StringType())
    )

skill_confidence_udf = udf(
    skill_confidence,
    MapType(StringType(), FloatType())
)
