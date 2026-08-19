from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class CanonicalJobV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["adzuna", "usajobs"]
    source_job_id: str = Field(min_length=1)

    job_title_raw: str | None = None
    company_raw: str | None = None
    location_raw: str | None = None
    description_raw: str | None = None
    url: str | None = None

    employment_type_raw: str | None = None
    contract_type_raw: str | None = None

    salary_min_raw: float | None = None
    salary_max_raw: float | None = None
    currency_raw: str | None = None

    posted_at_raw: str | None = None
