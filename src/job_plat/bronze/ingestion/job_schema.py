from pydantic import BaseModel
from typing import Optional

class CanonicalJobV1(BaseModel):
    
    source: str
    source_job_id: str
    
    job_title_raw: Optional[str]
    company_raw: Optional[str]
    location_raw: Optional[str]
    description_raw: Optional[str]
    url: Optional[str]
    
    employment_type_raw: Optional[str]
    contract_type_raw: Optional[str]
    
    salary_min_raw: Optional[float]
    salary_max_raw: Optional[float]
    currency_raw: Optional[str]
    
    posted_at_raw: Optional[str]
