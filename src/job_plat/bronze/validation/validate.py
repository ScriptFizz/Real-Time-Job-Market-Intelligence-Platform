from pydantic import BaseModel, Field, HttpUrl, field_validator, ValidationError
from typing import Optional, Literal
from datetime import datetime
from typing import Iterator, Union
import logging

logger = logging.gerLogger(__name__)

JobSource = Literal["indeed", "linkedin"]


class JobBaseModel(BaseModel):
    source: JobSource
    job_id: str = Field(..., min_length=1)
    
    job_title_raw: Optional[str]
    company_raw: Optional[str]
    location_raw: Optional[str]
    description_raw: Optional[str]
    
    url: Optional[HttpUrl]
    scraped_at: datetime
    
    model_config = {
        "extra": "forbid" # detect schema drift, prevent unexpected fields
    }

class IndeedJob(JobBaseModel)
    source: Literal["indeed"]
    
class LinkedinJob(JobBaseModel):
    source: Literal["linkedin"]
    
    employment_type: Optional[str]
    seniority_level: Optional[str]
    industry: Optional[str]

BronzeJob = Union[IndeedJob, LinkedinJob]

MODEL_REGISTRY = {
    "indeed": IndeedJob,
    "linkedin": LinkedinJob,
}


def validate_jobs(records: Iterator[dict]) -> Iterator[dict]:
    valid = 0
    invalid = 0
    for record in records:
        try:
            # source = record.get("source")
            
            # if source == "indeed":
                # model = IndeedJob
            # elif source == "likedin":
                # model = LinkedinJob
            # else:
                # raise ValueError(f"unsupported source: {source}")
            
            model = MODEL_REGISTRY[record.get("source")]
                
            valid += 1
            yield model(**record).model_dump()
            
        except (ValidationError, ValueError) as e:
            invalid += 1
            logger.warning(
                "Validation failed", 
                extra={
                    "source": record.get("source")
                    "errors": str(e)
                    }
                )
    
    logger.info(
        "Validation summary",
        extra={"valid_records": valid, "invalid_records": invalid},
    )


