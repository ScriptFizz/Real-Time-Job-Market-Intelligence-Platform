from dataclasses import dataclass

@dataclass
class JobSearchCriteria:
    query: str
    country: str
    location: str | None = None


# @dataclass
# class JobSearchCriteria:
    # query: str
    # location: str | None = None
