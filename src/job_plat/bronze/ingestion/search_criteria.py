from dataclasses import dataclass

@dataclass
class JobSearchCriteria:
    query: str
    location: str | None = None
