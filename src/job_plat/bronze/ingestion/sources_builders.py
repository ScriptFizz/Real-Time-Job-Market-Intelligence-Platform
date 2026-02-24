from urllib.parse import urlencode
from job_plat.bronze.ingestion.sources_config import IndeedJobSource, LinkedInJobSource

INDEED_DOMAINS = {
    "us": "https://www.indeed.com",
    "de": "https://de.indeed.com",
    "uk": "https://uk.indeed.com",
    "fr": "https://fr.indeed.com",
    "it": "https://it.indeed.com",
}

def build_indeed_source(
    query: str,
    location: str,
    domain: str = "us") -> IndeedJobSource:
        
    domains = {
    "us": "https://www.indeed.com",
    "de": "https://de.indeed.com",
    "uk": "https://uk.indeed.com",
    "fr": "https://fr.indeed.com",
    "it": "https://it.indeed.com",
    }   
    
    errors = []
    if not query:
        errors.append("Missing job role")
    if not location:
        errors.append("Missing location")
    
    base_url = domains.get(domain.lower())
    if not base_url:
        errors.append(f"Unsupported country: {domain}")
    
    if errors:
        raise ValueError(f"Error in building the url: {', '.join(errors)}")
    
    params = {
        "q": query,
        "l": location
    }
    search_url = f"{base_url}/jobs?{urlencode(params)}"
    
    return IndeedJobSource(
        name="indeed",
        base_url=base_url,
        search_url=search_url,
        ready_selector='[data-testid="job-card"], :has-text("No jobs found")',
    )


def build_linkedin_source(
    query: str,
    location: str,
    #domain: str = "us"
    ) -> LinkedInJobSource:
        
    errors = []
    if not query:
        errors.append("Missing job role")
    if not location:
        errors.append("Missing location")
    
    # base_url = domains.get(domain.lower())
    # if not base_url:
        # errors.append(f"Unsupported country: {domain}")
    
    if errors:
        raise ValueError(f"Error in building the url: {', '.join(errors)}")
    
    params = {
        "keywords": query,
        "location": location
    }
    search_url = f"https://www.linkedin.com/jobs/search?{urlencode(params)}"
    
    return LinkedInJobSource(
        name="linkedin",
        base_url="https://www.linkedin.com",
        search_url=search_url,
        ready_selector='div.base-card',
    )
