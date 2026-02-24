from urllib.parse import urlencode

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
    country: str = "us") -> IndeedJobSource:
        
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
    
    base_url = domains.get(country.lower())
    if not base_url:
        errors.append(f"Unsupported country: {country}")
    
    if errors:
        raise ValueError(f"Error in building the url: {', '.join(errors)}")
    
    params = {
        "q": query,
        "l": location
    }
    search_url = f"{base}/jobs?{urlencode(params)}"
    
    return IndeedJobSource(
        name="indeed",
        base_url=base_url,
        search_url=search_url,
        ready_selector='[data-testid="job-card"], :has-text("No jobs found")',
    )


def build_linkedin_source(
    query: str,
    location: str,
    country: str = "us") -> LinkedInJobSource:
        
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
    
    base_url = domains.get(country.lower())
    if not base_url:
        errors.append(f"Unsupported country: {country}")
    
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



# def build_indeed_url(
    # query: str,
    # location: str,
    # country: str = "us") -> str:
    # """
    
    # """
    # errors = []
    # if not query:
        # errors.append("Missing job role")
    # if not location:
        # errors.append("Missing location")
    
    # base = INDEED_DOMAINS.get(country.lower())
    # if not base:
        # errors.append(f"Unsupported country: {country}")
    
    # if errors:
        # raise ValueError(f"Error in building the url: {', '.join(errors)}")
    
    # params = {
        # "q": query,
        # "l": location
    # }
    # return f"{base}/jobs?{urlencode(params)}"


# def build_linkedin_url(
    # query: str,
    # location: str
    # ) -> str:
    # """
    
    # """
    # missing = []
    # if not query:
        # missing.append("Missing job role")
    # if not location:
        # missing.append("Missing location")
    
    # if missing:
        # raise ValueError(f"Incomplete data: {', '.join(missing)}")
    
    # params = {
        # "q": query,
        # "l": location
    # }
    # return f"https://www.linkedin.com/jobs/search?{urlencode(params)}"
