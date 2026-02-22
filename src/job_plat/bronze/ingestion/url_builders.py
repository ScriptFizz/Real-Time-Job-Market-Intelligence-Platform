from urllib.parse import urlencode

def build_indeed_url(
    query str,
    location: str
    ) -> str:
    """
    
    """
    missing = []
    if not query:
        missing.append("Missing job role")
    if not location:
        missing.append("Missing location")
    
    if missing:
        raise ValueError(f"Incomplete data: {', '.join(missing)}")
    
    params = {
        "q": query,
        "l": location
    }
    return f"https://www.indeed.com/jobs?{urlencode(params)}"


def build_linkedin_url(
    query str,
    location: str
    ) -> str:
    """
    
    """
    missing = []
    if not query:
        missing.append("Missing job role")
    if not location:
        missing.append("Missing location")
    
    if missing:
        raise ValueError(f"Incomplete data: {', '.join(missing)}")
    
    params = {
        "q": query,
        "l": location
    }
    return f"https://www.indeed.com/jobs?{urlencode(params)}"
