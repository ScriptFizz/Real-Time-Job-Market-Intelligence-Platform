import requests
from tenacity import retry, stop_after_attempt, wait_exponential

class HttpClient:
    def __init__(self, headers: dict | None = None):
        self.session = requests.Session()
        if headers:
            self.session.headers.update(headers)
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True
    )
    def get_text(self, url: str) -> str:
        response = self.session.get(url, timeout=10)
        response.raise_for_status()
        return response.text()
    
    def close(self):
        self.session.close()
