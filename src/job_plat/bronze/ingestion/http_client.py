import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Optional
from playwright.sync_api import sync_playwright


class HttpClient:
    def __init__(self, headers: dict | None = None):
        """
        HttpClient using Playwright to fetch pages with JS rendering.
        
        Args:
            headers: Optional dictionary of HTTP headers.
        """
        
        self.headers = headers or {}
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            extra_http_headers=self.headers
        )
        self._page = self._context.new_page()
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True
    )
    def get_text(self, url: str) -> str:
        """
            Fetches the HTML content of a URL with JS execution.
            Retries on failure using Tenacity.
        """
        self._page.goto(url, timeout=30000)
        self._page.wait_for_load_state("networkidle")
        html = self._page.content()
        if not html:
            raise ValueError(f"Empty response from {url}")
        return html
    
    def close(self):
        """
        Properly close Playwright browser.
        """
        self._context.close()
        self.browser.close()
        self._playwright.stop()


# class HttpClient:
    # def __init__(self, headers: dict | None = None):
        # self.session = requests.Session()
        # if headers:
            # self.session.headers.update(headers)
    
    # @retry(
        # stop=stop_after_attempt(5),
        # wait=wait_exponential(multiplier=1, min=2, max=30),
        # reraise=True
    # )
    # def get_text(self, url: str) -> str:
        # response = self.session.get(url, timeout=10)
        # response.raise_for_status()
        # return response.text
    
    # def close(self):
        # self.session.close()
