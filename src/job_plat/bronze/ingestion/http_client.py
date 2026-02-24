import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Optional
from playwright.sync_api import sync_playwright
from job_plat.config.browser import DEFAULT_BROWSER_HEADERS, REALISTIC_USER_AGENT

class HttpClient:
    def __init__(self, headers: dict  = DEFAULT_BROWSER_HEADERS, user_agent: str = REALISTIC_USER_AGENT):
        """
        HttpClient using Playwright to fetch pages with JS rendering.
        
        Args:
            headers: Optional dictionary of HTTP headers.
        """
        
        self.headers = headers or {}
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            #channel="chrome",
            headless=False
            )
        self._context = self._browser.new_context(
            locale="en-US",
            viewport={"width": 1280, "height": 800}
        )
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def get_text(self, url: str) -> str:
        """
            Fetches the HTML content of a URL with JS execution.
            Retries on failure using Tenacity.
        """
        page = self._context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_selector('[data-testid="job-card"], text="No jobs found"', timeout=15000)
            
            # Small human-like pause
            page.wait_for_timeout(random.randint(1000, 2500))
            
            # Small scroll to trigger lazy rendering
            page.mouse.wheel(0, random.randint(300, 700))
            page.wait_for_timeout(random.randint(500, 1500))

            
            html = page.content()
            
            if not html:
                raise ValueError(f"Empty response from {url}")
            
            # Scrape quality check
            if "captcha" in html.lower() or "Security Check" in html:
                raise RuntimeError("Blocked by anti-bot protection")
                
            return html
        finally:
            page.close()
    
    def close(self):
        """
        Properly close Playwright browser.
        """
        self._context.close()
        self._browser.close()
        self._playwright.stop()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()



# class HttpClient:
    # def __init__(self, headers: dict  = DEFAULT_BROWSER_HEADERS, user_agent: str = REALISTIC_USER_AGENT):
        # """
        # HttpClient using Playwright to fetch pages with JS rendering.
        
        # Args:
            # headers: Optional dictionary of HTTP headers.
        # """
        
        # self.headers = headers or {}
        # self._playwright = sync_playwright().start()
        # self._browser = self._playwright.chromium.launch(
            # headless=False, #True,
            # args=["--disable-blink-features=AutomationControlled"]
            # )
        # self._context = self._browser.new_context(
            # extra_http_headers=self.headers,
            # user_agent=user_agent,
            # locale="en-US",
            # viewport={"width": 1280, "height": 800}
        # )
        # self._page = self._context.new_page()
    
    # @retry(
        # stop=stop_after_attempt(5),
        # wait=wait_exponential(multiplier=1, min=2, max=30),
        # reraise=True
    # )
    # def get_text(self, url: str) -> str:
        # """
            # Fetches the HTML content of a URL with JS execution.
            # Retries on failure using Tenacity.
        # """
        # page = self._context.new_page()
        # try:
            # page.goto(url, wait_until="networkidle", timeout=60000)
            # page.wait_for_selector('[data-testid="job-card"]')
            # html = page.content()
            # if not html:
                # raise ValueError(f"Empty response from {url}")
            # return html
        # finally:
            # page.close()
    
    # def close(self):
        # """
        # Properly close Playwright browser.
        # """
        # self._context.close()
        # self._browser.close()
        # self._playwright.stop()

    # def __enter__(self):
        # return self
    
    # def __exit__(self, exc_type, exc_val, exc_tb):
        # self.close()

