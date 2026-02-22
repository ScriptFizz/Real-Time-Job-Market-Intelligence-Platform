from job_plat.bronze.ingestion.scrapers import IndeedScraper, LinkedInScraper
from bs4 import BeautifulSoup


def test_parse_indeed_returns_jobs():
    html= """
    <html>
        <div class="job">
            <h2>Data Engineer</h2>
        </div>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    scraper = IndeedScraper(client = None)
    
    jobs = list(scraper.parse(soup))
    
    assert len(jobs) == 1
    assert jobs[0]["title"] == "Data Engineer"


def test_parse_linkedin_returns_jobs():
    html= """
    <html>
        <div class="job">
            <h2>Data Engineer</h2>
        </div>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    scraper = LinkedInScraper(client = None)
    
    jobs = list(scraper.parse(soup))
    
    assert len(jobs) == 1
    assert jobs[0]["title"] == "Data Engineer"
