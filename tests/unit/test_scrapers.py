from job_plat.bronze.ingestion.scrapers import IndeedScraper, LinkedInScraper
from bs4 import BeautifulSoup


def test_parse_indeed_returns_jobs():
    html= """
    <html>
        <div data-testid="jobsearch-SerpJobCard">
            <h2>Data Engineer</h2>
        </div>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    scraper = IndeedScraper(client = None)
    
    jobs = list(scraper.parse(soup))
    
    assert len(jobs) == 1
    assert jobs[0]["job_title_raw"] == "Data Engineer"


def test_parse_linkedin_returns_jobs():
    html= """
    <html>
        <div class="base-card">
            <a class="base-card__full-link"
               href="https://linkedin.com/jobs/view/1234567890"></a>
            <h3>Data Engineer</h3>
            <h4>ACME Corp</h4>
            <span class="job-search-card__location">
                Berlin, Germany
            </span>
        </div>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    scraper = LinkedInScraper(client = None)
    
    jobs = list(scraper.parse(soup))
    
    assert len(jobs) == 1
    assert jobs[0]["job_title_raw"] == "Data Engineer"
