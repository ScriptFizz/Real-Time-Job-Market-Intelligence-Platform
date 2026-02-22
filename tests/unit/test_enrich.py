from job_plat.silver.enrichment.extract_skills import extract_skills, normalize_skills

def test_extract_skills():
    tokens = "We require Python and Spark experience"
    skills = extract_skills(tokens)
    
    assert "python" in skills
    assert "spark" in skills


def test_normalize_skills():
    skills = ["py spark", "google cloud", "amazon web services"]
    normalized_skills = normalize_skills(tokens)
    
    assert "pyspark" in normalized_skills
    assert "gcp" in normalized_skills
    assert "was" in normalized_skills
