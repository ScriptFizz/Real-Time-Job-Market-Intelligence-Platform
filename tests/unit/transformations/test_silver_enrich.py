from job_plat.transformations.silver.enrichment.extract_skills import (
    extract_skills,
    normalize_skills,
    skill_confidence,
)


def test_extract_skills():
    tokens = "We require Python and Spark experience".lower().split()
    skills = extract_skills(tokens)

    assert "python" in skills
    assert "spark" in skills


def test_extract_skills_returns_unique_sorted_skills():
    tokens = ["spark", "python", "spark"]

    assert extract_skills(tokens) == ["python", "spark"]


def test_normalize_skills():
    skills = ["py spark", "google cloud", "amazon web services"]
    normalized_skills = normalize_skills(skills)

    assert "pyspark" in normalized_skills
    assert "gcp" in normalized_skills
    assert "aws" in normalized_skills


def test_normalize_skills_returns_unique_sorted_skills():
    skills = [
        "py spark",
        "google cloud",
        "amazon web services",
        "py spark",
    ]

    assert normalize_skills(skills) == ["aws", "gcp", "pyspark"]


def test_skill_confidence_counts_skill_occurrences():
    confidence = skill_confidence(
        tokens=["python", "python", "spark"],
        skills=["python", "spark"],
    )

    assert confidence == {
        "python": 0.9,
        "spark": 0.7,
    }
