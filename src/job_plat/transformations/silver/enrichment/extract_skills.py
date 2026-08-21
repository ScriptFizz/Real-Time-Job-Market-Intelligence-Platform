from job_plat.transformations.silver.enrichment.skills import RAW_SKILLS, SKILL_SYNONYMS

SKILL_SET: frozenset[str] = frozenset(RAW_SKILLS)


def extract_skills(tokens: list[str]) -> list[str]:
    """
    Identify skills (defined in SKILL_SET) from  a job description.

    Args:
        tokens (list[str]): List of tokens from a job descriptions.

    Returns:
        list[str]: List of skills filtered from tokens.
    """
    if not tokens:
        return []
    return sorted({t for t in tokens if t in SKILL_SET})


def normalize_skills(skills: list[str]) -> list[str]:
    """
    Normalize a list of skills by grouping synonyms under the same label.

    Args:
        skills (list[str]): List of skills from a job descriptions.

    Returns:
        list[str]: List of skills normalized.
    """
    normalized = {SKILL_SYNONYMS.get(skill, skill) for skill in skills}
    return sorted(normalized)


def skill_confidence(tokens: list[str], skills: list[str]) -> dict[str, float]:
    """
    Assign to each skill in a list of tokens its confidence rating.

    Args:
        tokens (list[str]): List of tokens from a job description.
        skills (list[str]): List of skills from a job description.

    Returns:
        dict[str, float]: Mapping from each skill to its confidence rating.
    """
    confidence: dict[str, float] = {}
    if not tokens or not skills:
        return confidence
    for skill in skills:
        count = tokens.count(skill)
        confidence[skill] = min(0.5 + count * 0.2, 0.95)
    return confidence
