from typing import List, Dict

from job_plat.transformations.silver.enrichment.skills import RAW_SKILLS, SKILL_SYNONYMS

SKILL_SET = set(RAW_SKILLS)

def extract_skills(tokens: List[str]) -> List[str]:
    """
    Identify skills (defined in SKILL_SET) from  a job description.
    
    Args:
        tokens (List[str]): List of tokens from a job descriptions.
        
    Returns:
        List[str]: List of skills filtered from tokens.
    """
    if not tokens:
        return []
    return list({t for t in tokens if t in SKILL_SET})


def normalize_skills(skills: List[str]) -> List[str]:
    """
    Normalize a list of skills by grouping synonyms under the same label.
    
    Args:
        skills (List[str]): List of skills from a job descriptions.
        
    Returns:
        List[str]: List of skills normalized.
    """
    normalized = []
    for s in skills:
        normalized.append(SKILL_SYNONYMS.get(s, s))
    return list(set(normalized))
    

def skill_confidence(tokens: List[str], skills: List[str]) -> Dict[str, float]:
    """
    Assign to each skill in a list of tokens its confidence rating.
    
    Args:
        tokens (List[str]): List of tokens from a job description.
        skills (List[str]): List of skills from a job description.
    
    Returns:
        Dict[str, float]: Mapping from each skill to its confidence rating.
    """
    confidence = {}
    if not tokens or not skills:
        return confidence
    for skill in skills:
        count = tokens.count(skill)
        confidence[skill] = min(0.5 + count * 0.2, 0.95)
    return confidence
    



