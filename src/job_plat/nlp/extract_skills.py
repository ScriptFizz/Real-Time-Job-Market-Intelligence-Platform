from typing import List, Dict

from job_plat.processing.skills import RAW_SKILLS, SKILL_SYNONYMS

SKILL_SET = set(RAW_SKILLS)

def extract_skills(tokens: List[str]) -> List[str]:
    
    if not tokens:
        return []
    return list({t for t in tokens if t in SKILL_SET})


def normalize_skills(skills: List[str]) -> List[str]:
    
    normalized = []
    for s in skills:
        normalized.append(SKILL_SYNONYMS.get(s, s))
    return list(set(normalized))
    

def skill_confidence(tokens: List[str], skills: List[str]) -> Dict[str, float]:
    
    confidence = {}
    if not tokens or not skills:
        return confidence
    for skill in skills:
        count = tokens.count(skill)
        confidence[skill] = min(0.5 + count * 0.2, 0.95)
    return confidence
    



