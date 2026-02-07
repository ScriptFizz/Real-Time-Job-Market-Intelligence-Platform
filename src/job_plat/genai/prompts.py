NORMALIZE_SKILLS_PROMPT = """
You are a data normalization assistant.

Given a list of job skill phrases, return a JSON mapping where:
- keys are original phrases
- values are normalized canonical skills
- use lowercase
- no explanations
- no hallucinations
- return only JSON

Example:
Input:
["python dev", "python programming", "py"]

Output:
{
  "python dev": "python",
  "python programming": "python",
  "py": "python"
}

Input:
{skills}
"""
