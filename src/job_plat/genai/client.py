from openAI import OpenAI
import os

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)

def normalize_skills_batch(
    skills: list[str],
    model: str = "gpt-4o-mini"
) -> dict:
    from job_plat.genai.prompts import (
        NORMALIZE_SKILLS_PROMPT
    )
    
    prompt = NORMALIZE_SKILLS_PROMPT.format(
        skills=skills
    )
    
    response = client.chat.completions.create(
        model=model,
        message=[
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )
    
    # eval acceptable since prompt is trusted and response is controlled
    return eval(response.choices[0].message.content)
