import pandas as pd

# 1. seniority_level: Determine from job_title (Junior, Mid, Senior, Executive)
def get_seniority(job_title):
    if pd.isna(job_title):
        return None
    job_title_lower = job_title.lower()
    if any(x in job_title_lower for x in ['intern', 'junior', 'entry', 'assistant', 'associate']):
        return 'Junior'
    elif any(x in job_title_lower for x in ['lead', 'mid', 'intermediate']):
        return 'Mid'
    elif any(x in job_title_lower for x in ['senior', 'sr', 'principal', 'staff']):
        return 'Senior'
    elif any(x in job_title_lower for x in ['executive', 'chief', 'ceo', 'founder', 'cto', 'coo', 'cfo', 'head', 'president', 'vp', 'vice president', 'director']):
        return 'Executive'
    else:
        return None