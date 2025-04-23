#!/usr/bin/env python3
"""
Skills Crosswalk Analysis

This script performs a comparative analysis between ESCO and O*NET skills,
creating a crosswalk mapping and identifying skills gaps and overlaps.
"""

import os
import sys
import pandas as pd
from pathlib import Path

# Add the project root to the path to allow importing from src
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.append(str(project_root))

from src.utils.db import get_esco_connection, get_onet_connection

def extract_esco_skills():
    """Extract skills data from ESCO database"""
    con = get_esco_connection()
    
    # SQL to extract ESCO skills with their categories
    sql = """
    SELECT 
        s.conceptUri as skill_id,
        s.preferredLabel as skill_name,
        s.description,
        (SELECT GROUP_CONCAT(r.conceptType, ', ') 
         FROM broaderRelationsSkillPillar_en r 
         WHERE r.conceptUri = s.conceptUri) as categories
    FROM skills_en s
    WHERE s.preferredLabel IS NOT NULL
    ORDER BY s.preferredLabel
    """
    
    esco_skills = con.execute(sql).fetchdf()
    print(f"Extracted {len(esco_skills)} skills from ESCO")
    
    return esco_skills

def extract_onet_skills():
    """Extract skills data from O*NET database"""
    con = get_onet_connection()
    
    # SQL to extract O*NET skills with their importance and level across occupations
    sql = """
    SELECT 
        element_name as skill_name,
        AVG(CASE WHEN scale_id = 'IM' THEN data_value ELSE NULL END) as avg_importance,
        AVG(CASE WHEN scale_id = 'LV' THEN data_value ELSE NULL END) as avg_level,
        COUNT(DISTINCT onetsoc_code) as occupation_count
    FROM skills
    WHERE element_name IS NOT NULL
    GROUP BY element_name
    ORDER BY element_name
    """
    
    onet_skills = con.execute(sql).fetchdf()
    print(f"Extracted {len(onet_skills)} skills from O*NET")
    
    return onet_skills

def create_skills_crosswalk(esco_skills, onet_skills):
    """Create a crosswalk between ESCO and O*NET skills based on name similarity"""
    # Simple string matching for this example
    # In a real implementation, would use more sophisticated NLP techniques
    
    crosswalk = []
    for onet_skill in onet_skills.itertuples():
        onet_skill_name = onet_skill.skill_name.lower()
        
        # Find potential matches in ESCO
        potential_matches = esco_skills[
            esco_skills['skill_name'].str.lower().str.contains(onet_skill_name) | 
            esco_skills['description'].str.lower().str.contains(onet_skill_name)
        ]
        
        if not potential_matches.empty:
            for match in potential_matches.itertuples():
                crosswalk.append({
                    'onet_skill': onet_skill.skill_name,
                    'onet_importance': onet_skill.avg_importance,
                    'onet_occupations': onet_skill.occupation_count,
                    'esco_skill': match.skill_name,
                    'esco_id': match.skill_id,
                    'esco_categories': match.categories,
                    'match_type': 'name_similarity'
                })
    
    crosswalk_df = pd.DataFrame(crosswalk)
    print(f"Created crosswalk with {len(crosswalk_df)} potential skill mappings")
    
    return crosswalk_df

def identify_skill_gaps(esco_skills, onet_skills, crosswalk_df):
    """Identify skills that exist in one framework but not the other"""
    
    # O*NET skills that don't have a match in ESCO
    onet_matched_skills = set(crosswalk_df['onet_skill'].unique())
    onet_missing_in_esco = onet_skills[~onet_skills['skill_name'].isin(onet_matched_skills)]
    
    # ESCO skills that don't have a match in O*NET
    esco_matched_skills = set(crosswalk_df['esco_skill'].unique())
    esco_missing_in_onet = esco_skills[~esco_skills['skill_name'].isin(esco_matched_skills)]
    
    print(f"Found {len(onet_missing_in_esco)} O*NET skills without ESCO match")
    print(f"Found {len(esco_missing_in_onet)} ESCO skills without O*NET match")
    
    return onet_missing_in_esco, esco_missing_in_onet

def main():
    # Create output directory
    output_dir = os.path.join(project_root, "data", "derived")
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract skills from both databases
    esco_skills = extract_esco_skills()
    onet_skills = extract_onet_skills()
    
    # Create skills crosswalk
    crosswalk_df = create_skills_crosswalk(esco_skills, onet_skills)
    
    # Identify skill gaps
    onet_missing, esco_missing = identify_skill_gaps(esco_skills, onet_skills, crosswalk_df)
    
    # Save results
    crosswalk_df.to_csv(os.path.join(output_dir, "skills_crosswalk.csv"), index=False)
    onet_missing.to_csv(os.path.join(output_dir, "onet_skills_not_in_esco.csv"), index=False)
    esco_missing.to_csv(os.path.join(output_dir, "esco_skills_not_in_onet.csv"), index=False)
    
    print(f"Results saved to {output_dir}")

if __name__ == "__main__":
    main()
