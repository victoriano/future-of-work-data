-- Top technology skills across all occupations in ESCO
-- This query identifies the most common technology-related skills across all occupations

SELECT 
    s.preferredLabel as skill_name,
    COUNT(*) as occupation_count,
    COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT occupationUri) FROM occupationSkillRelations_en) as percentage
FROM skills_en s
JOIN occupationSkillRelations_en r ON s.conceptUri = r.skillUri
WHERE 
    s.preferredLabel LIKE '%software%' OR
    s.preferredLabel LIKE '%programming%' OR
    s.preferredLabel LIKE '%computer%' OR
    s.preferredLabel LIKE '%digital%' OR
    s.preferredLabel LIKE '%technology%' OR
    s.preferredLabel LIKE '%data%'
GROUP BY s.preferredLabel
ORDER BY occupation_count DESC
