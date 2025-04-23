-- Emerging skills with high demand in the job market
-- This query identifies skills that have high importance scores across occupations

SELECT 
    s.element_name as skill_name,
    AVG(s.data_value) as avg_importance,
    COUNT(DISTINCT s.onetsoc_code) as occupation_count,
    STRING_AGG(DISTINCT CASE WHEN s.data_value > 3.5 THEN o.title ELSE NULL END, ', ' ORDER BY o.title) 
        AS top_occupations_sample
FROM skills s
JOIN occupation_data o ON s.onetsoc_code = o.onetsoc_code
WHERE 
    s.scale_id = 'IM' -- Importance scale
    AND s.data_value > 2.5 -- Moderate to high importance
GROUP BY s.element_name
HAVING COUNT(DISTINCT s.onetsoc_code) > 10 -- Present in multiple occupations
ORDER BY avg_importance DESC, occupation_count DESC
LIMIT 50;
