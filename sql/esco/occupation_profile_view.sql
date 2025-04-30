-- ESCO Occupation Profile View
-- This script creates a comprehensive view of occupations with related data grouped as JSON arrays

CREATE OR REPLACE VIEW occupation_profile AS
WITH
-- Pre-process skills to get distinct labels per occupation/type
distinct_occupation_skills AS (
    SELECT DISTINCT
        r.occupationUri,
        s.preferredLabel,
        r.relationType,
        s.skillType,
        s.conceptUri as skillUri
    FROM occupationSkillRelations_en r
    JOIN skills_en s ON r.skillUri = s.conceptUri
    WHERE s.preferredLabel IS NOT NULL
),

-- Aggregate essential skills
essential_skills_agg AS (
    SELECT
        occupationUri,
        json_group_array(preferredLabel) AS essential_skills
    FROM distinct_occupation_skills
    WHERE relationType = 'essential'
    GROUP BY occupationUri
),

-- Aggregate optional skills
optional_skills_agg AS (
    SELECT
        occupationUri,
        json_group_array(preferredLabel) AS optional_skills
    FROM distinct_occupation_skills
    WHERE relationType = 'optional'
    GROUP BY occupationUri
),

-- Aggregate essential technical skills
essential_technical_skills_agg AS (
    SELECT
        occupationUri,
        json_group_array(preferredLabel) AS essential_technical_skills
    FROM distinct_occupation_skills
    WHERE relationType = 'essential' AND skillType = 'skill'
    GROUP BY occupationUri
),

-- Aggregate essential knowledge
essential_knowledge_agg AS (
    SELECT
        occupationUri,
        json_group_array(preferredLabel) AS essential_knowledge
    FROM distinct_occupation_skills
    WHERE relationType = 'essential' AND skillType = 'knowledge'
    GROUP BY occupationUri
),

-- Aggregate essential competences
essential_competences_agg AS (
    SELECT
        occupationUri,
        json_group_array(preferredLabel) AS essential_competences
    FROM distinct_occupation_skills
    WHERE relationType = 'essential' AND skillType = 'competence'
    GROUP BY occupationUri
),

-- Pre-process hierarchy
distinct_hierarchy AS (
    SELECT DISTINCT
        o.conceptUri,
        parent_occ.preferredLabel as parent_label
    FROM occupations_en o
    LEFT JOIN broaderRelationsOccPillar_en rel ON o.conceptUri = rel.conceptUri
    LEFT JOIN occupations_en parent_occ ON rel.broaderUri = parent_occ.conceptUri
    WHERE parent_occ.preferredLabel IS NOT NULL
),

-- Aggregate parent occupations
parent_occupations_agg AS (
    SELECT
        conceptUri,
        json_group_array(parent_label) AS parent_occupations
    FROM distinct_hierarchy
    GROUP BY conceptUri
),

-- Get ISCO info (doesn't need JSON aggregation here)
isco_info AS (
    SELECT
        o.conceptUri,
        MAX(isco.preferredLabel) AS isco_group_name,
        MAX(isco.code) AS isco_code,
        MAX(isco.description) AS isco_description
    FROM occupations_en o
    LEFT JOIN ISCOGroups_en isco ON o.iscoGroup = isco.code
    GROUP BY o.conceptUri
),

-- Aggregate green skills
green_skills_agg AS (
    SELECT
        dos.occupationUri,
        json_group_array(dos.preferredLabel) AS green_skills
    FROM distinct_occupation_skills dos
    JOIN greenSkillsCollection_en gs ON dos.skillUri = gs.conceptUri
    GROUP BY dos.occupationUri
),

-- Aggregate digital skills
digital_skills_agg AS (
    SELECT
        dos.occupationUri,
        json_group_array(dos.preferredLabel) AS digital_skills
    FROM distinct_occupation_skills dos
    JOIN digitalSkillsCollection_en ds ON dos.skillUri = ds.conceptUri
    GROUP BY dos.occupationUri
),

-- Aggregate transversal skills
transversal_skills_agg AS (
    SELECT
        dos.occupationUri,
        json_group_array(dos.preferredLabel) AS transversal_skills
    FROM distinct_occupation_skills dos
    JOIN transversalSkillsCollection_en ts ON dos.skillUri = ts.conceptUri
    GROUP BY dos.occupationUri
),

-- Aggregate language skills
language_skills_agg AS (
    SELECT
        dos.occupationUri,
        json_group_array(dos.preferredLabel) AS language_skills
    FROM distinct_occupation_skills dos
    JOIN languageSkillsCollection_en ls ON dos.skillUri = ls.conceptUri
    GROUP BY dos.occupationUri
),

-- Simplified CTE to pass through raw altLabels
alt_labels_raw AS (
    SELECT
        conceptUri,
        altLabels AS raw_alternative_names -- Pass through raw labels
    FROM occupations_en
    WHERE altLabels IS NOT NULL AND altLabels != ''
)

-- Main query combining all the information
SELECT
    o.conceptUri AS occupation_uri,
    o.preferredLabel AS occupation_name,
    o.description AS occupation_description,
    o.iscoGroup AS isco_group,
    COALESCE(i.isco_group_name, '') AS isco_group_name,
    COALESCE(i.isco_code, '') AS isco_code,
    COALESCE(i.isco_description, '') AS isco_description,
    o.regulatedProfessionNote,
    COALESCE(poa.parent_occupations, '[]') AS parent_occupations,
    -- Calculate counts based on JSON arrays
    COALESCE(json_array_length(esa.essential_skills), 0) AS num_essential_skills,
    COALESCE(json_array_length(osa.optional_skills), 0) AS num_optional_skills,
    COALESCE(esa.essential_skills, '[]') AS essential_skills,
    COALESCE(osa.optional_skills, '[]') AS optional_skills,
    COALESCE(etka.essential_technical_skills, '[]') AS essential_technical_skills,
    COALESCE(ekna.essential_knowledge, '[]') AS essential_knowledge,
    COALESCE(eca.essential_competences, '[]') AS essential_competences,
    COALESCE(gsa.green_skills, '[]') AS green_skills,
    COALESCE(dsa.digital_skills, '[]') AS digital_skills,
    COALESCE(tsa.transversal_skills, '[]') AS transversal_skills,
    COALESCE(lsa.language_skills, '[]') AS language_skills,
    -- Output raw alternative names for external processing
    COALESCE(alr.raw_alternative_names, '') AS raw_alternative_names,
    (
        CASE
            WHEN COALESCE(json_array_length(gsa.green_skills), 0) > 0 THEN true
            ELSE false
        END
    ) AS has_green_skills,
    (
        CASE
            WHEN COALESCE(json_array_length(dsa.digital_skills), 0) > 0 THEN true
            ELSE false
        END
    ) AS has_digital_skills,
    -- Calculate digitalization score based on count of digital skills
    CASE
        WHEN COALESCE(json_array_length(dsa.digital_skills), 0) = 0 THEN 0
        WHEN COALESCE(json_array_length(dsa.digital_skills), 0) BETWEEN 1 AND 3 THEN 1
        WHEN COALESCE(json_array_length(dsa.digital_skills), 0) BETWEEN 4 AND 10 THEN 2
        ELSE 3
    END AS digitalization_level
FROM
    occupations_en o
LEFT JOIN essential_skills_agg esa ON o.conceptUri = esa.occupationUri
LEFT JOIN optional_skills_agg osa ON o.conceptUri = osa.occupationUri
LEFT JOIN essential_technical_skills_agg etka ON o.conceptUri = etka.occupationUri
LEFT JOIN essential_knowledge_agg ekna ON o.conceptUri = ekna.occupationUri
LEFT JOIN essential_competences_agg eca ON o.conceptUri = eca.occupationUri
LEFT JOIN parent_occupations_agg poa ON o.conceptUri = poa.conceptUri
LEFT JOIN isco_info i ON o.conceptUri = i.conceptUri
LEFT JOIN green_skills_agg gsa ON o.conceptUri = gsa.occupationUri
LEFT JOIN digital_skills_agg dsa ON o.conceptUri = dsa.occupationUri
LEFT JOIN transversal_skills_agg tsa ON o.conceptUri = tsa.occupationUri
LEFT JOIN language_skills_agg lsa ON o.conceptUri = lsa.occupationUri
LEFT JOIN alt_labels_raw alr ON o.conceptUri = alr.conceptUri -- Join new CTE
ORDER BY o.preferredLabel;

-- Create an index on the occupation name for faster lookups
-- Note: This would be added if the view was materialized as a table
-- CREATE INDEX IF NOT EXISTS idx_occupation_profile_name ON occupation_profile(occupation_name);

-- Example query to use this view:
-- SELECT * FROM occupation_profile WHERE occupation_name LIKE '%data scientist%';
-- SELECT * FROM occupation_profile WHERE has_digital_skills = true ORDER BY digitalization_level DESC LIMIT 25;
-- SELECT * FROM occupation_profile WHERE json_contains(essential_skills, '"Python"'); -- Example JSON query
