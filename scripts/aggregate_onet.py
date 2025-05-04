import os
import logging
import duckdb
import polars as pl

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ONET_DB_PATH = 'data/duckdb/onet_dataset_29.2.duckdb'
OUTPUT_DIR = 'data/processed/onet'
OUTPUT_FILENAME = 'onet_occupations_aggregated.parquet'
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

# Helper to run a SQL query against the DuckDB file and return a Polars DataFrame
def _query_pl(con: duckdb.DuckDBPyConnection, sql: str) -> pl.DataFrame:
    """Execute SQL and return result as Polars DataFrame."""
    return con.sql(sql).pl()

def main() -> None:
    if not os.path.exists(ONET_DB_PATH):
        logging.error(f"ONET database not found at {ONET_DB_PATH}")
        return

    logging.info(f"Connecting to ONET database: {ONET_DB_PATH}")
    con = duckdb.connect(ONET_DB_PATH, read_only=True)

    # ---------------------------------------------------------
    # 1. Base occupation metadata (one row per occupation)
    # ---------------------------------------------------------
    logging.info("Loading base occupation metadata (occupation_data)")
    base_df = _query_pl(
        con,
        """
            SELECT onetsoc_code, title AS occupation_title, description AS occupation_description
            FROM occupation_data
        """
    )
    logging.info(f"Base occupation rows: {base_df.height}")

    # ---------------------------------------------------------
    # 2. Job zone information
    # ---------------------------------------------------------
    logging.info("Aggregating job zone information")
    job_zone_df = _query_pl(
        con,
        """
            SELECT onetsoc_code, FIRST(job_zone) AS job_zone
            FROM job_zones
            GROUP BY onetsoc_code
        """
    )

    # ---------------------------------------------------------
    # 3. Alternate title counts
    # ---------------------------------------------------------
    logging.info("Counting alternate titles")
    alt_title_df = _query_pl(
        con,
        """
            SELECT onetsoc_code, COUNT(DISTINCT alternate_title) AS n_alternate_titles
            FROM alternate_titles
            GROUP BY onetsoc_code
        """
    )

    # ---------------------------------------------------------
    # 4. Skills (Count, Avg Importance/Level, List) - Filtered by P75 Level
    # ---------------------------------------------------------
    logging.info("Aggregating skills (P75 Level filter)")
    skills_agg_sql = """
        WITH SkillLevels AS (
            SELECT onetsoc_code, skill_name, skill_level
            FROM occupation_skills
            WHERE scale_id = 'LV'
        ),
        SkillP75 AS (
            SELECT
                onetsoc_code,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY skill_level) AS p75_skill_level
            FROM SkillLevels
            GROUP BY onetsoc_code
        ),
        SignificantSkills AS (
            SELECT sl.onetsoc_code, sl.skill_name
            FROM SkillLevels sl
            JOIN SkillP75 p75 ON sl.onetsoc_code = p75.onetsoc_code
            WHERE sl.skill_level >= p75.p75_skill_level
        )
        SELECT
            os.onetsoc_code,
            COUNT(DISTINCT os.skill_name) AS n_skills,
            ROUND(AVG(CASE WHEN os.scale_id = 'IM' THEN os.skill_level END), 2) AS avg_skill_importance,
            ROUND(AVG(CASE WHEN os.scale_id = 'LV' THEN os.skill_level END), 2) AS avg_skill_level,
            list(DISTINCT os.skill_name ORDER BY os.skill_name) AS skills_list
        FROM occupation_skills os
        JOIN SignificantSkills ss ON os.onetsoc_code = ss.onetsoc_code AND os.skill_name = ss.skill_name
        GROUP BY os.onetsoc_code;
    """
    skills_df = _query_pl(con, skills_agg_sql)

    # ---------------------------------------------------------
    # 5. Knowledge Areas (Count, Avg Importance/Level, List) - Filtered by P75 Level
    # ---------------------------------------------------------
    logging.info("Aggregating knowledge areas (P75 Level filter)")
    knowledge_agg_sql = """
        WITH KnowledgeLevels AS (
            SELECT onetsoc_code, knowledge_area, knowledge_level
            FROM occupation_knowledge
            WHERE scale_id = 'LV'
        ),
        KnowledgeP75 AS (
            SELECT
                onetsoc_code,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY knowledge_level) AS p75_knowledge_level
            FROM KnowledgeLevels
            GROUP BY onetsoc_code
        ),
        SignificantKnowledge AS (
            SELECT kl.onetsoc_code, kl.knowledge_area
            FROM KnowledgeLevels kl
            JOIN KnowledgeP75 p75 ON kl.onetsoc_code = p75.onetsoc_code
            WHERE kl.knowledge_level >= p75.p75_knowledge_level
        )
        SELECT
            ok.onetsoc_code,
            COUNT(DISTINCT ok.knowledge_area) AS n_knowledge_areas,
            ROUND(AVG(CASE WHEN ok.scale_id = 'IM' THEN ok.knowledge_level END), 2) AS avg_knowledge_importance,
            ROUND(AVG(CASE WHEN ok.scale_id = 'LV' THEN ok.knowledge_level END), 2) AS avg_knowledge_level,
            list(DISTINCT ok.knowledge_area ORDER BY ok.knowledge_area) AS knowledge_list
        FROM occupation_knowledge ok
        JOIN SignificantKnowledge sk ON ok.onetsoc_code = sk.onetsoc_code AND ok.knowledge_area = sk.knowledge_area
        GROUP BY ok.onetsoc_code;
    """
    knowledge_df = _query_pl(con, knowledge_agg_sql)

    # ---------------------------------------------------------
    # 6. Abilities (Count, Avg Importance/Level, List) - Filtered by P75 Level
    # ---------------------------------------------------------
    logging.info("Aggregating abilities (P75 Level filter)")
    abilities_agg_sql = """
        WITH AbilityLevels AS (
            SELECT onetsoc_code, element_name, data_value AS ability_level
            FROM abilities
            WHERE scale_id = 'LV'
        ),
        AbilityP75 AS (
            SELECT
                onetsoc_code,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ability_level) AS p75_ability_level
            FROM AbilityLevels
            GROUP BY onetsoc_code
        ),
        SignificantAbilities AS (
            SELECT al.onetsoc_code, al.element_name
            FROM AbilityLevels al
            JOIN AbilityP75 p75 ON al.onetsoc_code = p75.onetsoc_code
            WHERE al.ability_level >= p75.p75_ability_level
        )
        SELECT
            a.onetsoc_code,
            COUNT(DISTINCT a.element_name) AS n_abilities,
            ROUND(AVG(CASE WHEN a.scale_id = 'IM' THEN a.data_value END), 2) AS avg_ability_importance,
            ROUND(AVG(CASE WHEN a.scale_id = 'LV' THEN a.data_value END), 2) AS avg_ability_level,
            list(DISTINCT a.element_name ORDER BY a.element_name) AS abilities_list
        FROM abilities a
        JOIN SignificantAbilities sa ON a.onetsoc_code = sa.onetsoc_code AND a.element_name = sa.element_name
        GROUP BY a.onetsoc_code;
    """
    abilities_df = _query_pl(con, abilities_agg_sql)

    # ---------------------------------------------------------
    # 7. Work Activities (Count, Avg Importance/Level, List) - Filtered by P75 Level
    # ---------------------------------------------------------
    logging.info("Aggregating work activities (P75 Level filter)")
    work_activities_agg_sql = """
        WITH ActivityLevels AS (
            SELECT onetsoc_code, activity, activity_level
            FROM occupation_work_activities
            WHERE scale_id = 'LV'
        ),
        ActivityP75 AS (
            SELECT
                onetsoc_code,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY activity_level) AS p75_activity_level
            FROM ActivityLevels
            GROUP BY onetsoc_code
        ),
        SignificantActivities AS (
            SELECT acl.onetsoc_code, acl.activity
            FROM ActivityLevels acl
            JOIN ActivityP75 p75 ON acl.onetsoc_code = p75.onetsoc_code
            WHERE acl.activity_level >= p75.p75_activity_level
        )
        SELECT
            owa.onetsoc_code,
            COUNT(DISTINCT owa.activity) AS n_work_activities,
            ROUND(AVG(CASE WHEN owa.scale_id = 'IM' THEN owa.activity_level END), 2) AS avg_activity_importance,
            ROUND(AVG(CASE WHEN owa.scale_id = 'LV' THEN owa.activity_level END), 2) AS avg_activity_level,
            list(DISTINCT owa.activity ORDER BY owa.activity) AS work_activities_list
        FROM occupation_work_activities owa
        JOIN SignificantActivities sa ON owa.onetsoc_code = sa.onetsoc_code AND owa.activity = sa.activity
        GROUP BY owa.onetsoc_code;
    """
    activities_df = _query_pl(con, work_activities_agg_sql)

    # ---------------------------------------------------------
    # 8. Technology Skills (Count, List)
    # ---------------------------------------------------------
    logging.info("Aggregating technology skills")
    tech_df = _query_pl(
        con,
        """
            SELECT
                onetsoc_code,
                COUNT(DISTINCT commodity_title) AS n_technology_skills,
                list(DISTINCT commodity_title ORDER BY commodity_title) AS tech_skills_list
            FROM technology_skills
            GROUP BY onetsoc_code
        """
    )

    # ---------------------------------------------------------
    # 9. Merge all aggregates using Polars left joins
    # ---------------------------------------------------------
    logging.info("Merging all aggregates into final DataFrame")
    df_final = base_df
    joins = [
        job_zone_df,
        alt_title_df,
        skills_df,
        knowledge_df,
        abilities_df,
        activities_df,
        tech_df,
    ]

    for join_df in joins:
        df_final = df_final.join(join_df, on='onetsoc_code', how='left')

    # ---------------------------------------------------------
    # 10. Fill nulls for count columns with 0
    # ---------------------------------------------------------
    count_cols = [
        c for c in df_final.columns if c.startswith('n_') or c in ['job_zone']
    ]
    df_final = df_final.with_columns([
        pl.col(col).fill_null(0) if col != 'job_zone' else pl.col(col) for col in count_cols
    ])

    logging.info(f"Final DataFrame shape before reorder: {df_final.shape}")

    # Reorder columns: Place list columns right after description
    list_cols = [
        'skills_list',
        'knowledge_list',
        'abilities_list',
        'work_activities_list',
        'tech_skills_list'
    ]
    # Get remaining columns, excluding the base and list columns already specified
    base_cols = ['onetsoc_code', 'occupation_title', 'occupation_description']
    other_cols = [col for col in df_final.columns if col not in base_cols + list_cols]

    # Define the final order
    final_column_order = base_cols + list_cols + other_cols
    df_final = df_final.select(final_column_order)

    logging.info(f"Final DataFrame shape after reorder: {df_final.shape}")

    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    logging.info(f"Writing Parquet to {OUTPUT_PATH}")
    df_final.write_parquet(OUTPUT_PATH)
    logging.info("Aggregation completed successfully")

if __name__ == "__main__":
    main()
