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
    # 4. Skills (Count, Avg Importance/Level, List)
    # ---------------------------------------------------------
    logging.info("Aggregating skills")
    skills_df = _query_pl(
        con,
        """
            SELECT
                onetsoc_code,
                COUNT(DISTINCT skill_name) AS n_skills,
                ROUND(AVG(CASE WHEN scale_id = 'IM' THEN skill_level END), 2) AS avg_skill_importance,
                ROUND(AVG(CASE WHEN scale_id = 'LV' THEN skill_level END), 2) AS avg_skill_level,
                list(DISTINCT skill_name ORDER BY skill_name) AS skills_list
            FROM occupation_skills
            GROUP BY onetsoc_code
        """
    )

    # ---------------------------------------------------------
    # 5. Knowledge Areas (Count, Avg Importance/Level, List)
    # ---------------------------------------------------------
    logging.info("Aggregating knowledge areas")
    knowledge_df = _query_pl(
        con,
        """
            SELECT
                onetsoc_code,
                COUNT(DISTINCT knowledge_area) AS n_knowledge_areas,
                ROUND(AVG(CASE WHEN scale_id = 'IM' THEN knowledge_level END), 2) AS avg_knowledge_importance,
                ROUND(AVG(CASE WHEN scale_id = 'LV' THEN knowledge_level END), 2) AS avg_knowledge_level,
                list(DISTINCT knowledge_area ORDER BY knowledge_area) AS knowledge_list
            FROM occupation_knowledge
            GROUP BY onetsoc_code
        """
    )

    # ---------------------------------------------------------
    # 6. Abilities (Count, Avg Importance/Level, List)
    # ---------------------------------------------------------
    logging.info("Aggregating abilities")
    abilities_df = _query_pl(
        con,
        """
            SELECT
                onetsoc_code,
                COUNT(DISTINCT element_name) AS n_abilities,
                ROUND(AVG(CASE WHEN scale_id = 'IM' THEN data_value END), 2) AS avg_ability_importance,
                ROUND(AVG(CASE WHEN scale_id = 'LV' THEN data_value END), 2) AS avg_ability_level,
                list(DISTINCT element_name ORDER BY element_name) AS abilities_list
            FROM abilities
            GROUP BY onetsoc_code
        """
    )

    # ---------------------------------------------------------
    # 7. Work Activities (Count, Avg Importance/Level, List)
    # ---------------------------------------------------------
    logging.info("Aggregating work activities")
    activities_df = _query_pl(
        con,
        """
            SELECT
                onetsoc_code,
                COUNT(DISTINCT activity) AS n_work_activities,
                ROUND(AVG(CASE WHEN scale_id = 'IM' THEN activity_level END), 2) AS avg_activity_importance,
                ROUND(AVG(CASE WHEN scale_id = 'LV' THEN activity_level END), 2) AS avg_activity_level,
                list(DISTINCT activity ORDER BY activity) AS work_activities_list
            FROM occupation_work_activities
            GROUP BY onetsoc_code
        """
    )

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

    logging.info(f"Final DataFrame shape: {df_final.shape}")

    # ---------------------------------------------------------
    # 11. Save to Parquet
    # ---------------------------------------------------------
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Writing Parquet to {OUTPUT_PATH}")
    df_final.write_parquet(OUTPUT_PATH)
    logging.info("Aggregation completed successfully")

if __name__ == "__main__":
    main()
