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

# --- OEWS Salary Data Config ---
OEWS_SALARY_PATH = 'data/raw/OEWS/national_M2024_dl.xlsx'
OEWS_OCC_CODE_COL = 'OCC_CODE' # Expected OCC Code column name in OEWS
OEWS_SALARY_COLS = ['A_MEDIAN', 'A_PCT25', 'A_PCT75'] # Salary columns to fetch
OEWS_EXTRA_COLS = ['TOT_EMP', 'O_GROUP', 'EMP_PRSE', 'MEAN_PRSE', 'OCC_TITLE'] # Additional OEWS cols
OEWS_NUMERIC_EXTRA_COLS = ['TOT_EMP', 'EMP_PRSE', 'MEAN_PRSE'] # Extra cols needing numeric cleaning
OEWS_NULL_VALUES = ['*', '#'] # Values representing nulls in OEWS salary data

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
    tech_skills_agg_sql = """
        SELECT
            ts.onetsoc_code,
            COUNT(DISTINCT ts.commodity_title) AS n_technology_skills,
            list(DISTINCT ts.commodity_title ORDER BY ts.commodity_title) AS tech_skills_list
        FROM technology_skills ts
        GROUP BY ts.onetsoc_code
    """
    tech_skills_df = _query_pl(con, tech_skills_agg_sql)

    con.close() # Close DuckDB connection

    # ---------------------------------------------------------
    # 9. Load and Prepare OEWS Salary Data
    # ---------------------------------------------------------
    logging.info(f"Loading OEWS salary data from: {OEWS_SALARY_PATH}")
    try:
        df_salary = pl.read_excel(
            OEWS_SALARY_PATH # Read without null_values argument
        )
        logging.info(f"OEWS salary data loaded. Shape: {df_salary.shape}")

        # Select and rename necessary columns
        cols_to_select_from_oews = [OEWS_OCC_CODE_COL] + OEWS_SALARY_COLS + OEWS_EXTRA_COLS
        df_salary = df_salary.select(cols_to_select_from_oews)

        # Rename OCC code column for clarity before join
        df_salary = df_salary.rename({
            OEWS_OCC_CODE_COL: 'soc_code',
            'OCC_TITLE': 'occupation_group_title' # Rename OCC_TITLE
        })

        # Replace specific strings with nulls in salary columns before casting
        cols_to_clean = OEWS_SALARY_COLS + OEWS_NUMERIC_EXTRA_COLS
        for col in cols_to_clean:
            for null_val in OEWS_NULL_VALUES:
                df_salary = df_salary.with_columns(
                    # Explicitly cast to string for comparison
                    pl.when(pl.col(col).cast(pl.Utf8) == null_val)
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
        logging.info(f"Replaced {OEWS_NULL_VALUES} with nulls in salary columns.")

        # Now, convert salary columns to Float64, errors -> null
        # Convert TOT_EMP to Int64, others to Float64
        for col in OEWS_SALARY_COLS:
            df_salary = df_salary.with_columns(
                pl.col(col).cast(pl.Float64, strict=False).alias(col)
            )
        if 'TOT_EMP' in df_salary.columns:
             df_salary = df_salary.with_columns(
                 pl.col('TOT_EMP').cast(pl.Int64, strict=False).alias('TOT_EMP')
             )
        for col in ['EMP_PRSE', 'MEAN_PRSE']:
             if col in df_salary.columns:
                 df_salary = df_salary.with_columns(
                     pl.col(col).cast(pl.Float64, strict=False).alias(col)
                 )
        logging.info("OEWS salary columns selected, renamed, and cast to float.")

    except Exception as e:
        logging.error(f"Failed to load or process OEWS salary data: {e}")
        # Optionally, decide if you want to proceed without salary data or exit
        # For now, we'll create an empty placeholder if loading fails
        df_salary = pl.DataFrame({
            'soc_code': [],
            **{col: [] for col in OEWS_SALARY_COLS},
            **{col: [] for col in OEWS_EXTRA_COLS}
        }).with_columns([
            pl.col('soc_code').cast(pl.Utf8),
            *[pl.col(col).cast(pl.Float64) for col in OEWS_SALARY_COLS],
            pl.col('TOT_EMP').cast(pl.Int64),
            pl.col('O_GROUP').cast(pl.Utf8),
            pl.col('occupation_group_title').cast(pl.Utf8), # Add schema for placeholder
            pl.col('EMP_PRSE').cast(pl.Float64),
            pl.col('MEAN_PRSE').cast(pl.Float64),
        ])
        logging.warning("Proceeding without salary data due to loading error.")

    # ---------------------------------------------------------
    # 10. Merge all ONET aggregates
    # ---------------------------------------------------------
    logging.info("Merging all ONET aggregates into final DataFrame")
    df_final = base_df
    joins = [
        job_zone_df,
        alt_title_df,
        skills_df,
        knowledge_df,
        abilities_df,
        activities_df,
        tech_skills_df,
    ]

    for join_df in joins:
        df_final = df_final.join(join_df, on='onetsoc_code', how='left')

    logging.info(f"Final DataFrame shape before salary join: {df_final.shape}")

    # ---------------------------------------------------------
    # 12. Join with Salary Data
    # ---------------------------------------------------------
    # Prepare join key: Create SOC code (XX-XXXX) from ONET code (XX-XXXX.XX)
    df_final = df_final.with_columns(
        pl.col("onetsoc_code").str.slice(0, 7).alias("soc_code")
    )

    # Perform the join
    df_final = df_final.join(df_salary, on='soc_code', how='left')
    logging.info(f"DataFrame shape after salary join: {df_final.shape}")

    # Rename columns as requested
    rename_map = {
        'EMP_PRSE': 'Employment percent relative standard error',
        'MEAN_PRSE': 'Wage percent relative standard error'
    }
    # Only rename if columns exist (in case of load error)
    actual_rename_map = {k: v for k, v in rename_map.items() if k in df_final.columns}
    if actual_rename_map:
        df_final = df_final.rename(actual_rename_map)
        logging.info(f"Renamed columns: {actual_rename_map}")

    # Drop the temporary soc_code column used for joining
    df_final = df_final.drop('soc_code')

    # Reorder columns: Place list columns right after description,
    # O_GROUP, then salary columns, TOT_EMP, error cols near end.
    list_cols = [
        'skills_list',
        'knowledge_list',
        'abilities_list',
        'work_activities_list',
        'tech_skills_list'
    ]
    soc_code_col = ['onetsoc_code'] if 'onetsoc_code' in df_final.columns else []
    occupation_group_title_col = ['occupation_group_title'] if 'occupation_group_title' in df_final.columns else []
    occupation_title_col = ['occupation_title'] if 'occupation_title' in df_final.columns else []
    description_col = ['occupation_description'] if 'occupation_description' in df_final.columns else [] # Keep description separate for now
    early_oews_cols = []
    if 'TOT_EMP' in df_final.columns: early_oews_cols.append('TOT_EMP')
    if 'A_MEDIAN' in df_final.columns: early_oews_cols.append('A_MEDIAN')
    other_salary_cols = [col for col in OEWS_SALARY_COLS if col in df_final.columns and col not in early_oews_cols]
    o_group_col = ['O_GROUP'] if 'O_GROUP' in df_final.columns else []
    error_cols = [v for k, v in actual_rename_map.items() if v in df_final.columns]
    grouped_cols = soc_code_col + occupation_group_title_col + occupation_title_col + early_oews_cols + description_col + list_cols + other_salary_cols + o_group_col + error_cols
    other_cols_final = [col for col in df_final.columns if col not in grouped_cols]
    final_column_order = (
        soc_code_col
        + occupation_group_title_col
        + occupation_title_col
        + early_oews_cols
        + description_col # Put description back after early OEWS cols
        + list_cols
        + other_salary_cols
        + other_cols_final # Remaining ONET aggregates (counts, averages)
        + error_cols
        + o_group_col      # O_GROUP at the very end
    )
    df_final = df_final.select(final_column_order) # Apply final order

    logging.info(f"Final DataFrame shape after reorder: {df_final.shape}")

    # Sort by Total Employment descending if the column exists
    if 'TOT_EMP' in df_final.columns:
        df_final = df_final.sort('TOT_EMP', descending=True, nulls_last=True)
        logging.info("Sorted DataFrame by TOT_EMP descending.")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    logging.info(f"Writing Parquet to {OUTPUT_PATH}")
    df_final.write_parquet(OUTPUT_PATH)
    logging.info("Aggregation completed successfully")

if __name__ == "__main__":
    main()
