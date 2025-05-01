import duckdb
import polars as pl
import os
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = 'data/duckdb/esco_dataset_1.2.0.duckdb'
VIEW_SQL_PATH = 'sql/esco/occupation_profile_view.sql'
HIGH_HIERARCHY_PATH = 'data/derived/isco_hierarchy.parquet' # Path to the hierarchy file
OUTPUT_DIR = 'data/processed/esco'
OUTPUT_FILENAME = 'esco_occupation_profiles.parquet'
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

# Column containing raw alternative names (needs special newline handling)
RAW_ALT_NAMES_COL = 'raw_alternative_names'
FINAL_ALT_NAMES_COL = 'alternative_names'

# Columns expected to contain JSON array strings from the SQL view
JSON_LIST_COLUMNS = [
    'broader_occupation_uris',
    'narrower_occupation_uris',
    'parent_occupations',
    'essential_skills',
    'optional_skills',
    'essential_technical_skills',
    'essential_knowledge',
    'essential_competences',
    'digital_skills',
    'green_skills',
    'transversal_skills',
    'language_skills'
]

def _process_raw_alt_name_to_list(raw_name):
    """Processes a single raw multi-line string into a list of strings or None if empty."""
    if raw_name is None or not isinstance(raw_name, str) or not raw_name.strip():
        return None # Return None for null/empty input -> will become null in Polars

    lines = []
    for line in raw_name.split('\n'):
        lines.extend(line.split('\r'))
    cleaned_labels = [label.strip() for label in lines if label.strip()]

    return cleaned_labels if cleaned_labels else None # Return None if list is empty

def main():
    logging.info(f"Connecting to database: {DB_PATH}")
    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        exit(1)
        
    # --- Explicitly create base views from CSVs with correct types ---
    RAW_ESCO_DIR = 'data/raw/esco/1.2.0'
    csv_configs = {
        'occupations_en': {'path': os.path.join(RAW_ESCO_DIR, 'occupations_en.csv'), 'dtypes': {'conceptUri': 'VARCHAR', 'iscoGroup': 'VARCHAR'}},
        'ISCOGroups_en': {'path': os.path.join(RAW_ESCO_DIR, 'ISCOGroups_en.csv'), 'dtypes': {'code': 'VARCHAR', 'conceptUri': 'VARCHAR'}},
        'skills_en': {'path': os.path.join(RAW_ESCO_DIR, 'skills_en.csv'), 'dtypes': {'conceptUri': 'VARCHAR', 'skillType': 'VARCHAR'}},
        'occupationSkillRelations_en': {'path': os.path.join(RAW_ESCO_DIR, 'occupationSkillRelations_en.csv'), 'dtypes': {'occupationUri': 'VARCHAR', 'skillUri': 'VARCHAR', 'relationType': 'VARCHAR'}},
        'broaderRelationsOccPillar_en': {'path': os.path.join(RAW_ESCO_DIR, 'broaderRelationsOccPillar_en.csv'), 'dtypes': {'conceptUri': 'VARCHAR', 'broaderUri': 'VARCHAR'}},
        'greenSkillsCollection_en': {'path': os.path.join(RAW_ESCO_DIR, 'greenSkillsCollection_en.csv'), 'dtypes': {'conceptUri': 'VARCHAR'}},
        'digitalSkillsCollection_en': {'path': os.path.join(RAW_ESCO_DIR, 'digitalSkillsCollection_en.csv'), 'dtypes': {'conceptUri': 'VARCHAR'}},
        'transversalSkillsCollection_en': {'path': os.path.join(RAW_ESCO_DIR, 'transversalSkillsCollection_en.csv'), 'dtypes': {'conceptUri': 'VARCHAR'}},
        'languageSkillsCollection_en': {'path': os.path.join(RAW_ESCO_DIR, 'languageSkillsCollection_en.csv'), 'dtypes': {'conceptUri': 'VARCHAR'}}
    }

    logging.info("Creating/replacing base views from CSVs with explicit VARCHAR types...")
    try:
        for view_name, config in csv_configs.items():
            if os.path.exists(config['path']):
                logging.info(f"  Dropping existing table (if any) and creating view: {view_name} from {config['path']}")
                # Construct dtypes string for SQL
                dtype_str = ', '.join([f"'{k}': '{v}'" for k, v in config['dtypes'].items()])
                drop_sql = f"DROP TABLE IF EXISTS {view_name};"
                create_sql = f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT * FROM read_csv_auto('{config['path']}', header=True,
                    dtypes={{{dtype_str}}});
                """
                con.sql(drop_sql) # Drop the table first
                con.sql(create_sql) # Then create the view
            else:
                logging.warning(f"  CSV file not found for {view_name}: {config['path']}. Skipping view creation.")
        logging.info("Base views created/replaced successfully (where CSVs were found).")
    except Exception as e:
        logging.error(f"Failed to create base views from CSVs: {e}")
        con.close()
        exit(1)
    # ------------------------------------------------------------------

    logging.info(f"Reading view definition from: {VIEW_SQL_PATH}")
    try:
        with open(VIEW_SQL_PATH, 'r', encoding='utf-8') as f:
            view_sql = f.read()
    except FileNotFoundError:
        logging.error(f"SQL file not found at {VIEW_SQL_PATH}")
        con.close()
        exit(1)
    except Exception as e:
        logging.error(f"Error reading SQL file: {e}")
        con.close()
        exit(1)

    logging.info("Executing SQL to create/replace occupation_profile view...")
    try:
        con.execute(view_sql)
    except Exception as e:
        logging.error(f"Error executing SQL view definition: {e}")
        con.close()
        exit(1)

    logging.info("Querying occupation_profile view...")
    try:
        query = "SELECT * FROM occupation_profile ORDER BY occupation_name"
        df = con.execute(query).pl()
    except Exception as e:
        logging.error(f"Error querying occupation_profile view: {e}")
        con.close()
        exit(1)

    con.close()
    logging.info("Database connection closed.")

    # --- Load ISCO Hierarchy Data ---
    logging.info(f"Loading ISCO hierarchy data from: {HIGH_HIERARCHY_PATH}")
    try:
        df_hierarchy = pl.read_parquet(HIGH_HIERARCHY_PATH)
        # Ensure hierarchy code is string for joining
        df_hierarchy = df_hierarchy.with_columns(pl.col("code").cast(pl.Utf8))
    except Exception as e:
        logging.error(f"Failed to load ISCO hierarchy file: {e}")
        exit(1)

    # Ensure the join key in the main df is also string
    if 'isco_group' in df.columns:
        df = df.with_columns(pl.col("isco_group").cast(pl.Utf8))
    else:
        logging.error("'isco_group' column not found in occupation data. Cannot join hierarchy.")
        exit(1)

    # --- Process Columns ---
    expressions = []

    # 1. Process raw_alternative_names (special newline handling)
    if RAW_ALT_NAMES_COL in df.columns:
        logging.info(f"Processing column: {RAW_ALT_NAMES_COL}")
        expressions.append(
            pl.col(RAW_ALT_NAMES_COL)
              .map_elements(_process_raw_alt_name_to_list, return_dtype=pl.List(pl.Utf8))
              .alias(FINAL_ALT_NAMES_COL)
        )
    else:
        logging.warning(f"Column '{RAW_ALT_NAMES_COL}' not found. Skipping processing.")

    # 2. Process other JSON list columns
    for col_name in JSON_LIST_COLUMNS:
        if col_name in df.columns:
            logging.info(f"Processing column: {col_name}")
            expressions.append(
                # Decode JSON string to list
                pl.col(col_name)
                  .str.json_decode(dtype=pl.List(pl.Utf8))
                  # Replace empty list [] with null
                  .pipe(lambda s: pl.when(s.list.len() == 0).then(None).otherwise(s))
                  .alias(col_name) # Overwrite the original column
            )
        else:
            logging.warning(f"Expected JSON list column '{col_name}' not found. Skipping.")

    # Apply all processing expressions
    if expressions:
        logging.info("Applying column processing expressions...")
        df = df.with_columns(expressions)

    # Drop the original raw alt names column if it exists and was processed
    if RAW_ALT_NAMES_COL in df.columns and FINAL_ALT_NAMES_COL != RAW_ALT_NAMES_COL:
         df = df.drop(RAW_ALT_NAMES_COL)
         logging.info(f"Dropped intermediate column: {RAW_ALT_NAMES_COL}")

    # --- Integrate ISCO Hierarchy ---
    logging.info("Integrating ISCO hierarchy...")

    # Define hierarchy levels and their target column names
    hierarchy_levels = {
        4: {'code': 'isco_level_4_code', 'label': 'isco_level_4_label', 'parent_join_col_next': 'isco_level_3_code_join'}, # Unit Group
        3: {'code': 'isco_level_3_code', 'label': 'isco_level_3_label', 'parent_join_col_next': 'isco_level_2_code_join'}, # Minor Group
        2: {'code': 'isco_level_2_code', 'label': 'isco_level_2_label', 'parent_join_col_next': 'isco_level_1_code_join'}, # Sub-Major Group
        1: {'code': 'isco_level_1_code', 'label': 'isco_level_1_label', 'parent_join_col_next': None}                     # Major Group
    }

    # --- Initial Join for Level 4 --- 
    level_4_info = hierarchy_levels[4]
    df = df.join(
        df_hierarchy.select(['code', 'label', 'parent_code']), # Select join key + payload
        left_on='isco_group',       # Use occupation's ISCO code
        right_on='code',            # Match hierarchy code
        how='left'
        # No suffix
    ).rename({
        'label': level_4_info['label'],               # Rename added label -> isco_level_4_label
        'parent_code': level_4_info['parent_join_col_next'] # Rename added parent_code -> isco_level_3_code_join
    })

    # Create Level 4 code column: if join successful, it's the isco_group value
    df = df.with_columns(
        pl.when(pl.col(level_4_info['label']).is_not_null())
        .then(pl.col('isco_group'))
        .otherwise(None)
        .alias(level_4_info['code']) # isco_level_4_code
    )

    # Drop the 'code' column from the right side of the join (df_hierarchy)
    if 'code' in df.columns:
        df = df.drop('code')

    # --- Iterative Joins for Levels 3, 2, 1 ---
    for level in range(3, 0, -1):
        current_level_info = hierarchy_levels[level]
        parent_level_info = hierarchy_levels[level + 1]
        # Column in df containing the parent code to join on (created in previous iteration)
        join_col_left = parent_level_info['parent_join_col_next'] 

        # Check if the join column exists before proceeding
        if join_col_left not in df.columns:
             logging.warning(f"Join column '{join_col_left}' not found for level {level}. Skipping subsequent hierarchy levels.")
             # Add null columns for this level and potentially break if needed
             df = df.with_columns([
                 pl.lit(None, dtype=pl.Utf8).alias(current_level_info['code']),
                 pl.lit(None, dtype=pl.Utf8).alias(current_level_info['label'])
             ])
             # Depending on desired behavior, you might want to 'continue' or 'break' here
             continue # Continue to try lower levels if possible, they might join from a different branch

        df = df.join(
            df_hierarchy.select(['code', 'label', 'parent_code']), # Select join key + payload
            left_on=join_col_left,    # Use parent code from previous level
            right_on='code',          # Match hierarchy code
            how='left'
            # No suffix
        )

        # Rename the newly added payload columns ('label', 'parent_code')
        rename_map = {'label': current_level_info['label']}
        if current_level_info['parent_join_col_next']:
            rename_map['parent_code'] = current_level_info['parent_join_col_next']
        df = df.rename(rename_map)

        # Create the code column for the current level: if join successful, it's the join_col_left value
        df = df.with_columns(
            pl.when(pl.col(current_level_info['label']).is_not_null())
            .then(pl.col(join_col_left))
            .otherwise(None)
            .alias(current_level_info['code']) # e.g., isco_level_3_code
        )

        # Drop the 'code' column from the right side of the join (df_hierarchy)
        if 'code' in df.columns:
            df = df.drop('code')
        
        # Drop the parent_code column if it was added but not renamed (level 1)
        if 'parent_code' in df.columns and not current_level_info['parent_join_col_next']:
            df = df.drop('parent_code')

    # Clean up intermediate join columns (parent codes used for joining)
    join_parent_cols = [info['parent_join_col_next'] for info in hierarchy_levels.values() if info['parent_join_col_next']]
    df = df.drop([col for col in join_parent_cols if col in df.columns])

    # --- Reorder columns ---
    logging.info("Reordering columns...")
    priority_order = [
        'occupation_uri', 'occupation_name', FINAL_ALT_NAMES_COL,
        'occupation_description', 'occupation_definition',
        # --- ISCO Hierarchy --- Start
        'isco_group', # Original ISCO code from source
        'isco_code', # ISCO code from joined ISCOGroups table (likely same as isco_group)
        'isco_group_name',
        'isco_level_1_code', 'isco_level_1_label',
        'isco_level_2_code', 'isco_level_2_label',
        'isco_level_3_code', 'isco_level_3_label',
        'isco_level_4_code', 'isco_level_4_label',
        # --- ISCO Hierarchy --- End
        'broader_occupation_uris', 'narrower_occupation_uris', 'parent_occupations',
        'essential_skills', 'optional_skills', 'essential_technical_skills',
        'essential_knowledge', 'essential_competences', 'digital_skills',
        'green_skills', 'transversal_skills', 'language_skills',
        'is_green', 'is_digital'
    ]
    current_columns = df.columns
    final_column_order = [col for col in priority_order if col in current_columns]
    remaining_columns = sorted([col for col in current_columns if col not in final_column_order])
    final_column_order.extend(remaining_columns)
    df = df.select(final_column_order)

    # --- Save Output ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Saving final data to Parquet file: {OUTPUT_PATH}")
    try:
        df.write_parquet(OUTPUT_PATH)
        logging.info("Successfully saved Parquet file.")
    except Exception as e:
        logging.error(f"Error writing Parquet file: {e}")
        exit(1)

if __name__ == "__main__":
    main()
