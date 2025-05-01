import duckdb
import polars as pl
import os
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = 'data/duckdb/esco_dataset_1.2.0.duckdb'
VIEW_SQL_PATH = 'sql/esco/occupation_profile_view.sql'
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

    # --- Reorder columns ---
    logging.info("Reordering columns...")
    priority_order = [
        'occupation_uri', 'occupation_name', FINAL_ALT_NAMES_COL,
        'occupation_description', 'occupation_definition',
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
