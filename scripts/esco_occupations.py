import duckdb
import polars as pl
import json
import os

# --- Configuration ---
DB_PATH = 'data/duckdb/esco_dataset_1.2.0.duckdb'
VIEW_SQL_PATH = 'sql/esco/occupation_profile_view.sql'
OUTPUT_DIR = 'data/processed/esco'
OUTPUT_FILENAME = 'esco_occupation_profiles.parquet'
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
COLUMN_TO_PROCESS = 'raw_alternative_names'
NEW_COLUMN_NAME = 'alternative_names'

def _process_single_alt_name(raw_name):
    """Processes a single raw multi-line string into a JSON array string."""
    if raw_name is None or not isinstance(raw_name, str) or not raw_name.strip():
        return '[]' # Return empty JSON array for null/empty input

    # Split by newline, handling both \n and \r\n, \r
    lines = []
    for line in raw_name.split('\n'):
        lines.extend(line.split('\r'))

    # Clean up each line and filter empty ones
    cleaned_labels = [label.strip() for label in lines if label.strip()]

    # Convert list to JSON array string
    return json.dumps(cleaned_labels)


def main():
    print(f"Connecting to database: {DB_PATH}")
    con = duckdb.connect(database=DB_PATH, read_only=False)

    print(f"Reading view definition from: {VIEW_SQL_PATH}")
    try:
        with open(VIEW_SQL_PATH, 'r', encoding='utf-8') as f:
            view_sql = f.read()
    except FileNotFoundError:
        print(f"Error: SQL file not found at {VIEW_SQL_PATH}")
        con.close()
        exit(1)
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        con.close()
        exit(1)

    print("Executing SQL to create/replace occupation_profile view...")
    try:
        con.execute(view_sql)
    except Exception as e:
        print(f"Error executing SQL view definition: {e}")
        con.close()
        exit(1)

    print("Querying occupation_profile view...")
    try:
        # Fetch data directly into a Polars DataFrame
        query = "SELECT * FROM occupation_profile ORDER BY occupation_name"
        df = con.execute(query).pl()
    except Exception as e:
        print(f"Error querying occupation_profile view: {e}")
        con.close()
        exit(1)

    # Close the database connection as it's no longer needed
    con.close()
    print("Database connection closed.")

    print(f"Processing column: {COLUMN_TO_PROCESS}")
    if COLUMN_TO_PROCESS in df.columns:
        # Apply the processing function using map_elements
        df = df.with_columns(
            pl.col(COLUMN_TO_PROCESS)
              .map_elements(_process_single_alt_name, return_dtype=pl.Utf8)
              .alias(NEW_COLUMN_NAME)
        )
        # Drop the original raw column
        df = df.drop(COLUMN_TO_PROCESS)
        print(f"Column '{COLUMN_TO_PROCESS}' processed into '{NEW_COLUMN_NAME}'.")
    else:
        print(f"Warning: Column '{COLUMN_TO_PROCESS}' not found. Skipping processing.")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Saving final data to Parquet file: {OUTPUT_PATH}")
    try:
        df.write_parquet(OUTPUT_PATH)
        print("Successfully saved Parquet file.")
    except Exception as e:
        print(f"Error writing Parquet file: {e}")
        exit(1)

if __name__ == "__main__":
    main()
