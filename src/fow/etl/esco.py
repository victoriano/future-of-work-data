#!/usr/bin/env python3
"""
Convert ESCO CSV dataset to DuckDB database
This script loads all CSV files from the ESCO dataset into a DuckDB database,
allowing for fast SQL queries on the data.
"""

import duckdb
import pandas as pd
from tqdm.auto import tqdm

from .. import pathto

CSV_DIR = pathto("data/raw/esco/1.2.0")
DB_PATH = pathto("data/duckdb/esco_dataset_1.2.0.duckdb")


def to_duckdb():
    # Connect to DuckDB database (will be created if it doesn't exist)
    print(f"Creating/connecting to DuckDB database at: {DB_PATH}")
    DB_PATH.parent.mkdir(exist_ok=True, parents=True)
    con = duckdb.connect(DB_PATH)

    # Get all CSV files
    csv_files = list(CSV_DIR.glob("*.csv"))

    # Import each CSV file as a table
    for csv_file in tqdm(csv_files, total=len(csv_files), desc="Importing CSV files"):
        table_name = csv_file.stem

        try:
            # Special handling for files that need specific settings
            if table_name == "skillsHierarchy_en":
                # Use pandas to read the file with explicit CSV options then load to DuckDB
                df = pd.read_csv(csv_file, delimiter=",", quotechar='"', escapechar="\\")
                con.register(f"temp_{table_name}", df)
                con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_{table_name}")
                con.execute(f"DROP VIEW IF EXISTS temp_{table_name}")
            else:
                # Create table directly from CSV for other files
                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} AS
                    SELECT * FROM read_csv_auto('{csv_file}', ignore_errors=false, delim=',', quote='"')
                """)

            # Get row count for verification
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        except Exception as e:
            tqdm(f"Error importing {csv_file}: {e}")

    # Create some helpful views for common queries
    try:
        print("Creating helpful views...")

        # Example: View to join occupations with their skills
        con.execute("""
            CREATE OR REPLACE VIEW occupation_skills AS
            SELECT 
                o.preferredLabel as occupation_name,
                o.conceptUri as occupation_uri,
                s.preferredLabel as skill_name,
                s.conceptUri as skill_uri,
                r.relationType
            FROM occupationSkillRelations_en r
            JOIN occupations_en o ON r.occupationUri = o.conceptUri
            JOIN skills_en s ON r.skillUri = s.conceptUri
        """)

        # Example: View for occupation hierarchy
        con.execute("""
            CREATE OR REPLACE VIEW occupation_hierarchy AS
            SELECT 
                c.conceptUri,
                c.broaderUri,
                o.preferredLabel as occupation_name,
                b.preferredLabel as broader_name
            FROM broaderRelationsOccPillar_en c
            LEFT JOIN occupations_en o ON c.conceptUri = o.conceptUri
            LEFT JOIN occupations_en b ON c.broaderUri = b.conceptUri
        """)

        print("Views created successfully")
    except Exception as e:
        print(f"Error creating views: {e}")

    # Close the connection
    con.close()
    print("\nDatabase creation completed successfully!")
    print(f"You can now query the database using: duckdb {DB_PATH}")
    print("Example query: SELECT * FROM occupations_en LIMIT 10;")
