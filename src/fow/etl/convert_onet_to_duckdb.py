#!/usr/bin/env python3
"""
Convert O*NET Excel dataset to DuckDB database
This script loads all Excel files from the O*NET dataset into a DuckDB database,
allowing for fast SQL queries on the data.
"""

import os
import glob
import duckdb
import pandas as pd
from pathlib import Path

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()

# Configuration
EXCEL_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "onet", "29.2")
DB_PATH = os.path.join(PROJECT_ROOT, "data", "duckdb", "onet_dataset_29.2.duckdb")

def sanitize_table_name(name):
    """Sanitize Excel filename to be used as a SQL table name"""
    # Replace spaces and special characters
    sanitized = name.replace(" ", "_").replace(",", "").replace(".", "")
    # Remove other undesirable characters
    sanitized = ''.join(c for c in sanitized if c.isalnum() or c == '_')
    return sanitized.lower()

def main():
    # Connect to DuckDB database (will be created if it doesn't exist)
    print(f"Creating/connecting to DuckDB database at: {DB_PATH}")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    
    # Get all Excel files (excluding any temporary files like ~$)
    excel_files = [f for f in glob.glob(os.path.join(EXCEL_DIR, "*.xlsx")) if not os.path.basename(f).startswith("~$")]
    print(f"Found {len(excel_files)} Excel files to import")
    
    # Create database metadata table
    con.execute("""
        CREATE TABLE IF NOT EXISTS onet_metadata (
            file_name VARCHAR,
            table_name VARCHAR,
            original_columns VARCHAR,
            row_count INTEGER,
            import_date TIMESTAMP
        )
    """)
    
    # Import each Excel file as a table
    for excel_file in excel_files:
        file_name = os.path.basename(excel_file)
        table_name = sanitize_table_name(Path(excel_file).stem)
        print(f"Importing {file_name} as table '{table_name}'...")
        
        try:
            # Read Excel file
            df = pd.read_excel(excel_file)
            
            # Store original column names before any sanitization
            original_columns = ', '.join(df.columns.tolist())
            
            # Sanitize column names to be SQL-friendly
            df.columns = [sanitize_table_name(col) for col in df.columns]
            
            # Register dataframe as a view and create table
            con.register(f"temp_{table_name}", df)
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_{table_name}")
            
            # Get row count for verification
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  Successfully imported {row_count} rows into table '{table_name}'")
            
            # Add metadata
            con.execute(f"""
                INSERT INTO onet_metadata VALUES 
                ('{file_name}', '{table_name}', '{original_columns}', {row_count}, CURRENT_TIMESTAMP)
            """)
            
        except Exception as e:
            print(f"  Error importing {excel_file}: {e}")
    
    # Create some helpful views for common queries
    try:
        print("Creating helpful views...")
        
        # Example: View to join occupations with their skills
        con.execute("""
            CREATE OR REPLACE VIEW occupation_skills AS
            SELECT 
                o.onetsoc_code,
                o.title,
                s.element_name as skill_name,
                s.scale_id,
                s.data_value as skill_level,
                s.standard_error,
                s.lower_ci_bound,
                s.upper_ci_bound,
                s.recommend_suppress,
                s.not_relevant
            FROM occupation_data o
            JOIN skills s ON o.onetsoc_code = s.onetsoc_code
        """)
        
        # Example: View for occupation knowledge
        con.execute("""
            CREATE OR REPLACE VIEW occupation_knowledge AS
            SELECT 
                o.onetsoc_code,
                o.title,
                k.element_name as knowledge_area,
                k.scale_id,
                k.data_value as knowledge_level,
                k.standard_error,
                k.lower_ci_bound,
                k.upper_ci_bound,
                k.recommend_suppress,
                k.not_relevant
            FROM occupation_data o
            JOIN knowledge k ON o.onetsoc_code = k.onetsoc_code
        """)
        
        # Example: View for occupation work activities
        con.execute("""
            CREATE OR REPLACE VIEW occupation_work_activities AS
            SELECT 
                o.onetsoc_code,
                o.title,
                w.element_name as activity,
                w.scale_id,
                w.data_value as activity_level,
                w.standard_error,
                w.lower_ci_bound,
                w.upper_ci_bound,
                w.recommend_suppress,
                w.not_relevant
            FROM occupation_data o
            JOIN work_activities w ON o.onetsoc_code = w.onetsoc_code
        """)
        
        print("Views created successfully")
        
    except Exception as e:
        print(f"Error creating views: {e}")
    
    # Create indexes to speed up common queries
    try:
        print("Creating indexes...")
        
        # Create index on O*NET-SOC codes in main tables
        tables_to_index = [
            "occupation_data", 
            "skills", 
            "knowledge", 
            "work_activities",
            "abilities",
            "interests",
            "work_context",
            "job_zones",
            "education_training_and_experience"
        ]
        
        for table in tables_to_index:
            try:
                con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_code ON {table} (onetsoc_code)")
                print(f"  Created index on {table}.onetsoc_code")
            except Exception as e:
                print(f"  Could not create index on {table}: {e}")
        
        print("Indexes created successfully")
        
    except Exception as e:
        print(f"Error creating indexes: {e}")
    
    # Close the connection
    con.close()
    print("\nDatabase creation completed successfully!")
    print(f"You can now query the database using: duckdb {DB_PATH}")
    print("Example query: SELECT * FROM occupation_data LIMIT 10;")

if __name__ == "__main__":
    main()
