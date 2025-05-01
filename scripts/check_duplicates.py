import polars as pl
import logging
import duckdb
import os

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

RAW_OCCUPATIONS_CSV_PATH = 'data/raw/esco/1.2.0/occupations_en.csv' # Path to raw occupations
JOB_OFFERS_CSV_PATH = 'data/raw/datamarket/datamarket_job_offers_victoriano.csv'
ESCO_PROFILES_PARQUET_PATH = 'data/processed/esco/esco_occupation_profiles.parquet'
ISCO_HIERARCHY_PARQUET_PATH = 'data/derived/isco_hierarchy.parquet' # Path to hierarchy
TARGET_COUNTRY = 'United Kingdom'

# --- Database and View Paths (needed for view check) ---
DB_PATH = 'data/duckdb/esco_dataset_1.2.0.duckdb'
VIEW_SQL_PATH = 'sql/esco/occupation_profile_view.sql'

# --- Helper function to find duplicates ---
def find_duplicates(df, column_name):
    """Finds and prints duplicate values in a specified column."""
    duplicates = (
        df.group_by(column_name)
        .agg(pl.count().alias('count'))
        .filter(pl.col('count') > 1)
        .sort('count', descending=True)
    )
    return duplicates

# --- Helper function to check view output ---
def check_processed_profiles_uri(db_path, view_sql_path):
    """Checks for duplicate occupation_uri in the output of the SQL view."""
    logging.info(f"\nChecking occupation_uri uniqueness in view defined by: {view_sql_path}")
    if not os.path.exists(db_path):
        logging.error(f"Database file not found at {db_path}. Skipping view check.")
        return
    if not os.path.exists(view_sql_path):
        logging.error(f"View SQL file not found at {view_sql_path}. Skipping view check.")
        return
        
    try:
        con = duckdb.connect(database=db_path, read_only=False) # Ensure write access if view needs creating
        # Ensure view exists by executing its definition
        logging.info("Executing view definition...")
        with open(view_sql_path, 'r', encoding='utf-8') as f:
            view_sql = f.read()
        con.execute(view_sql)
        
        # Query the view for occupation_uri and occupation_name
        logging.info("Querying view for occupation_uri and occupation_name...")
        query = "SELECT occupation_uri, occupation_name FROM occupation_profile"
        df_view = con.execute(query).pl()
        con.close()
        logging.info("Database connection closed.")

        if df_view.height == 0:
            logging.warning("View query returned no rows. Cannot check for duplicates.")
            return
            
        # Check for duplicates using the helper function
        logging.info("Checking for duplicate occupation_uri in view results...")
        uri_duplicates = find_duplicates(df_view, 'occupation_uri') # Uses the helper
        
        if uri_duplicates.height > 0:
            print("\n--- Duplicate occupation_uri found in occupation_profile VIEW output --- DANGER! ---")
            print(uri_duplicates)
        else:
            print("\n--- occupation_uri is unique in occupation_profile VIEW output --- OK ---")
            
        # --- Check occupation_name uniqueness in VIEW output ---
        logging.info("Checking for duplicate occupation_name in view results (after cleaning)...")
        df_view_cleaned_name = df_view.with_columns(
            pl.col('occupation_name').str.strip_chars().alias('occupation_name_cleaned')
        )
        name_duplicates = find_duplicates(df_view_cleaned_name, 'occupation_name_cleaned')
        
        if name_duplicates.height > 0:
            print("\n--- Duplicate occupation_name found in occupation_profile VIEW output --- Problem Origin? ---")
            print(name_duplicates)
        else:
            print("\n--- occupation_name is unique in occupation_profile VIEW output --- OK ---")

    except Exception as e:
        logging.error(f"Failed to check processed profiles URI from view: {e}")
        # Ensure connection is closed if error occurs mid-way
        try:
            con.close()
            logging.info("Database connection closed after error.")
        except:
            pass 

def check_isco_hierarchy_duplicates(hierarchy_path):
    """Checks for duplicate isco_group in the ISCO hierarchy Parquet file."""
    logging.info(f"\nChecking isco_group uniqueness in hierarchy file: {hierarchy_path}")
    if not os.path.exists(hierarchy_path):
        logging.error(f"Hierarchy file not found at {hierarchy_path}. Skipping hierarchy check.")
        return
        
    try:
        df_hierarchy = pl.read_parquet(hierarchy_path)
        
        if df_hierarchy.height == 0:
            logging.warning("Hierarchy file is empty. Cannot check for duplicates.")
            return
            
        # Check for duplicates using the helper function
        logging.info("Checking for duplicate isco_group in hierarchy file...")
        # Ensure the column name is correct - assuming it's 'code'
        if 'code' not in df_hierarchy.columns:
             logging.error(f"Column 'code' not found in hierarchy file. Available columns: {df_hierarchy.columns}")
             return
             
        # Let's assume the key column in the hierarchy is 'code'
        isco_duplicates = find_duplicates(df_hierarchy, 'code') # Uses the helper
        
        if isco_duplicates.height > 0:
            print("\n--- Duplicate isco_group ('code') found in isco_hierarchy.parquet --- CAUSE IDENTIFIED! ---")
            print(isco_duplicates)
        else:
            print("\n--- isco_group ('code') is unique in isco_hierarchy.parquet --- OK (Unexpected) ---")

    except Exception as e:
        logging.error(f"Failed to check ISCO hierarchy duplicates: {e}")

# --- Main Script Logic ---
def main():
    # --- Check Raw Occupations File --- 
    logging.info(f"Checking for duplicates in raw occupations file: {RAW_OCCUPATIONS_CSV_PATH}")
    try:
        df_raw_occ = pl.read_csv(RAW_OCCUPATIONS_CSV_PATH)
        # Clean label just in case (trimming)
        df_raw_occ_cleaned = df_raw_occ.with_columns(
            pl.col('preferredLabel').str.strip_chars().alias('preferredLabel_cleaned')
        )
        
        raw_occ_duplicates = find_duplicates(df_raw_occ_cleaned, 'preferredLabel_cleaned')
        if raw_occ_duplicates.height > 0:
            print("\n--- Duplicates found in cleaned 'preferredLabel' in Raw Occupations CSV ---")
            print(raw_occ_duplicates)
            # Optional: Exit early if source duplicates are found?
            # exit(1) 
        else:
            print("\n--- No duplicates found in cleaned 'preferredLabel' in Raw Occupations CSV ---")
            
        # --- Check conceptUri uniqueness in Raw Occupations ---
        logging.info("Checking conceptUri uniqueness in raw occupations...")
        uri_duplicates = find_duplicates(df_raw_occ, 'conceptUri') # Use original df
        if uri_duplicates.height > 0:
            print("\n--- Duplicate conceptUri found in Raw Occupations CSV ---")
            print(uri_duplicates)
        else:
            print("\n--- conceptUri is unique in Raw Occupations CSV ---")
            
    except Exception as e:
        logging.error(f"Failed to process raw occupations CSV: {e}")
        # Decide if we should continue checking other files if this one fails
        # exit(1)

    # --- Check Job Offers --- 
    logging.info(f"\nChecking for duplicates in job offers: {JOB_OFFERS_CSV_PATH}")
    try:
        df_jobs = pl.read_csv(JOB_OFFERS_CSV_PATH)
        # Filter and clean
        df_jobs_uk_cleaned = (
            df_jobs.filter(pl.col('country_name') == TARGET_COUNTRY)
            .with_columns(pl.col('esco_role').str.strip_chars().alias('esco_role_cleaned'))
        )
        
        if df_jobs_uk_cleaned.height == 0:
             logging.warning(f"No job offers found for {TARGET_COUNTRY} to check.")
        else:
            job_duplicates = find_duplicates(df_jobs_uk_cleaned, 'esco_role_cleaned')
            if job_duplicates.height > 0:
                print(f"\n--- Duplicates found in cleaned 'esco_role' for {TARGET_COUNTRY} Job Offers ---")
                print(job_duplicates)
            else:
                print(f"\n--- No duplicates found in cleaned 'esco_role' for {TARGET_COUNTRY} Job Offers ---")
                
    except Exception as e:
        logging.error(f"Failed to process job offers CSV: {e}")

    # --- Check ESCO Profiles --- 
    logging.info(f"\nChecking for duplicates in ESCO profiles: {ESCO_PROFILES_PARQUET_PATH}")
    try:
        df_esco = pl.read_parquet(ESCO_PROFILES_PARQUET_PATH)
        # Clean
        df_esco_cleaned = (
            df_esco.with_columns(pl.col('occupation_name').str.strip_chars().alias('occupation_name_cleaned'))
        )
        
        esco_duplicates = find_duplicates(df_esco_cleaned, 'occupation_name_cleaned')
        if esco_duplicates.height > 0:
            print("\n--- Duplicates found in cleaned 'occupation_name' in ESCO Profiles ---")
            print(esco_duplicates)
        else:
            print("\n--- No duplicates found in cleaned 'occupation_name' in ESCO Profiles ---")
            
    except Exception as e:
        logging.error(f"Failed to process ESCO profiles Parquet: {e}")
        
    # --- Check occupation_uri uniqueness in the VIEW output ---
    check_processed_profiles_uri(DB_PATH, VIEW_SQL_PATH)

    # --- Check isco_group uniqueness in the ISCO Hierarchy file ---
    check_isco_hierarchy_duplicates(ISCO_HIERARCHY_PARQUET_PATH)

if __name__ == "__main__":
    main()
