import polars as pl
import os
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

JOB_OFFERS_CSV_PATH = 'data/raw/datamarket/datamarket_job_offers_victoriano.csv'
ESCO_PROFILES_PARQUET_PATH = 'data/processed/esco/esco_occupation_profiles.parquet'
OUTPUT_DIR = 'data/processed/datamarket'
OUTPUT_FILENAME = 'uk_job_offers_enriched.parquet'
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

TARGET_COUNTRY = 'United Kingdom'

# --- Main Script Logic ---
def main():
    # --- Load Data ---
    logging.info(f"Loading job offers from: {JOB_OFFERS_CSV_PATH}")
    try:
        df_jobs = pl.read_csv(JOB_OFFERS_CSV_PATH)
    except Exception as e:
        logging.error(f"Failed to load job offers CSV: {e}")
        exit(1)

    logging.info(f"Loading ESCO profiles from: {ESCO_PROFILES_PARQUET_PATH}")
    try:
        df_esco = pl.read_parquet(ESCO_PROFILES_PARQUET_PATH)
    except Exception as e:
        logging.error(f"Failed to load ESCO profiles Parquet: {e}")
        exit(1)

    # --- Clean and Filter Job Offers ---
    logging.info(f"Filtering job offers for country: {TARGET_COUNTRY}")
    df_jobs_uk = df_jobs.filter(pl.col('country_name') == TARGET_COUNTRY)

    if df_jobs_uk.height == 0:
        logging.warning(f"No job offers found for country '{TARGET_COUNTRY}'. Exiting.")
        exit(0)

    logging.info("Cleaning 'esco_role' column...")
    df_jobs_uk = df_jobs_uk.with_columns(
        pl.col('esco_role').str.strip_chars().alias('esco_role_cleaned')
    )

    # --- Prepare ESCO Profiles for Join ---
    # Ensure occupation_name is clean as well (though likely already is)
    df_esco = df_esco.with_columns(
        pl.col('occupation_name').str.strip_chars().alias('occupation_name_cleaned')
    )

    # --- Join Data ---
    logging.info("Joining UK job offers with ESCO profiles...")
    df_enriched = df_jobs_uk.join(
        df_esco,
        left_on='esco_role_cleaned',
        right_on='occupation_name_cleaned',
        how='left'
    )

    # --- Clean Up and Save --- 
    # Conditionally drop intermediate columns 
    columns_to_drop = []
    if 'esco_role_cleaned' in df_enriched.columns:
        columns_to_drop.append('esco_role_cleaned')
    if 'occupation_name_cleaned' in df_enriched.columns:
        columns_to_drop.append('occupation_name_cleaned')
        
    if columns_to_drop:
        df_enriched = df_enriched.drop(columns_to_drop)
    
    # Drop duplicate occupation_name if join was successful and created a _right column
    if 'occupation_name_right' in df_enriched.columns:
         # Assuming the original esco_role is preferred over the matched occupation_name
         if 'occupation_name' in df_enriched.columns:
            df_enriched = df_enriched.drop('occupation_name') 
         df_enriched = df_enriched.rename({'occupation_name_right': 'occupation_name'})

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Saving enriched UK job offers data to: {OUTPUT_PATH}")
    try:
        df_enriched.write_parquet(OUTPUT_PATH)
        logging.info(f"Successfully saved enriched data for {TARGET_COUNTRY}.")
    except Exception as e:
        logging.error(f"Error writing Parquet file: {e}")
        exit(1)

if __name__ == "__main__":
    main()
