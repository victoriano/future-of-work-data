import polars as pl
import logging
import os

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

JOB_OFFERS_CSV_PATH = 'data/raw/datamarket/datamarket_job_offers_victoriano.csv'
ESCO_PROFILES_PARQUET_PATH = 'data/processed/esco/esco_occupation_profiles.parquet'
OUTPUT_DIR = 'data/processed/datamarket'
OUTPUT_FILENAME = 'job_offers_aggregated_by_occupation.parquet'
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

# Countries for specific aggregation
TARGET_COUNTRIES = {
    'uk': 'United Kingdom',
    'de': 'Germany',
    'es': 'Spain'
}

# --- Main Script Logic ---
def main():
    logging.info(f"Loading job offers from: {JOB_OFFERS_CSV_PATH}")
    try:
        df_jobs = pl.read_csv(JOB_OFFERS_CSV_PATH)
        # Basic cleaning: Ensure numeric types, handle potential errors
        df_jobs = df_jobs.with_columns(
            pl.col('n_job_offers').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('median_min_salary').cast(pl.Float64, strict=False),
            pl.col('median_max_salary').cast(pl.Float64, strict=False),
            pl.col('esco_role').str.strip_chars().alias('esco_role_cleaned')
        )
        logging.info(f"Job offers loaded and basic cleaning done. Shape: {df_jobs.shape}")
    except Exception as e:
        logging.error(f"Failed to load or clean job offers CSV: {e}")
        return
        
    logging.info(f"Loading ESCO profiles from: {ESCO_PROFILES_PARQUET_PATH}")
    try:
        df_esco = pl.read_parquet(ESCO_PROFILES_PARQUET_PATH)
        # Clean occupation name for joining
        df_esco = df_esco.with_columns(
            pl.col('occupation_name').str.strip_chars().alias('occupation_name_cleaned')
        )
         # Keep only essential columns for joining + final output base
        esco_cols_to_keep = [
            'occupation_uri', 'occupation_name', 'occupation_name_cleaned',
            'isco_level_1_code', 'isco_level_1_label',
            'isco_level_2_code', 'isco_level_2_label',
            'isco_level_3_code', 'isco_level_3_label',
            'isco_level_4_code', 'isco_level_4_label',
            'description'
        ]
        # Filter out columns not in the dataframe
        esco_cols_to_keep = [col for col in esco_cols_to_keep if col in df_esco.columns]
        df_esco_base = df_esco.select(esco_cols_to_keep).unique(subset=['occupation_uri'], keep='first')
        
        logging.info(f"ESCO profiles loaded and prepared. Shape: {df_esco_base.shape}")
    except Exception as e:
        logging.error(f"Failed to load or process ESCO profiles Parquet: {e}")
        return

    # --- Join Data ---
    logging.info("Joining job offers with ESCO profiles...")
    df_joined = df_jobs.join(
        df_esco_base,
        left_on='esco_role_cleaned',
        right_on='occupation_name_cleaned',
        how='left',
        suffix='_esco' # Add suffix to avoid potential column name conflicts
    )
    logging.info(f"Join complete. Shape after join: {df_joined.shape}")
    
    # Filter out rows where the join failed (no matching ESCO profile)
    initial_rows = df_joined.height
    df_joined = df_joined.filter(pl.col('occupation_uri').is_not_null())
    rows_dropped = initial_rows - df_joined.height
    if rows_dropped > 0:
        logging.warning(f"Dropped {rows_dropped} rows where job offer role did not match any ESCO occupation name.")
    
    # --- Aggregation (New Strategy) --- 
    logging.info("Starting aggregation by occupation (Pivoting Strategy)...")
    
    # 1. Calculate Global Aggregates
    logging.info("Calculating global aggregates...")
    global_agg_expressions = [
        pl.sum('n_job_offers').alias('total_job_offers_global'),
        pl.median('median_min_salary').alias('median_min_salary_global'),
        pl.median('median_max_salary').alias('median_max_salary_global'),
        pl.n_unique('country_name').alias('n_countries_present')
    ]
    df_global_aggregated = df_joined.group_by('occupation_uri').agg(global_agg_expressions)
    logging.info(f"Global aggregation complete. Shape: {df_global_aggregated.shape}")

    # 2. Prepare data for Pivoting (Filter for target countries)
    logging.info("Filtering data for country-specific pivoting...")
    target_country_names = list(TARGET_COUNTRIES.values())
    df_pivot_source = df_joined.filter(pl.col('country_name').is_in(target_country_names))
    logging.info(f"Data for pivoting filtered. Shape: {df_pivot_source.shape}")

    # 3. Pivot for Country-Specific Aggregates
    # We need to aggregate *before* pivoting if multiple entries per occupation/country exist
    # Although the source data seems aggregated, let's ensure it.
    logging.info("Aggregating within country before pivoting...")
    df_pivot_agg = df_pivot_source.group_by(['occupation_uri', 'country_name']).agg(
        pl.sum('n_job_offers').alias('n_job_offers'),
        pl.median('median_min_salary').alias('median_min_salary'),
        pl.median('median_max_salary').alias('median_max_salary')
    )

    logging.info("Pivoting country-specific data...")
    try:
        df_pivoted = df_pivot_agg.pivot(
            index='occupation_uri', 
            on='country_name', 
            values=['n_job_offers', 'median_min_salary', 'median_max_salary']
        )
        logging.info(f"Pivoting complete. Shape: {df_pivoted.shape}")
        
        # Rename pivoted columns to match desired format (e.g., n_job_offers_uk)
        rename_mapping = {}
        for short_code, full_name in TARGET_COUNTRIES.items():
            rename_mapping[f'n_job_offers_{full_name}'] = f'total_job_offers_{short_code}'
            rename_mapping[f'median_min_salary_{full_name}'] = f'median_min_salary_{short_code}'
            rename_mapping[f'median_max_salary_{full_name}'] = f'median_max_salary_{short_code}'
        
        df_pivoted = df_pivoted.rename(rename_mapping)
        logging.info("Pivoted columns renamed.")
        
    except Exception as e:
        logging.error(f"Pivoting failed: {e}. This might happen if there are no rows for any target country after filtering.")
        # Create an empty dataframe with occupation_uri if pivot fails, to allow subsequent joins
        df_pivoted = pl.DataFrame({'occupation_uri': []}).with_columns(pl.col('occupation_uri').cast(df_joined['occupation_uri'].dtype))

    # --- Final Joins and Save ---
    logging.info("Joining base ESCO profiles with global aggregates...")
    df_final = df_esco_base.join(
        df_global_aggregated, 
        on='occupation_uri', 
        how='left'
    )

    logging.info("Joining result with pivoted country-specific aggregates...")
    df_final = df_final.join(
        df_pivoted,
        on='occupation_uri',
        how='left'
    )
    
    # Fill nulls for aggregated columns introduced by left joins
    logging.info("Filling nulls for aggregated columns...")
    
    # Step 1: Fill global counts
    global_cols_to_fill_zero = ['total_job_offers_global', 'n_countries_present']
    df_final = df_final.with_columns([
        pl.col(col).fill_null(0) for col in global_cols_to_fill_zero if col in df_final.columns
    ])

    # Step 2: Fill country-specific counts
    country_cols_to_fill_zero = [col for col in df_final.columns if col.startswith('total_job_offers_')]
    if country_cols_to_fill_zero:
         df_final = df_final.with_columns([
             pl.col(col).fill_null(0) for col in country_cols_to_fill_zero
         ])
    # Note: Median salary columns (global and country-specific) will remain null if no offers.

    # Drop intermediate cleaning column
    if 'occupation_name_cleaned' in df_final.columns:
        df_final = df_final.drop('occupation_name_cleaned')
        
    logging.info(f"Final joins complete. Final shape: {df_final.shape}")

    # Save the result
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Saving aggregated data to: {OUTPUT_PATH}")
    try:
        df_final.write_parquet(OUTPUT_PATH)
        logging.info("Successfully saved aggregated job offers data.")
    except Exception as e:
        logging.error(f"Failed to save final Parquet file: {e}")

if __name__ == "__main__":
    main()
