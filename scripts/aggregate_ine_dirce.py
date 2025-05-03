# scripts/aggregate_ine_dirce.py
import polars as pl
import os

# Define paths relative to the project root directory
input_file = "data/derived/ine_dirce_empresas_filtered.parquet"
output_dir = "data/processed/ine_dirce"
output_file = os.path.join(output_dir, "ine_dirce_aggregated_by_activity.parquet")

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

print(f"Reading file: {input_file}")

try:
    # Read the processed Parquet file
    df = pl.read_parquet(input_file)
    print("Input shape:", df.shape)
    print("Input columns:", df.columns)

    # --- Add Pre-filtering Step ---
    conditions_to_exclude = ['Otras formas jurídicas', 'Personas físicas']
    df_pre_filtered = df.filter(
        ~pl.col("Condición jurídica").is_in(conditions_to_exclude)
    )
    print(f"Shape after excluding {conditions_to_exclude}:", df_pre_filtered.shape)
    # --- End Pre-filtering Step ---


    # --- Step 1: Filter by code_length (using pre-filtered data) ---
    df_code3_filtered = df_pre_filtered.filter(pl.col("code_length") == 3) # Renamed for clarity
    print("Shape after filtering code_length==3:", df_code3_filtered.shape)

    if df_code3_filtered.height == 0:
        print("No data found matching the criteria. Exiting.")
    else:
        # --- Step 2: Aggregate Total per Year (using all years in df_code3_filtered) ---
        years = sorted(df_code3_filtered["Periodo"].unique().to_list())
        year_cols = {str(year): f"Total_{year}" for year in years}
        year_agg = (
            df_code3_filtered.pivot( # Use df_code3_filtered
                index="Actividad principal",
                on="Periodo", # Changed from columns to on
                values="Total",
                aggregate_function="sum"
            )
            .rename(year_cols)
        )
        print(f"Aggregated years. Shape: {year_agg.shape}")

        # --- Create 2024 specific DataFrame ---
        df_2024_filtered = df_code3_filtered.filter(pl.col("Periodo") == 2024)
        print(f"Shape for 2024 only data: {df_2024_filtered.shape}")

        if df_2024_filtered.height == 0:
             print("Warning: No data found for the year 2024. Stratum and Condition aggregations will be empty.")
             # Create empty placeholders if needed, or handle downstream
             stratum_agg = year_agg.select("Actividad principal") # Start with just keys
             condition_agg = year_agg.select("Actividad principal")
             stratum_cols = []
             condition_cols = []
        else:
            # --- Step 3: Aggregate by Estrato de asalariados (using 2024 data only) ---
            stratum_agg = (
                df_2024_filtered.pivot( # Use df_2024_filtered
                    index="Actividad principal",
                    on="Estrato de asalariados", # Changed from columns to on
                    values="Total",
                    aggregate_function="sum"
                )
            )
            stratum_cols = [col for col in stratum_agg.columns if col != "Actividad principal"]
            print(f"Aggregated strata (2024 only). Shape: {stratum_agg.shape}")

            # --- Step 4: Aggregate by Condición jurídica (using 2024 data only) ---
            condition_agg = (
                df_2024_filtered.pivot( # Use df_2024_filtered
                    index="Actividad principal",
                    on="Condición jurídica", # Changed from columns to on
                    values="Total",
                    aggregate_function="sum"
                )
            )
            condition_cols = [col for col in condition_agg.columns if col != "Actividad principal"]
            print(f"Aggregated conditions (2024 only). Shape: {condition_agg.shape}")


        # --- Step 5: Join Aggregations ---
        # Start with year_agg (all years) and join the 2024-specific aggregations
        final_agg = year_agg.join(
            stratum_agg, on="Actividad principal", how="left" # Join 2024 stratum data
        ).join(
            condition_agg, on="Actividad principal", how="left" # Join 2024 condition data
        )

        # Fill any NaNs resulting from pivots/joins with 0
        final_agg = final_agg.fill_null(0)
        print(f"Joined aggregations. Shape: {final_agg.shape}")

        # --- Step 6: Calculate Overall Total (for 2024) and Percentages ---
        base_year_for_pct = "Total_2024"
        if base_year_for_pct not in final_agg.columns:
             print(f"Warning: {base_year_for_pct} column not found for percentage calculation. Percentages will be null/zero.")
             # Add a dummy column to avoid errors if it's missing, percentages will be 0/null
             final_agg = final_agg.with_columns(pl.lit(0).cast(pl.Int64).alias(base_year_for_pct))

        # Add Total_Overall_for_pct alias for consistency in calculation code
        final_agg = final_agg.with_columns(pl.col(base_year_for_pct).alias("Total_Overall_for_pct"))


        # Rename absolute stratum/condition columns (from 2024 data) and calculate percentages based on Total_2024
        stratum_abs_pct_cols = []
        for col_name in stratum_cols:
            abs_col = f"Estrato_{col_name}_abs"
            pct_col = f"Estrato_{col_name}_pct"
            stratum_abs_pct_cols.extend([
                pl.col(col_name).alias(abs_col),
                ((pl.col(col_name) / pl.col("Total_Overall_for_pct")) * 100).round(2).alias(pct_col)
            ])

        condition_abs_pct_cols = []
        for col_name in condition_cols:
            abs_col = f"Condicion_{col_name}_abs"
            pct_col = f"Condicion_{col_name}_pct"
            condition_abs_pct_cols.extend([
                pl.col(col_name).alias(abs_col),
                ((pl.col(col_name) / pl.col("Total_Overall_for_pct")) * 100).round(2).alias(pct_col)
            ])

        final_agg = final_agg.with_columns(stratum_abs_pct_cols + condition_abs_pct_cols)

        # Drop original stratum/condition columns (which contain the 2024 data) and the helper Total_Overall_for_pct
        # Check if stratum_cols and condition_cols exist before dropping (in case 2024 data was empty)
        cols_to_drop = ["Total_Overall_for_pct"]
        if stratum_cols:
            cols_to_drop.extend(stratum_cols)
        if condition_cols:
            cols_to_drop.extend(condition_cols)
        final_agg = final_agg.drop(cols_to_drop)


        print(f"Final aggregated shape: {final_agg.shape}")

        # --- Add Growth Calculation (2020-2024) ---
        start_year_col = "Total_2020"
        end_year_col = "Total_2024"
        growth_col_name = "Growth_2020_2024_pct"

        if start_year_col in final_agg.columns and end_year_col in final_agg.columns:
            print(f"Calculating growth between {start_year_col} and {end_year_col}")
            final_agg = final_agg.with_columns(
                (
                    pl.when(pl.col(start_year_col) != 0)
                    .then(
                        (
                            (pl.col(end_year_col) - pl.col(start_year_col)) / pl.col(start_year_col)
                        ) * 100
                    )
                    # Handle case where start is 0: if end is also 0, growth is 0. If end > 0, growth is infinite (represent as None or a large number? Using None)
                    .when((pl.col(start_year_col) == 0) & (pl.col(end_year_col) == 0))
                    .then(0.0)
                    .otherwise(None) # Represents infinite growth when start is 0 and end > 0
                ).round(2).alias(growth_col_name) # Round to 2 decimal places
            )
        else:
            print(f"Warning: Cannot calculate growth. Missing one or both columns: {start_year_col}, {end_year_col}")
            # Add a null column if calculation wasn't possible
            final_agg = final_agg.with_columns(pl.lit(None).cast(pl.Float64).alias(growth_col_name))
        # --- End Growth Calculation ---

        # --- Calculate Median YoY Growth (2020-2024) ---
        years = sorted([int(col.split('_')[1]) for col in final_agg.columns if col.startswith("Total_") and col.split('_')[1].isdigit()])
        median_growth_col_name = "Median_YoY_Growth_pct"
        yoy_growth_exprs = []

        if len(years) > 1:
            print(f"Calculating YoY growth for years: {', '.join(map(str, years))}")
            min_year, max_year = min(years), max(years)
            if min_year <= 2020 and max_year >= 2024:
                for i in range(len(years) - 1):
                    year_start = years[i]
                    year_end = years[i+1]
                    # Only consider years within the 2020-2024 range for median calc
                    if 2020 <= year_start < 2024:
                        col_start = f"Total_{year_start}"
                        col_end = f"Total_{year_end}"
                        if col_start in final_agg.columns and col_end in final_agg.columns:
                            print(f"  Adding growth calculation: {year_start}-{year_end}")
                            yoy_growth_exprs.append(
                                (
                                    pl.when(pl.col(col_start) != 0)
                                    .then(((pl.col(col_end) - pl.col(col_start)) / pl.col(col_start)) * 100)
                                    .when(pl.col(col_end) == 0) # Start is 0, End is 0 -> 0% growth
                                    .then(0.0)
                                    .otherwise(None) # Start is 0, End > 0 -> infinite growth (None)
                                ).round(2)
                            )
                        else:
                            print(f"  Skipping {year_start}-{year_end}: Columns missing.")

                if yoy_growth_exprs:
                    final_agg = final_agg.with_columns(
                        pl.concat_list(yoy_growth_exprs).alias("_yoy_growths")
                    )
                    final_agg = final_agg.with_columns(
                        pl.col("_yoy_growths").list.median().alias(median_growth_col_name)
                    ).drop("_yoy_growths")
                    print(f"Calculated {median_growth_col_name}")
                else:
                    print("Warning: No valid YoY growth periods found for median calculation.")
                    final_agg = final_agg.with_columns(pl.lit(None).cast(pl.Float64).alias(median_growth_col_name))
            else:
                 print(f"Warning: Cannot calculate median YoY growth for 2020-2024. Required years missing.")
                 final_agg = final_agg.with_columns(pl.lit(None).cast(pl.Float64).alias(median_growth_col_name))
        else:
            print("Warning: Not enough year columns to calculate YoY growth.")
            final_agg = final_agg.with_columns(pl.lit(None).cast(pl.Float64).alias(median_growth_col_name))
        # --- End Median YoY Growth Calculation ---

        # --- Reorder Columns --- 
        all_cols = final_agg.columns
        if "Actividad principal" in all_cols and growth_col_name in all_cols and median_growth_col_name in all_cols:
            # Identify column groups
            fixed_start_cols = ["Actividad principal", median_growth_col_name, growth_col_name]
            total_year_cols = sorted([col for col in all_cols if col.startswith("Total_") and col[6:].isdigit()], reverse=True)

            # Define semantic order for Estrato columns
            estrato_order = [
                "Sin asalariados",
                "De 1 a 2",
                "De 3 a 5",
                "De 6 a 9",
                "De 10 a 19",
                "De 20 a 49",
                "De 50 a 99",
                "De 100 a 199",
                "De 200 a 249",
                "De 250 a 999",
                "De 1000 a 4999",
                "De 5000 o más asalariados"
            ]
            estrato_abs_cols_ordered = [f"Estrato_{e}_abs" for e in estrato_order if f"Estrato_{e}_abs" in all_cols]
            estrato_pct_cols_ordered = [f"Estrato_{e}_pct" for e in estrato_order if f"Estrato_{e}_pct" in all_cols]

            # Keep Condicion columns sorted alphabetically for now
            condicion_abs_cols = sorted([col for col in all_cols if col.startswith("Condicion_") and col.endswith("_abs")])
            condicion_pct_cols = sorted([col for col in all_cols if col.startswith("Condicion_") and col.endswith("_pct") and col not in fixed_start_cols])

            # Combine Abs and Pct columns with desired order
            final_abs_cols = estrato_abs_cols_ordered + condicion_abs_cols
            final_pct_cols = estrato_pct_cols_ordered + condicion_pct_cols

            # Combine in desired order
            ordered_cols = fixed_start_cols + total_year_cols + final_abs_cols + final_pct_cols

            # Check if all original columns are accounted for
            if len(ordered_cols) == len(all_cols) and set(ordered_cols) == set(all_cols):
                final_agg = final_agg.select(ordered_cols)
                print("Columns reordered into groups (Totals, Abs, Pct).")
            else:
                print("Warning: Column mismatch during reordering. Keeping original order.")
                # Log missing/extra columns for debugging if needed
                print("  Original:", sorted(all_cols))
                print("  Expected Order:", ordered_cols)
        else:
            print("Warning: Could not reorder columns. Key columns ('Actividad principal', median growth, overall growth) missing.")

        # Update final schema printout
        print(f"Final aggregated shape (with growth): {final_agg.shape}")
        print("Final columns:", final_agg.columns)
        print("Sample of final data:\n", final_agg.head())

        # --- Step 7: Save Output ---
        final_agg.write_parquet(output_file)
        print(f"Aggregated data saved to: {output_file}")

except pl.exceptions.ComputeError as e:
    print(f"A Polars computation error occurred: {e}")
    print("This might happen if pivoting results in unexpected column names or types.")
except FileNotFoundError:
    print(f"Error: Input file not found at {input_file}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
