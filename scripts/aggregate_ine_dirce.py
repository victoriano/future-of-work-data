# scripts/aggregate_ine_dirce.py
import polars as pl
import os
from collections import defaultdict

def sort_strata_columns(cols):
    # Defines the desired order for strata columns
    order = [
        "Sin asalariados", "De 1 a 2", "De 3 a 5", "De 6 a 9", "De 10 a 19",
        "De 20 a 49", "De 50 a 99", "De 100 a 199", "De 200 a 249", 
        "De 250 a 999", "De 1000 a 4999", "De 5000 o más asalariados"
    ]
    # Create a mapping from the extracted name to its index in the order list
    order_map = {name: i for i, name in enumerate(order)}
    
    def get_sort_key(col_name):
        # Extract the part of the column name that corresponds to the strata description
        parts = col_name.split('_')
        # Handle potential format variations, assuming description starts after "Estrato_"
        if len(parts) > 2:
            strata_name = " ".join(parts[1:-1]) # Join parts between Estrato_ and _abs/_pct
            return order_map.get(strata_name, float('inf')) # Use inf for any unexpected names
        return float('inf') # Default for columns not matching expected format

    return sorted(cols, key=get_sort_key)


def main():
    # Define file paths relative to the script location or project root
    script_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(script_dir, '..')) 
    input_file = os.path.join(project_root, "data", "derived", "ine_dirce_empresas_filtered.parquet")
    output_dir = os.path.join(project_root, "data", "processed", "ine_dirce")
    output_file = os.path.join(output_dir, "ine_dirce_aggregated_by_activity.parquet")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    print(f"Reading file: {input_file}")
    df_filtered = pl.read_parquet(input_file)
    print("Input shape:", df_filtered.shape)
    print("Input columns:", df_filtered.columns)

    # --- Prepare Division Mapping --- 
    df_level2 = df_filtered.filter(pl.col("code_length") == 2)
    division_map = df_level2.select(
        pl.col("activity_code").alias("division_code"), 
        pl.col("Actividad principal").alias("Division")
    ).unique(subset=["division_code"], keep="first")

    # --- Filter data for aggregation --- 
    # Exclude 'Otras formas jurídicas' and 'Personas físicas' for aggregation
    exclude_conditions = ['Otras formas jurídicas', 'Personas físicas']
    df_agg_base = df_filtered.filter(~pl.col("Condición jurídica").is_in(exclude_conditions))
    print(f"Shape after excluding {exclude_conditions}:", df_agg_base.shape)
    
    # Filter for 3-digit activity codes for main aggregation
    df_level3 = df_agg_base.filter(pl.col("code_length") == 3)
    print("Shape after filtering code_length==3:", df_level3.shape)

    # --- Aggregate Total Companies per Year (and rename) --- 
    # Pivot the table to get years as columns
    df_pivoted = df_level3.pivot(
         index="Actividad principal",
         on="Periodo", # Use 'on' instead of deprecated 'columns'
         values="Total",
         aggregate_function="sum"
     )
    # Rename year columns to 'Total_YYYY'
    year_rename_map = {col: f"Total_{col}" for col in df_pivoted.columns if col.isdigit() and len(col) == 4}
    df_pivoted = df_pivoted.rename(year_rename_map)
    
    print(f"Aggregated years. Shape: {df_pivoted.shape}")

    # --- Create 2024 specific DataFrame ---
    df_2024 = df_level3.filter(pl.col("Periodo") == 2024)
    print(f"Shape for 2024 only data: {df_2024.shape}")

    if df_2024.height == 0:
         print("Warning: No data found for the year 2024. Stratum and Condition aggregations will be empty.")
         # Create empty placeholders if needed, or handle downstream
         stratum_agg = df_pivoted.select("Actividad principal") # Start with just keys
         condition_agg = df_pivoted.select("Actividad principal")
         stratum_cols = []
         condition_cols = []
    else:
        # --- Step 3: Aggregate by Estrato de asalariados (using 2024 data only) ---
        stratum_agg = (
            df_2024.pivot( # Use df_2024_filtered
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
            df_2024.pivot( # Use df_2024_filtered
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
        final_agg = df_pivoted.join(
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
                ).round(1).alias(growth_col_name) # Round to 1 decimal place
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
                        pl.col("_yoy_growths").list.median().round(1).alias(median_growth_col_name) # Round median to 1
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

        # --- Estimate Employees 2024 ---
        strata_midpoints = {
            "Estrato_Sin asalariados_abs": 1, # Changed from 0 to 1
            "Estrato_De 1 a 2_abs": 1.5,
            "Estrato_De 3 a 5_abs": 4,
            "Estrato_De 6 a 9_abs": 7.5,
            "Estrato_De 10 a 19_abs": 14.5,
            "Estrato_De 20 a 49_abs": 34.5,
            "Estrato_De 50 a 99_abs": 74.5,
            "Estrato_De 100 a 199_abs": 149.5,
            "Estrato_De 200 a 249_abs": 224.5,
            "Estrato_De 250 a 999_abs": 624.5,
            "Estrato_De 1000 a 4999_abs": 2999.5,
            "Estrato_De 5000 o más asalariados_abs": 5000, # Using lower bound as estimate
        }

        # Check which strata columns actually exist in the aggregated data
        existing_strata_cols = [col for col in strata_midpoints if col in final_agg.columns]

        if existing_strata_cols:
            print("Calculating Estimated_Employees_2024...")
            # Calculate the sum of (companies_in_stratum * midpoint_employees)
            employee_sum_expr = pl.sum_horizontal(
                [pl.col(col) * strata_midpoints[col] for col in existing_strata_cols]
            )
            final_agg = final_agg.with_columns(
                employee_sum_expr.round(0).alias("Estimated_Employees_2024") # Round to whole number
            )
        else:
            print("Warning: No strata absolute columns found to estimate employees.")
            final_agg = final_agg.with_columns(pl.lit(None).cast(pl.Float64).alias("Estimated_Employees_2024"))

        # Calculate % of total estimated employees
        if "Estimated_Employees_2024" in final_agg.columns:
            total_estimated_employees = final_agg["Estimated_Employees_2024"].sum()
            if total_estimated_employees and total_estimated_employees != 0:
                final_agg = final_agg.with_columns(
                    (
                        (pl.col("Estimated_Employees_2024") / total_estimated_employees) * 100
                    ).round(2).alias("Estimated_Employees_pct")
                )
                print("Calculated Estimated_Employees_pct")
            else:
                print("Warning: Total estimated employees is zero or null, cannot calculate percentage.")
                final_agg = final_agg.with_columns(pl.lit(None).cast(pl.Float64).alias("Estimated_Employees_pct"))
        else:
            print("Warning: Estimated_Employees_2024 column not found, cannot calculate percentage.")

        # --- Aggregate into Simplified Size Categories --- 
        print("Aggregating into simplified size categories...")
        size_mapping = {
            "Size_Micro (0-9)": ["Estrato_Sin asalariados_abs", "Estrato_De 1 a 2_abs", "Estrato_De 3 a 5_abs", "Estrato_De 6 a 9_abs"],
            "Size_Small (10-49)": ["Estrato_De 10 a 19_abs", "Estrato_De 20 a 49_abs"],
            "Size_Medium (50-249)": ["Estrato_De 50 a 99_abs", "Estrato_De 100 a 199_abs", "Estrato_De 200 a 249_abs"],
            "Size_Large (250+)": ["Estrato_De 250 a 999_abs", "Estrato_De 1000 a 4999_abs", "Estrato_De 5000 o más asalariados_abs"]
        }

        for size_cat, strata_cols in size_mapping.items():
            abs_col_name = f"{size_cat}_abs"
            pct_col_name = f"{size_cat}_pct"
            
            # Check which source columns exist
            existing_source_cols = [col for col in strata_cols if col in final_agg.columns]

            if existing_source_cols:
                # Calculate absolute sum
                final_agg = final_agg.with_columns(
                    pl.sum_horizontal([pl.col(c) for c in existing_source_cols]).alias(abs_col_name)
                )

                # Calculate percentage
                if "Total_2024" in final_agg.columns:
                    final_agg = final_agg.with_columns(
                        pl.when(pl.col("Total_2024") != 0)
                        .then((pl.col(abs_col_name) / pl.col("Total_2024")) * 100)
                        .otherwise(0.0) # Avoid division by zero
                        .round(2)
                        .alias(pct_col_name)
                    )
                    print(f"  Calculated {abs_col_name} and {pct_col_name}")
                else:
                    print(f"  Warning: Total_2024 column missing, cannot calculate {pct_col_name}.")
                    final_agg = final_agg.with_columns(pl.lit(None).cast(pl.Float64).alias(pct_col_name))
            else:
                print(f"  Warning: No source columns found for {size_cat}, skipping {abs_col_name} and {pct_col_name}.")
                final_agg = final_agg.with_columns(pl.lit(None).cast(pl.Int64).alias(abs_col_name))
                final_agg = final_agg.with_columns(pl.lit(None).cast(pl.Float64).alias(pct_col_name))

        # --- End Estimate Employees ---

        # --- Add Division Column --- 
        # Extract 3-digit code, then 2-digit prefix
        final_agg = final_agg.with_columns(
            pl.col("Actividad principal").str.extract(r"^(\d{3})", 1).alias("_activity_code_3")
        ).with_columns(
            pl.col("_activity_code_3").str.slice(0, 2).alias("division_code")
        )

        # Join with the division map
        final_agg = final_agg.join(division_map, on="division_code", how="left")

        # Drop temporary columns
        final_agg = final_agg.drop("_activity_code_3", "division_code")

        # --- Reorder Columns --- 
        all_cols = final_agg.columns
        if "Actividad principal" in all_cols and "Growth_2020_2024_pct" in all_cols and "Median_YoY_Growth_pct" in all_cols:
            # Identify column groups
            activity_col = ["Actividad principal"]
            employee_col = ["Estimated_Employees_2024"] if "Estimated_Employees_2024" in final_agg.columns else []
            employee_pct_col = ["Estimated_Employees_pct"] if "Estimated_Employees_pct" in final_agg.columns else []
            growth_cols = sorted([col for col in final_agg.columns if "Growth" in col], reverse=True) # Place Median first if name matches
            total_cols = sorted([col for col in final_agg.columns if col.startswith("Total_") and col[6:].isdigit()], reverse=True)
            strata_abs_cols = sorted([col for col in final_agg.columns if col.startswith("Estrato_") and col.endswith("_abs")], key=lambda x: ["Sin asalariados", "De 1 a 2", "De 3 a 5", "De 6 a 9", "De 10 a 19", "De 20 a 49", "De 50 a 99", "De 100 a 199", "De 200 a 249", "De 250 a 999", "De 1000 a 4999", "De 5000 o más asalariados"].index(x.split("_")[1]))
            strata_pct_cols = sorted([col for col in final_agg.columns if col.startswith("Estrato_") and col.endswith("_pct")], key=lambda x: ["Sin asalariados", "De 1 a 2", "De 3 a 5", "De 6 a 9", "De 10 a 19", "De 20 a 49", "De 50 a 99", "De 100 a 199", "De 200 a 249", "De 250 a 999", "De 1000 a 4999", "De 5000 o más asalariados"].index(x.split("_")[1]))
            condicion_abs_cols = sorted([col for col in final_agg.columns if col.startswith("Condicion_") and col.endswith("_abs")])
            condicion_pct_cols = sorted([col for col in final_agg.columns if col.startswith("Condicion_") and col.endswith("_pct")])
            # Explicitly order size columns from Micro to Large
            size_order = ['Micro (0-9)', 'Small (10-49)', 'Medium (50-249)', 'Large (250+)']
            size_abs_cols = [f"Size_{s}_abs" for s in size_order]
            size_pct_cols = [f"Size_{s}_pct" for s in size_order]
            division_col = ["Division"]

            # Combine in desired order
            ordered_cols = (
                division_col + 
                activity_col + 
                employee_col + 
                employee_pct_col + 
                growth_cols + 
                total_cols + 
                size_abs_cols + 
                size_pct_cols + 
                strata_abs_cols + 
                strata_pct_cols + 
                condicion_abs_cols + 
                condicion_pct_cols
            )

            # Check if all original columns are accounted for
            current_cols = set(final_agg.columns)
            expected_cols_set = set(ordered_cols)

            if len(ordered_cols) == len(current_cols) and expected_cols_set == current_cols:
                final_agg = final_agg.select(ordered_cols)
                print("Columns reordered into groups (Division, Activity, Employees, Growth, Totals, Simplified Size (Abs+Pct), Original Strata (Abs+Pct), Conditions (Abs+Pct)).")
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
        print("Sample of final data:")
        # Ensure print options display enough columns for verification if needed
        with pl.Config(tbl_width_chars=200, tbl_cols=len(final_agg.columns)):
             print(final_agg.head())

        # --- Step 7: Save Output ---
        final_agg.write_parquet(output_file)
        print(f"Aggregated data saved to: {output_file}")

try:
    main()
except pl.exceptions.ComputeError as e:
    print(f"A Polars computation error occurred: {e}")
    print("This might happen if pivoting results in unexpected column names or types.")
except FileNotFoundError:
    print(f"Error: Input file not found at {input_file}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
