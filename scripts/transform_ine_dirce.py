import polars as pl
import os

# Define paths relative to the project root directory
# Assumes the script is run from the project root
input_file = "data/raw/ine-dirce/39371.csv"
output_dir = "data/derived" # Changed output directory
output_file = os.path.join(output_dir, "ine_dirce_empresas_filtered.parquet")

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

print(f"Reading file: {input_file}")

# Read the CSV file
try:
    df = pl.read_csv(
        input_file,
        separator=";",
        infer_schema_length=10000 # Increase sample size for better type inference
        # Consider specifying encoding if default UTF-8 fails, e.g., encoding='latin-1'
    )
    print("File read successfully. Columns:", df.columns)
    print("Original shape:", df.shape)

    # Define filter conditions
    # Keep rows where NONE of the specified columns contain "Total"
    # Note: Case sensitivity might matter, adjust .str.contains if needed
    filter_condition = (
        (~pl.col("Condición jurídica").str.contains("Total", literal=True)) &
        (~pl.col("Actividad principal").str.contains("Total", literal=True)) &
        (~pl.col("Estrato de asalariados").str.contains("Total", literal=True))
    )

    # Apply the filter
    df_filtered = df.filter(filter_condition)

    print("Filtered shape:", df_filtered.shape)

    # --- Add new columns ---
    # Extract the numeric code from the beginning of 'Actividad principal'
    df_with_code = df_filtered.with_columns(
        pl.col("Actividad principal")
        .str.extract(r"^(\d+)\s", 1) # Extract first group (digits)
        .alias("activity_code")
    )

    # Calculate the length of the extracted code
    df_final = df_with_code.with_columns(
        pl.col("activity_code")
        .str.len_chars() # Get length of the code string
        .alias("code_length")
    )
    # --- End of adding new columns ---

    print("Shape after adding columns:", df_final.shape)
    print("Sample of new columns:\n", df_final.select(["Actividad principal", "activity_code", "code_length"]).head())

    # --- Clean and cast the 'Total' column ---
    df_final = df_final.with_columns(
        pl.col("Total")
        .str.replace_all(".", "", literal=True) # Remove dots
        .cast(pl.Int64, strict=False) # Cast to integer (strict=False handles potential errors gracefully, though unlikely here)
        .alias("Total") # Keep the original column name
    )
    # --- End of cleaning 'Total' column ---

    print("Shape after cleaning 'Total':", df_final.shape)
    print("Data types after cleaning 'Total':\n", df_final.dtypes)
    print("Sample after cleaning 'Total':\n", df_final.head())

    # Write the final data (including cleaned 'Total') to a Parquet file
    df_final.write_parquet(output_file)

    print(f"Filtered data saved to: {output_file}")

except Exception as e:
    print(f"An error occurred: {e}")
