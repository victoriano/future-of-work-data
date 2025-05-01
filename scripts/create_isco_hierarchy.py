import pandas as pd
import os

def create_isco_hierarchy(base_path):
    """Reads the raw ISCO groups CSV, calculates hierarchy levels and parent codes,
    and saves the structured hierarchy to a Parquet file.

    Args:
        base_path (str): The root directory of the project.
    """
    raw_csv_path = os.path.join(base_path, 'data/raw/esco/1.2.0/ISCOGroups_en.csv')
    derived_parquet_path = os.path.join(base_path, 'data/derived/isco_hierarchy.parquet')
    derived_dir = os.path.dirname(derived_parquet_path)

    # Create derived directory if it doesn't exist
    os.makedirs(derived_dir, exist_ok=True)

    try:
        print(f"Reading raw ISCO data from: {raw_csv_path}")
        df = pd.read_csv(raw_csv_path)
    except FileNotFoundError:
        print(f"Error: Raw ISCO file not found at {raw_csv_path}")
        return
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    print("Processing ISCO data...")
    # Select and rename columns
    df_hierarchy = df[['conceptUri', 'code', 'preferredLabel', 'description']].copy()
    df_hierarchy.rename(columns={
        'conceptUri': 'url',
        'preferredLabel': 'label'
    }, inplace=True)

    # Ensure code is string
    df_hierarchy['code'] = df_hierarchy['code'].astype(str)

    # Calculate level
    df_hierarchy['level'] = df_hierarchy['code'].apply(len)

    # Calculate parent_code
    df_hierarchy['parent_code'] = df_hierarchy['code'].apply(
        lambda x: x[:-1] if len(x) > 1 else None
    )

    # Reorder columns for clarity
    df_hierarchy = df_hierarchy[['code', 'label', 'description', 'url', 'parent_code', 'level']]

    # Remove duplicate rows based on the 'code' column
    initial_rows = len(df_hierarchy)
    df_hierarchy = df_hierarchy.drop_duplicates(subset=['code'], keep='first')
    final_rows = len(df_hierarchy)
    if final_rows < initial_rows:
        print(f"Removed {initial_rows - final_rows} duplicate rows based on 'code'.")
    else:
        print("No duplicate 'code' values found.")

    try:
        print(f"Saving hierarchy data to: {derived_parquet_path}")
        df_hierarchy.to_parquet(derived_parquet_path, index=False, engine='pyarrow')
        print("Successfully created ISCO hierarchy file.")
    except Exception as e:
        print(f"Error saving Parquet file: {e}")

if __name__ == "__main__":
    # Assuming the script is run from the 'scripts' directory or the project root
    # Adjust the path logic if necessary
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir) # Go up one level from 'scripts'
    if os.path.basename(project_root) == 'Code': # Adjust if project root is different
         project_root = os.path.dirname(project_root) # Go up another level if inside a 'Code' folder

    # Handle potential different running locations
    if not os.path.exists(os.path.join(project_root, 'data')):
        # Maybe running from the root directly?
        project_root = os.getcwd()
        if not os.path.exists(os.path.join(project_root, 'data')):
             print("Error: Could not determine project root containing the 'data' directory.")
             exit(1)

    create_isco_hierarchy(project_root)
