import pandas as pd
import json
import csv
import io

TEMP_CSV_PATH = 'data/processed/esco/temp_esco_occupation_profiles.csv'
FINAL_CSV_PATH = 'data/processed/esco/esco_occupation_profiles.csv'
COLUMN_TO_PROCESS = 'raw_alternative_names'
NEW_COLUMN_NAME = 'alternative_names'

def process_alt_names(raw_names):
    """Processes the raw multi-line string into a JSON array string."""
    if pd.isna(raw_names) or not isinstance(raw_names, str) or not raw_names.strip():
        return '[]' # Return empty JSON array for null/empty input

    # Split by newline, handling both \n and \r\n, \r
    lines = []
    for line in raw_names.split('\n'):
        lines.extend(line.split('\r'))

    # Clean up each line and filter empty ones
    cleaned_labels = [label.strip() for label in lines if label.strip()]

    # Convert list to JSON array string
    return json.dumps(cleaned_labels)

# Read the temporary CSV
try:
    # Use io.StringIO to handle potential CParserError with newline chars in fields
    with open(TEMP_CSV_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    df = pd.read_csv(io.StringIO(content), sep=',', quotechar='"', escapechar='\\')
    # If the above fails with CParserError, try python engine:
    # df = pd.read_csv(TEMP_CSV_PATH, sep=',', quotechar='"', escapechar='\\', engine='python')
except Exception as e:
    print(f"Error reading temporary CSV: {e}")
    print("Attempting with engine='python'")
    try:
      df = pd.read_csv(TEMP_CSV_PATH, sep=',', quotechar='"', escapechar='\\', engine='python')
    except Exception as e2:
      print(f"Error reading temporary CSV with python engine: {e2}")
      exit(1)

# Process the column
if COLUMN_TO_PROCESS in df.columns:
    df[NEW_COLUMN_NAME] = df[COLUMN_TO_PROCESS].apply(process_alt_names)
    # Drop the original raw column
    df = df.drop(columns=[COLUMN_TO_PROCESS])
else:
    print(f"Error: Column '{COLUMN_TO_PROCESS}' not found in temporary CSV.")
    exit(1)

# Save the final CSV
try:
    # Use csv.QUOTE_ALL to ensure proper quoting, especially for JSON strings
    df.to_csv(FINAL_CSV_PATH, index=False, quoting=csv.QUOTE_ALL, sep=',')
    print(f"Successfully processed and saved final CSV to {FINAL_CSV_PATH}")
except Exception as e:
    print(f"Error writing final CSV: {e}")
    exit(1)
