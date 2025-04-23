# Future of Work Data

This repository contains data and analysis tools for exploring job market data from ESCO (European Skills, Competences, Qualifications and Occupations) and O*NET (Occupational Information Network).

## Data Sources

- **ESCO Dataset**: European Skills, Competences, Qualifications and Occupations taxonomy, version 1.2.0 ([website](https://esco.ec.europa.eu/en/use-esco/download))
- **O*NET Dataset**: Occupational Information Network database, version 29.2 (February 2025 Release) ([website](https://www.onetcenter.org/database.html#all-files))

## Project Structure

```
future-of-work-data/
│
├── data/
│   ├── raw/                  # Original CSV and Excel files
│   │   ├── esco/             # ESCO dataset files
│   │   │   └── 1.2.0/        # ESCO version 1.2.0 
│   │   └── onet/             # O*NET dataset files
│   │       └── 29.2/         # O*NET version 29.2
│   ├── duckdb/               # DuckDB databases
│   │   ├── esco_dataset_1.2.0.duckdb
│   │   └── onet_dataset_29.2.duckdb
│   └── derived/              # Derived datasets from SQL queries
│
├── src/                      # Source code
│   ├── etl/                  # ETL scripts for data processing
│   └── utils/                # Utility functions
│
└── sql/                      # SQL queries
    ├── esco/                 # ESCO-specific queries
    ├── onet/                 # O*NET-specific queries
    ├── crosswalk/            # Queries linking ESCO and O*NET
    └── views/                # Python scripts with SQL queries
```

## Setup

1. Clone this repository
2. Create a virtual environment with uv:
   ```
   uv venv
   source .venv/bin/activate
   ```
3. Install dependencies:
   ```
   uv pip install -e .
   ```

## Data Processing

### Converting to DuckDB

The raw data is converted to DuckDB databases for efficient querying:

```python
# Convert ESCO dataset to DuckDB
python -m src.etl.convert_esco_to_duckdb

# Convert O*NET dataset to DuckDB
python -m src.etl.convert_onet_to_duckdb
```

## Usage Examples

### Query the data with SQL

```python
import duckdb

# Connect to the databases
esco_con = duckdb.connect('data/duckdb/esco_dataset_1.2.0.duckdb')
onet_con = duckdb.connect('data/duckdb/onet_dataset_29.2.duckdb')

# Example ESCO query
esco_occupations = esco_con.execute("SELECT * FROM occupations_en LIMIT 10").fetchdf()

# Example O*NET query 
onet_occupations = onet_con.execute("SELECT * FROM occupation_data LIMIT 10").fetchdf()
```

## License

This project uses data from:
- ESCO dataset, licensed under [their license terms](https://ec.europa.eu/esco/portal/document/en/87a9f66a-1830-4c93-94f0-5daa5e00507e)
- O*NET dataset, licensed under [Creative Commons Attribution 4.0 International License](https://www.onetcenter.org/license_db.html)
