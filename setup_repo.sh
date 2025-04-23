#!/bin/bash
# Script to copy data files from original location to the new structured directories

# Create required directories
mkdir -p data/raw/esco
mkdir -p data/raw/onet
mkdir -p data/duckdb
mkdir -p data/derived

# Copy ESCO dataset to the new location
echo "Copying ESCO dataset..."
cp -R "../data_raw/ESCO dataset - v1.2.0 - classification - en - csv/"* data/raw/esco/

# Copy O*NET dataset to the new location
echo "Copying O*NET dataset..."
cp -R "../data_raw/db_29_2_excel/"* data/raw/onet/

# Copy DuckDB databases if they exist
echo "Copying DuckDB databases if they exist..."
[ -f "../esco_dataset.duckdb" ] && cp "../esco_dataset.duckdb" data/duckdb/ || echo "ESCO DuckDB database not found"
[ -f "../onet_dataset.duckdb" ] && cp "../onet_dataset.duckdb" data/duckdb/ || echo "O*NET DuckDB database not found"

echo "Setup complete. Now you can set up a virtual environment using uv:"
echo "  uv venv"
echo "  source .venv/bin/activate"
echo "  uv pip install -e ."
