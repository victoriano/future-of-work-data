"""
Database utility functions for connecting to and querying DuckDB databases.
"""

import os
from pathlib import Path
import duckdb

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()

# Database paths
ESCO_DB_PATH = os.path.join(PROJECT_ROOT, "data", "duckdb", "esco_dataset.duckdb")
ONET_DB_PATH = os.path.join(PROJECT_ROOT, "data", "duckdb", "onet_dataset.duckdb")

def get_esco_connection():
    """
    Get a connection to the ESCO DuckDB database.
    
    Returns:
        duckdb.DuckDBPyConnection: Connection to the ESCO database
    """
    return duckdb.connect(ESCO_DB_PATH)

def get_onet_connection():
    """
    Get a connection to the O*NET DuckDB database.
    
    Returns:
        duckdb.DuckDBPyConnection: Connection to the O*NET database
    """
    return duckdb.connect(ONET_DB_PATH)

def execute_sql_file(connection, sql_file_path):
    """
    Execute a SQL file on a DuckDB connection.
    
    Args:
        connection (duckdb.DuckDBPyConnection): Database connection
        sql_file_path (str): Path to the SQL file
        
    Returns:
        pandas.DataFrame: Result of the SQL query
    """
    with open(sql_file_path, 'r') as f:
        sql = f.read()
    
    return connection.execute(sql).fetchdf()
