"""
Database utility functions for connecting to and querying DuckDB databases.
"""

from io import StringIO
from pathlib import Path

import duckdb as ddb
from duckdb import DuckDBPyConnection
from pandas import DataFrame
from . import pathto

DBS = {
    "esco": pathto("data/duckdb/esco_dataset_1.2.0.duckdb"),
    "onet": pathto("data/duckdb/onet_dataset_29.2.duckdb"),
}


def connect_db(db: str | Path, **kwds) -> DuckDBPyConnection:
    if isinstance(db, str):
        try:
            return ddb.connect(DBS[db], **kwds)
        except KeyError:
            return ddb.connect(db, **kwds)

    return ddb.connect(db, **kwds)


def connect_esco() -> DuckDBPyConnection:
    return connect_db("esco")


def connect_onet() -> DuckDBPyConnection:
    return connect_db("onet")


def list_tables(db: str | Path | DuckDBPyConnection) -> list[str]:
    if not isinstance(db, DuckDBPyConnection):
        db = connect_db(db)

    return [table[0] for table in db.execute("SHOW TABLES").fetchall()]


def execute_sql_file(connection: DuckDBPyConnection, sql_file_path: Path) -> DataFrame:
    with open(sql_file_path, "r") as f:
        sql = f.read()

    return connection.execute(sql).fetchdf()


def metadata(
    conn: str | DuckDBPyConnection,
    table_name: str,
    to_pandas: bool = False,
) -> tuple:
    """Get schema for a specific table"""
    try:
        if isinstance(conn, str):
            conn = connect_db(conn, read_only=True)

        schema = conn.execute(f"DESCRIBE {table_name}")
        schema = schema.df() if to_pandas else schema.fetchall()

        sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample = sample.df() if to_pandas else sample.fetchall()

        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        return schema, sample, count
    except Exception as e:
        print(f"Error getting schema for {table_name}: {e}")
        return None, None, 0


def generate_markdown(conn: str | DuckDBPyConnection, original_db_name: str | None = None) -> str:
    """Generate markdown documentation for database schema"""
    if isinstance(conn, DuckDBPyConnection):
        if original_db_name is None:
            raise ValueError("original_db_name must be provided when passing a DuckDBPyConnection!")
    else:
        db_name = original_db_name or Path(conn).stem
        conn = connect_db(conn, read_only=True)

    tables = list_tables(conn)
    if not tables:
        raise ValueError(f"No tables found for DB '{db_name}'!")

    with StringIO() as f:
        f.write(f"# {db_name} Database Schema\n\n")
        f.write(f"This document describes the schema of the {db_name} database.\n\n")
        f.write("## Tables\n\n")

        # Table of contents
        for table in sorted(tables):
            f.write(f"- [{table}](#{table.lower()})\n")
        f.write("\n")

        # Detailed schema for each table
        for table in sorted(tables):
            schema, sample, count = metadata(conn, table)
            if not schema:
                continue

            f.write(f"## {table}\n\n")
            f.write(f"This table contains {count:,} records.\n\n")

            # Schema table
            f.write("### Schema\n\n")
            f.write("| Column | Type | Description |\n")
            f.write("|--------|------|-------------|\n")
            for col in schema:
                f.write(f"| {col[0]} | {col[1]} | |\n")
            f.write("\n")

            # Sample data
            f.write("### Sample Data\n\n")
            if sample:
                # Use first row to get column names
                col_names = [col[0] for col in schema]

                # Header
                f.write("| " + " | ".join(col_names) + " |\n")
                f.write("| " + " | ".join(["---" for _ in col_names]) + " |\n")

                # Data rows
                for row in sample:
                    formatted_row = []
                    for val in row:
                        if val is None:
                            formatted_row.append("")
                        elif isinstance(val, str):
                            # Escape pipes and format for markdown
                            formatted_val = str(val).replace("|", "\\|")
                            # Truncate if too long
                            if len(formatted_val) > 50:
                                formatted_val = formatted_val[:47] + "..."
                            formatted_row.append(formatted_val)
                        else:
                            formatted_row.append(str(val))
                    f.write("| " + " | ".join(formatted_row) + " |\n")
            else:
                f.write("No sample data available.\n")

            f.write("\n---\n\n")

        return f.getvalue()
