#!/usr/bin/env python3
"""
Script to generate schema documentation for DuckDB databases
"""
import os
import sys
import duckdb
import json

def get_table_schema(db_path, table_name):
    """Get schema for a specific table"""
    try:
        conn = duckdb.connect(db_path, read_only=True)
        schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
        sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        conn.close()
        return schema, sample, count
    except Exception as e:
        print(f"Error getting schema for {table_name}: {e}")
        return None, None, 0

def get_all_tables(db_path):
    """Get list of all tables in the database"""
    try:
        conn = duckdb.connect(db_path, read_only=True)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        return tables
    except Exception as e:
        print(f"Error getting tables: {e}")
        return []

def generate_markdown(db_path, output_path, original_db_name=None):
    """Generate markdown documentation for database schema"""
    if original_db_name:
        db_name = original_db_name
    else:
        db_name = os.path.basename(db_path).replace('.duckdb', '')
    
    tables = get_all_tables(db_path)
    if not tables:
        print(f"No tables found in {db_path}")
        return
    
    with open(output_path, 'w') as f:
        f.write(f"# {db_name} Database Schema\n\n")
        f.write(f"This document describes the schema of the {db_name} database.\n\n")
        f.write("## Tables\n\n")
        
        # Table of contents
        for table in sorted(tables):
            f.write(f"- [{table}](#{table.lower()})\n")
        f.write("\n")
        
        # Detailed schema for each table
        for table in sorted(tables):
            schema, sample, count = get_table_schema(db_path, table)
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
                            formatted_val = str(val).replace('|', '\\|')
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

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <original_db_path> <temp_db_path>")
        sys.exit(1)
        
    original_db_path = sys.argv[1]
    temp_db_path = sys.argv[2]
    
    if not os.path.exists(temp_db_path):
        print(f"Temporary database {temp_db_path} not found")
        sys.exit(1)
        
    output_path = os.path.join(os.path.dirname(original_db_path), 
                               os.path.basename(original_db_path).replace('.duckdb', '_schema.md'))
    original_db_name = os.path.basename(original_db_path).replace('.duckdb', '')
    
    print(f"Generating schema documentation for {original_db_path} using {temp_db_path}")
    print(f"Output will be written to {output_path}")
    
    generate_markdown(temp_db_path, output_path, original_db_name)
    print("Done!")
