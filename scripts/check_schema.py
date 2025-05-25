import sqlite3
import pandas as pd
import os

# Check if database exists
db_path = 'data/caiso_queue.db'
print(f"Checking if database exists at {os.path.abspath(db_path)}: {os.path.exists(db_path)}")

# Connect to the database
conn = sqlite3.connect(db_path)
print("Connected to database")

# Get all tables
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
print(f"Tables in database ({len(tables)}):")
print(tables['name'].tolist())

# Check if withdrawn_projects table exists
if 'withdrawn_projects' in tables['name'].tolist():
    # Check withdrawn_projects schema
    print("\nSchema for withdrawn_projects:")
    schema = pd.read_sql("PRAGMA table_info(withdrawn_projects)", conn)
    print(f"Number of columns: {len(schema)}")
    print(schema[['name', 'type']])
else:
    print("\nWARNING: withdrawn_projects table does not exist!")
    
# Also check completed_projects for comparison
if 'completed_projects' in tables['name'].tolist():
    print("\nSchema for completed_projects (for comparison):")
    schema2 = pd.read_sql("PRAGMA table_info(completed_projects)", conn)
    print(f"Number of columns: {len(schema2)}")
    # Just print the first 10 columns to avoid overwhelming output
    print(schema2[['name', 'type']].head(10))

# Check if project_name column exists
if 'project_name' in schema['name'].tolist():
    print("\nproject_name column exists in withdrawn_projects")
else:
    print("\nproject_name column does NOT exist in withdrawn_projects")

# If it doesn't exist, check for the confidential column
if 'Unnamed: 0_level_0 Project Name - Confidential' in schema['name'].tolist():
    print("But 'Unnamed: 0_level_0 Project Name - Confidential' column exists instead")
    
# Show the first few rows of data to see what's actually there
print("\nFirst 3 rows of withdrawn_projects:")
try:
    data = pd.read_sql("SELECT * FROM withdrawn_projects LIMIT 3", conn)
    for col in data.columns:
        if 'project' in col.lower() or 'name' in col.lower():
            print(f"Potential project name column found: {col}")
    print(data.head())
except Exception as e:
    print(f"Error reading data: {str(e)}")
    
conn.close()
