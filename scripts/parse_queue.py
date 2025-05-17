import pandas as pd
import sqlite3
import os
from datetime import date

RAW_FILE = 'raw/publicqueuereport.xlsx'
DB_FILE  = 'data/caiso_queue.db'

# Ensure data directory exists
def ensure_dirs():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)

# Flatten multi‐level headers
def flatten_columns(df):
    df.columns = [
        ' '.join(filter(None, map(str, col))).strip()
        for col in df.columns.values
    ]
    return df

# Parse a single sheet
def parse_sheet(df):
    # First flatten the multi-level headers
    df = flatten_columns(df)
    df['ingestion_date'] = pd.to_datetime('today').date()
    
    # Derive combined fuel_types - check for correct column names
    possible_fuel_cols = [col for col in df.columns if 'fuel' in col.lower()]
    if possible_fuel_cols:
        print(f"Found fuel columns: {possible_fuel_cols}")
        df['fuel_types'] = (
            df[possible_fuel_cols]
            .fillna('')
            .apply(lambda row: '/'.join([f for f in row if f]), axis=1)
        )
    else:
        print("No fuel columns found, skipping fuel_types derivation")
        df['fuel_types'] = ''
    
    return df

# Main ingestion
def main():
    # Ensure data directory exists
    ensure_dirs()
    
    # Check if raw file exists
    if not os.path.exists(RAW_FILE):
        print(f"Error: Input file not found at {RAW_FILE}")
        print(f"Current working directory: {os.getcwd()}")
        return
    
    print(f"Processing file: {RAW_FILE}")
    
    # Load workbook
    sheets = {
        'grid_generation_queue': 'Grid GenerationQueue',
        'completed_projects'    : 'Completed Generation Projects',
        'withdrawn_projects'    : 'Withdrawn Generation Projects'
    }
    conn = sqlite3.connect(DB_FILE)
    
    # Process each sheet
    for table, sheet_name in sheets.items():
        try:
            print(f"Reading sheet: {sheet_name}")
            df = pd.read_excel(
                RAW_FILE,
                header=[2,3],
                sheet_name=sheet_name,
                engine='openpyxl'
            )
            print(f"Sheet {sheet_name} loaded with {len(df)} rows")
            df = parse_sheet(df)
            print(f"Parsed data for {table}, writing to database")
            df.to_sql(table, conn, if_exists='append', index=False)
            print(f"Successfully added {len(df)} rows to {table}")
        except Exception as e:
            print(f"Error processing sheet {sheet_name}: {str(e)}")
    
    # Create indexes only if at least one table was successfully created
    try:
        # Check if grid_generation_queue table exists
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='grid_generation_queue'"
        ).fetchone() is not None
        
        if table_exists:
            print("Creating indexes on grid_generation_queue table")
            with conn:
                try:
                    # Create index on Queue Position
                    conn.execute(
                        'CREATE INDEX IF NOT EXISTS idx_queue_position '
                        'ON grid_generation_queue(`Unnamed: 1_level_0 Queue Position`)'
                    )
                    print("Created index on Queue Position")
                except Exception as e:
                    print(f"Error creating index on Queue Position: {str(e)}")
        else:
            print("Skipping index creation - grid_generation_queue table does not exist")
    except Exception as e:
        print(f"Error during index creation: {str(e)}")
    finally:
        conn.close()

if __name__ == '__main__':
    main()