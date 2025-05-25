import pandas as pd
import sqlite3
import os
from datetime import date
from column_mapping import get_column_mapping, map_dataframe_columns

RAW_FILE = 'raw/publicqueuereport.xlsx'
DB_FILE  = 'data/caiso_queue.db'

# Import column mapping from the central configuration
COLUMN_MAPPING = get_column_mapping()

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
    
    # Apply column mapping to simplify column names
    print("Applying column mapping to simplify column names")
    original_cols = set(df.columns)
    df = map_dataframe_columns(df)
    new_cols = set(df.columns)
    
    # Print mapping summary
    mapped_cols = len(original_cols) - len(new_cols.intersection(original_cols))
    if mapped_cols > 0:
        print(f"Renamed {mapped_cols} columns to simpler names")
    else:
        print("No columns were renamed")
    
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
    
    # Get today's date for duplicate checking
    today = pd.to_datetime('today').date()
    
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
            
            # Filter out records with empty Queue Position
            queue_pos_col_orig = 'Unnamed: 1_level_0 Queue Position'
            queue_pos_col_mapped = 'queue_position'
            
            # Determine which column name to use (original or mapped)
            queue_pos_col = queue_pos_col_mapped if queue_pos_col_mapped in df.columns else queue_pos_col_orig
            
            if queue_pos_col in df.columns:
                before_count = len(df)
                # Drop rows where Queue Position is null, empty string, or whitespace only
                df = df.dropna(subset=[queue_pos_col])
                # Also drop rows where Queue Position is just whitespace
                df = df[df[queue_pos_col].astype(str).str.strip() != '']
                dropped_count = before_count - len(df)
                if dropped_count > 0:
                    print(f"Dropped {dropped_count} rows with empty Queue Position values")
                print(f"Remaining rows after filtering: {len(df)}")
            
            # Special handling for withdrawn_projects - check if we have project_name column
            if sheet_name == "Withdrawn Generation Projects":
                # Check if the confidential project name was correctly mapped
                if 'project_name' not in df.columns and 'Unnamed: 0_level_0 Project Name - Confidential' in df.columns:
                    print("Explicitly renaming 'Unnamed: 0_level_0 Project Name - Confidential' to 'project_name'")
                    df = df.rename(columns={'Unnamed: 0_level_0 Project Name - Confidential': 'project_name'})
            
            # Check if table exists and if it needs to be recreated for schema changes
            table_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,)
            ).fetchone() is not None
            
            if table_exists:
                # For withdrawn_projects, check if table schema has project_name
                if table == 'withdrawn_projects':
                    schema = pd.read_sql(f"PRAGMA table_info({table})", conn)
                    col_names = schema['name'].tolist()
                    
                    if 'project_name' not in col_names and 'Unnamed: 0_level_0 Project Name - Confidential' in col_names:
                        print(f"Table {table} has wrong column name, will drop and recreate")
                        conn.execute(f"DROP TABLE {table}")
                        conn.commit()
                        table_exists = False
                
                # Check for existing data with today's ingestion_date
                if table_exists:
                    try:
                        existing_count = conn.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE ingestion_date = ?", 
                            (today.strftime('%Y-%m-%d'),)
                        ).fetchone()[0]
                        
                        if existing_count > 0:
                            print(f"Found {existing_count} existing records for today in {table}, removing them first")
                            conn.execute(f"DELETE FROM {table} WHERE ingestion_date = ?", (today.strftime('%Y-%m-%d'),))
                            conn.commit()
                        
                        # Check for and remove duplicate queue positions (keeping only latest entries)
                        if queue_pos_col_mapped in df.columns:
                            # Get list of queue positions in current dataframe
                            queue_positions = df[queue_pos_col_mapped].dropna().unique().tolist()
                            if queue_positions:
                                # For each queue position in current data, remove existing older records with same queue position
                                for batch in [queue_positions[i:i + 500] for i in range(0, len(queue_positions), 500)]:
                                    # Use parameterized query with placeholders for each value in the batch
                                    placeholders = ','.join(['?' for _ in batch])
                                    dup_count = conn.execute(
                                        f"SELECT COUNT(*) FROM {table} WHERE {queue_pos_col_mapped} IN ({placeholders}) AND ingestion_date < ?", 
                                        (*batch, today.strftime('%Y-%m-%d'))
                                    ).fetchone()[0]
                                    
                                    if dup_count > 0:
                                        print(f"Found {dup_count} older records with duplicate queue positions in {table}, removing them")
                                        conn.execute(
                                            f"DELETE FROM {table} WHERE {queue_pos_col_mapped} IN ({placeholders}) AND ingestion_date < ?", 
                                            (*batch, today.strftime('%Y-%m-%d'))
                                        )
                                        conn.commit()
                                        print(f"Removed older duplicate entries to maintain unique queue positions")
                                        
                                        # Verify removal was successful
                                        verify_count = conn.execute(
                                            f"SELECT COUNT(*) FROM {table} WHERE {queue_pos_col_mapped} IN ({placeholders}) AND ingestion_date < ?", 
                                            (*batch, today.strftime('%Y-%m-%d'))
                                        ).fetchone()[0]
                                        
                                        if verify_count > 0:
                                            print(f"WARNING: Still found {verify_count} older records with duplicate queue positions after deletion attempt")
                    except Exception as e:
                        # Table might exist but not have required columns
                        print(f"Could not check for duplicates in {table}: {str(e)}")
            else:
                print(f"Table {table} does not exist yet, will be created")
            
            print(f"Parsed data for {table}, writing to database")
            df.to_sql(table, conn, if_exists='append', index=False)
            print(f"Successfully added {len(df)} rows to {table}")
        except Exception as e:
            print(f"Error processing sheet {sheet_name}: {str(e)}")
    
    # Create indexes for all tables
    try:
        # Create indexes for each table (if it exists)
        for table in sheets.keys():
            table_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,)
            ).fetchone() is not None
            
            if table_exists:
                print(f"Creating indexes on {table} table")
                with conn:
                    try:
                        # Create index on Queue Position
                        conn.execute(
                            f'CREATE INDEX IF NOT EXISTS idx_{table}_queue_position '
                            f'ON {table}(queue_position)'
                        )
                        print(f"Created index on queue_position for {table}")
                        
                        # Create index on ingestion_date for faster duplicate checking
                        conn.execute(
                            f'CREATE INDEX IF NOT EXISTS idx_{table}_ingestion_date '
                            f'ON {table}(ingestion_date)'
                        )
                        print(f"Created index on ingestion_date for {table}")
                        
                        # Final verification for duplicates
                        try:
                            duplicate_query = f"""
                                SELECT queue_position, COUNT(*) as count
                                FROM {table} 
                                WHERE queue_position IS NOT NULL
                                GROUP BY queue_position
                                HAVING COUNT(*) > 1
                                ORDER BY count DESC
                                LIMIT 10
                            """
                            duplicates = conn.execute(duplicate_query).fetchall()
                            
                            if duplicates:
                                print(f"\nWARNING: Found {len(duplicates)} queue positions with duplicates in {table}:")
                                for pos, count in duplicates:
                                    print(f"  Queue Position {pos}: {count} records")
                                print("Please check why duplicate removal was not successful")
                            else:
                                print(f"Verification successful: No duplicate queue positions found in {table}")
                        except Exception as e:
                            print(f"Error during final duplicate verification for {table}: {str(e)}")
                    except Exception as e:
                        print(f"Error creating indexes for {table}: {str(e)}")
            else:
                print(f"Skipping index creation - {table} table does not exist")
    except Exception as e:
        print(f"Error during index creation: {str(e)}")
    finally:
        conn.close()

if __name__ == '__main__':
    main() 
