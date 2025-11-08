import pandas as pd
import sqlite3
import os
from datetime import date
from column_mapping import get_column_mapping, map_dataframe_columns

RAW_FILE = 'raw/publicqueuereport.xlsx'
DB_FILE  = 'data/caiso_queue.db'

# Import column mapping from the central configuration
COLUMN_MAPPING = get_column_mapping()

# Dictionary of county coordinates (latitude, longitude)
COUNTY_COORDS = {
    # California counties
    'ALAMEDA': (37.6017, -121.7195),
    'ALPINE': (38.5898, -119.8208),
    'AMADOR': (38.4464, -120.6529),
    'BUTTE': (39.6679, -121.6008),
    'CALAVERAS': (38.2041, -120.5547),
    'COLUSA': (39.1776, -122.2375),
    'CONTRA COSTA': (37.9194, -121.9275),
    'DEL NORTE': (41.6029, -123.7458),
    'EL DORADO': (38.7786, -120.5246),
    'FRESNO': (36.7378, -119.7871),
    'GLENN': (39.5984, -122.3921),
    'HUMBOLDT': (40.7075, -123.8692),
    'IMPERIAL': (33.0395, -115.3597),
    'INYO': (36.5111, -117.4107),
    'KERN': (35.3425, -118.7299),
    'KINGS': (36.0753, -119.8156),
    'LAKE': (39.0997, -122.7532),
    'LASSEN': (40.6736, -120.5946),
    'LOS ANGELES': (34.3200, -118.2250),
    'MADERA': (37.2181, -119.7626),
    'MARIN': (37.9034, -122.5552),
    'MARIPOSA': (37.5708, -119.9052),
    'MENDOCINO': (39.4407, -123.3915),
    'MERCED': (37.1911, -120.7179),
    'MODOC': (41.5897, -120.7250),
    'MONO': (37.9389, -118.8870),
    'MONTEREY': (36.6002, -121.8947),
    'NAPA': (38.5025, -122.2654),
    'NEVADA': (39.3012, -120.7686),
    'ORANGE': (33.7175, -117.8311),
    'PLACER': (39.0639, -120.7179),
    'PLUMAS': (40.0037, -120.8397),
    'RIVERSIDE': (33.9534, -117.3962),
    'SACRAMENTO': (38.5816, -121.4944),
    'SAN BENITO': (36.6056, -121.0750),
    'SAN BERNARDINO': (34.8414, -116.1781),
    'SAN DIEGO': (32.7157, -117.1611),
    'SAN FRANCISCO': (37.7749, -122.4194),
    'SAN JOAQUIN': (37.9349, -121.2713),
    'SAN LUIS OBISPO': (35.3102, -120.4358),
    'SAN MATEO': (37.4969, -122.3330),
    'SANTA BARBARA': (34.4208, -119.6982),
    'SANTA CLARA': (37.3541, -121.9552),
    'SANTA CRUZ': (36.9741, -122.0308),
    'SHASTA': (40.5865, -122.3916),
    'SIERRA': (39.5807, -120.5166),
    'SISKIYOU': (41.5928, -122.5405),
    'SOLANO': (38.2884, -121.9497),
    'SONOMA': (38.5270, -122.9285),
    'STANISLAUS': (37.5591, -120.9979),
    'SUTTER': (39.0342, -121.6948),
    'TEHAMA': (40.1255, -122.2347),
    'TRINITY': (40.6507, -123.1126),
    'TULARE': (36.2077, -119.3473),
    'TUOLUMNE': (38.0276, -120.2326),
    'VENTURA': (34.3705, -119.1390),
    'YOLO': (38.6865, -121.9017),
    'YUBA': (39.2685, -121.3519),
    
    # Arizona counties (neighboring CAISO region)
    'YUMA': (32.6927, -114.6277),
    'MOHAVE': (35.7047, -113.7578),
    'LA PAZ': (33.7295, -113.9814),
    'MARICOPA': (33.5102, -112.0901),
    'PINAL': (32.9042, -111.3447),
    'GILA': (33.8003, -110.8127),
    'GRAHAM': (32.9319, -109.8875),
    'GREENLEE': (33.2151, -109.2402),
    'COCHISE': (31.8798, -109.7513),
    'SANTA CRUZ': (31.5282, -110.8361),
    'PIMA': (32.2217, -110.9265),
    
    # Nevada counties (neighboring CAISO region)
    'CLARK': (36.2146, -115.0137),
    'LINCOLN': (37.6433, -114.8769),
    'NYE': (38.0423, -116.4727),
    'ESMERALDA': (37.7847, -117.6322),
    'MINERAL': (38.5383, -118.4351),
    'DOUGLAS': (38.9121, -119.6163),
    'CARSON CITY': (39.1638, -119.7674),
    'WASHOE': (40.8386, -119.7528),
    'PERSHING': (40.4489, -118.4047),
    'HUMBOLDT': (41.4061, -118.1126),
    'ELKO': (41.1265, -115.3517),
    'EUREKA': (39.9841, -116.1786),
    'LANDER': (39.5708, -117.0705),
    'WHITE PINE': (39.4420, -114.8986),
    
    # Idaho counties (neighboring CAISO region)
    'LINCOLN': (42.9957, -114.1336),
    'TWIN FALLS': (42.3566, -114.6681),
    'GOODING': (42.9391, -114.7131),
    'JEROME': (42.6897, -114.2636),
    'MINIDOKA': (42.8549, -113.6375),
    'CASSIA': (42.2838, -113.6011),
    'ONEIDA': (42.1958, -112.5411),
    'POWER': (42.6938, -112.8408),
    'BANNOCK': (42.6685, -112.2158),
    'CARIBOU': (42.7705, -111.5622),
    
    # Wyoming counties (neighboring CAISO region)
    'PLATTE': (42.1330, -104.9658),
    'SWEETWATER': (41.6595, -108.8796),
    'CARBON': (41.6945, -106.9306),
    'ALBANY': (41.6545, -105.7236),
    'LARAMIE': (41.3072, -104.6902),
    'GOSHEN': (42.0879, -104.3533),
    'NIOBRARA': (43.0564, -104.4752),
    'WESTON': (43.8405, -104.5677),
    'CROOK': (44.5885, -104.5702),
    'CAMPBELL': (44.2483, -105.5483)
}

def get_county_coordinates(county, state):
    """
    Get latitude and longitude for a county using the local county coordinates dictionary.
    For multiple counties, returns the average coordinates.
    Returns (lat, lon) tuple or (None, None) if not found.
    """
    if pd.isna(county) or pd.isna(state):
        return None, None
        
    # Clean up county and state names
    county = str(county).strip().upper()
    state = str(state).strip().upper()
    
    # Handle special cases and misspellings
    county_fixes = {
        'SAN BERNADINO': 'SAN BERNARDINO',
        'SAN CLARA': 'SANTA CLARA',
        'MOJAVE': 'MOHAVE',
        'CHURCH': 'CHURCHILL',
        'TECATE BAJA CALIFORNIA MEXICO': 'TECATE',  # Try to match just the county part
        'ROSAMOND': 'KERN',  # Map to Kern County
        'HYDER': 'GOODING',  # Map to Gooding County
    }
    
    # Fix state if needed
    if state == 'NV' and county in ['SAN BENITO']:
        state = 'CA'
    
    # Handle multiple counties (split by /)
    counties = [c.strip() for c in county.split('/')]
    
    # For multiple counties, calculate average coordinates
    if len(counties) > 1:
        valid_coords = []
        for c in counties:
            # Clean up county name
            c = c.replace(' COUNTY', '').strip()
            # Apply fixes if needed
            c = county_fixes.get(c, c)
            if c is None:
                continue
            if c in COUNTY_COORDS:
                valid_coords.append(COUNTY_COORDS[c])
        
        if valid_coords:
            # Calculate average coordinates
            avg_lat = sum(c[0] for c in valid_coords) / len(valid_coords)
            avg_lon = sum(c[1] for c in valid_coords) / len(valid_coords)
            coords = (avg_lat, avg_lon)
            print(f"Found average coordinates for multiple counties {counties}: {coords}")
            return coords
        else:
            print(f"No valid coordinates found for counties: {counties}")
            return None, None
    
    # Single county case
    county = county.replace(' COUNTY', '').strip()
    # Apply fixes if needed
    county = county_fixes.get(county, county)
    if county is None:
        print(f"Skipping special case location: {county}, {state}")
        return None, None
        
    if county in COUNTY_COORDS:
        coords = COUNTY_COORDS[county]
        print(f"Found coordinates for {county}, {state}: {coords}")
        return coords
    else:
        print(f"No coordinates found for {county}, {state}")
        return None, None

# Ensure data directory exists
def ensure_dirs():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)

# Flatten multi‐level headers
def flatten_columns(df):
    """
    Flatten multi-level column headers into single-level.
    If columns are already single-level, return as-is.
    """
    if isinstance(df.columns, pd.MultiIndex):
        # Multi-level columns: join levels with spaces
        df.columns = [
            ' '.join(filter(None, map(str, col))).strip()
            for col in df.columns.values
        ]
    elif not all(isinstance(col, str) for col in df.columns):
        # Columns are tuples/lists but not MultiIndex
        df.columns = [
            ' '.join(filter(None, map(str, col))).strip() if isinstance(col, (tuple, list)) else str(col)
            for col in df.columns.values
        ]
    # else: columns are already simple strings, leave them as-is
    return df

# Parse a single sheet
def parse_sheet(df):
    # First flatten the multi-level headers
    df = flatten_columns(df)
    df['ingestion_date'] = pd.to_datetime('today').date()
    
    # Print column names for debugging
    print("Available columns:", df.columns.tolist())
    
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
    
    # Add latitude and longitude for each county
    print("Adding county coordinates...")
    df['latitude'] = None
    df['longitude'] = None
    
    # Find the correct column names for county and state
    county_col = next((col for col in df.columns if 'county' in col.lower()), None)
    state_col = next((col for col in df.columns if 'state' in col.lower()), None)
    
    if county_col and state_col:
        print(f"Found county column: {county_col}")
        print(f"Found state column: {state_col}")
        
        # Get unique county-state pairs to minimize lookups
        county_state_pairs = df[[county_col, state_col]].drop_duplicates()
        for _, row in county_state_pairs.iterrows():
            try:
                lat, lon = get_county_coordinates(row[county_col], row[state_col])
                if lat is not None and lon is not None:
                    try:
                        # Convert to float and validate
                        lat_float = float(lat)
                        lon_float = float(lon)
                        
                        # Validate coordinate ranges
                        if -90 <= lat_float <= 90 and -180 <= lon_float <= 180:
                            mask = (df[county_col] == row[county_col]) & (df[state_col] == row[state_col])
                            df.loc[mask, 'latitude'] = lat_float
                            df.loc[mask, 'longitude'] = lon_float
                            print(f"Added coordinates for {row[county_col]}, {row[state_col]}: ({lat_float}, {lon_float})")
                        else:
                            print(f"Invalid coordinate ranges for {row[county_col]}, {row[state_col]}: ({lat_float}, {lon_float})")
                    except (ValueError, TypeError) as e:
                        print(f"Error converting coordinates for {row[county_col]}, {row[state_col]}: {str(e)}")
                        print(f"Raw values: lat={lat}, lon={lon}")
            except Exception as e:
                print(f"Error processing coordinates for {row[county_col]}, {row[state_col]}: {str(e)}")
    else:
        print("Warning: Could not find county or state columns")
        if not county_col:
            print("County column not found")
        if not state_col:
            print("State column not found")
    
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
    
    # Final validation of coordinates
    print("\nValidating coordinates before returning:")
    print(f"Total rows: {len(df)}")
    print(f"Rows with valid coordinates: {df['latitude'].notna().sum()}")
    print(f"Rows with null coordinates: {df['latitude'].isna().sum()}")
    
    # Ensure coordinates are float type
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    
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
                # Check if table has latitude and longitude columns
                schema = pd.read_sql(f"PRAGMA table_info({table})", conn)
                col_names = schema['name'].tolist()
                
                if 'latitude' not in col_names or 'longitude' not in col_names:
                    print(f"Table {table} missing latitude/longitude columns, will drop and recreate")
                    conn.execute(f"DROP TABLE {table}")
                    conn.commit()
                    table_exists = False
                else:
                    # For withdrawn_projects, check if table schema has project_name
                    if table == 'withdrawn_projects':
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
