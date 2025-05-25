"""
Column mapping configuration for CAISO queue data.
This file defines how complex Excel column headers are mapped to simpler database column names.
"""

# Standard mapping for all tables
COLUMN_MAPPING = {
    # Project identification
    'Unnamed: 0_level_0 Project Name': 'project_name',
    'Unnamed: 0_level_0 Project Name - Confidential':'project_name', # for withdrawn projects
    'Unnamed: 1_level_0 Queue Position': 'queue_position',
    'Unnamed: 2_level_0 Interconnection Request\nReceive Date': 'request_receive_date',
    'Unnamed: 3_level_0 Queue Date': 'queue_date',
    'Unnamed: 4_level_0 Application Status': 'application_status',
    'Unnamed: 5_level_0 Study\nProcess': 'study_process',
    
    # Generating Facility details
    'Generating Facility Type-1': 'facility_type_1',
    'Generating Facility Type-2': 'facility_type_2',
    'Generating Facility Type-3': 'facility_type_3',
    'Generating Facility Fuel-1': 'fuel_type_1',
    'Generating Facility Fuel-2': 'fuel_type_2',
    'Generating Facility Fuel-3': 'fuel_type_3',
    
    # MW capacity 
    'MWs MW-1': 'mw_1',
    'MWs MW-2': 'mw_2',
    'MWs MW-3': 'mw_3',
    'MWs Net MWs to Grid': 'net_mw',
    
    # Deliverability Status
    'Deliverability\nStatus Full Capacity, Partial or Energy Only (FC/P/EO)': 'capacity_status',
    'Deliverability\nStatus TPD Allocation Percentage': 'tpd_allocation_pct',
    'Deliverability\nStatus Off-Peak Deliverability and Economic Only': 'off_peak_deliverability',
    'Deliverability\nStatus TPD Allocation Group': 'tpd_allocation_group',
    
    # Location details
    'Location County': 'county',
    'Location State': 'state',
    
    # Interconnection details
    'Point of Interconnection Utility': 'utility',
    'Point of Interconnection PTO Study Region': 'pto_study_region',
    'Point of Interconnection Station or Transmission Line': 'interconnection_point',
    'Point of Interconnection Proposed\nOn-line Date\n(as filed with IR)': 'proposed_online_date',
    'Point of Interconnection Current\nOn-line Date': 'current_online_date',
    'Point of Interconnection Suspension Status': 'suspension_status',
    
    # Study availability
    'Study Availability Feasibility Study or Supplemental Review': 'feasibility_study',
    'Study Availability System Impact Study or \nPhase I Cluster Study': 'system_impact_study_or_ph1',
    'Study Availability Facilities Study (FAS) or \nPhase II Cluster Study': 'facility_study_or_ph2',
    'Study Availability Optional Study\n(OS)': 'optional_study',
    'Study Availability Interconnection Agreement \nStatus': 'interconnection_study',
    
    # Other columns that might appear
    'ingestion_date': 'ingestion_date',
    'fuel_types': 'fuel_types',
}

def get_column_mapping():
    """
    Returns the standard column mapping dictionary.
    """
    return COLUMN_MAPPING

def map_dataframe_columns(df):
    """
    Maps DataFrame columns using the standard mapping.
    Handles cases where not all columns are present in the DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame with original column names
        
    Returns:
        pandas.DataFrame: DataFrame with mapped column names
    """
    # Print column names for debugging
    print(f"Original column names before mapping ({len(df.columns)} columns):")
    project_cols = [col for col in df.columns if 'project' in col.lower() or 'name' in col.lower()]
    if project_cols:
        print(f"Project/name related columns found: {project_cols}")
    
    # Create a mapping dict with only columns that exist in the dataframe
    mapping = {col: COLUMN_MAPPING.get(col, col) for col in df.columns}
    
    # Force mapping for confidential project name column (for withdrawn projects)
    confidential_col = 'Unnamed: 0_level_0 Project Name - Confidential'
    if confidential_col in df.columns:
        print(f"Found {confidential_col} - will map to project_name")
        mapping[confidential_col] = 'project_name'
    
    # Rename columns
    df_renamed = df.rename(columns=mapping)
    
    # Verify the mapping was successful
    if confidential_col in df.columns:
        if 'project_name' in df_renamed.columns:
            print("✓ Successfully mapped to project_name column")
        else:
            print(f"⚠️ WARNING: Failed to map {confidential_col} to project_name!")
            print(f"Renamed columns: {list(df_renamed.columns)[:5]}...")
    
    return df_renamed
