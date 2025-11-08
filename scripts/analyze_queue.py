"""
Analysis script for CAISO queue data
"""
import pandas as pd
import sqlite3
import os


def _safe_float(value, default=0.0):
    """Convert value to float, returning default on failure."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=0):
    """Convert value to int, returning default on failure."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

DB_FILE = 'data/caiso_queue.db'
REPORTS_DIR = 'reports'

# Ensure reports directory exists
os.makedirs(REPORTS_DIR, exist_ok=True)

# 1: Capacity by fuel types
def capacity_by_fuel(conn):
    """
    Analyze total capacity by different fuel types in the generation queue
    """
    df = pd.read_sql(
        """
        SELECT fuel_types AS fuel, SUM(mw_1) AS total_mw
        FROM grid_generation_queue
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM grid_generation_queue
        )
        GROUP BY fuel_types
        """, conn
    )
    os.makedirs(REPORTS_DIR, exist_ok=True)
    df.to_csv(os.path.join(REPORTS_DIR, 'capacity_by_fuel.csv'), index=False)
    print(f"Generated capacity by fuel report with {len(df)} fuel types")

# 2: Project count & capacity by status
def project_count_by_status(conn):
    """
    Provide overview of projects by status (Active, Completed, Withdrawn)
    """
    df = pd.read_sql(
        """
        SELECT
            'Active' as status,
            COUNT(*) AS project_count,
            SUM(mw_1) AS total_mw
        FROM grid_generation_queue
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM grid_generation_queue
        )

        UNION ALL

        SELECT
            'Completed' as status,
            COUNT(*) AS project_count,
            SUM(mw_1) AS total_mw
        FROM completed_projects
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM completed_projects
        )

        UNION ALL

        SELECT
            'Withdrawn' as status,
            COUNT(*) AS project_count,
            SUM(mw_1) AS total_mw
        FROM withdrawn_projects
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM withdrawn_projects
        )
        """, conn
    )
    os.makedirs(REPORTS_DIR, exist_ok=True)
    df.to_csv(os.path.join(REPORTS_DIR, 'project_count_by_status.csv'), index=False)
    print(f"Generated project count by status report with {len(df)} statuses")

# 3: Top 5 ISO Zones by active capacity
def top5_iso_zones(conn):
    """
    Identify top 5 ISO regions with the highest active generation capacity
    """
    df = pd.read_sql(
        """
        SELECT pto_study_region AS iso_zone, SUM(mw_1) AS total_mw
        FROM grid_generation_queue
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM grid_generation_queue
        )
        GROUP BY pto_study_region
        ORDER BY total_mw DESC
        LIMIT 5
        """, conn
    )
    os.makedirs(REPORTS_DIR, exist_ok=True)
    df.to_csv(os.path.join(REPORTS_DIR, 'top5_iso_zones.csv'), index=False)
    print(f"Generated top 5 ISO zones report with the highest capacity regions")

# 4: Cancellation rate
def cancellation_rate(conn):
    """
    Calculate ratio of withdrawn project capacity to total capacity
    """
    # Get active MW from active projects
    active_result = pd.read_sql(
        """
        SELECT SUM(mw_1) AS total_mw
        FROM grid_generation_queue
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM grid_generation_queue
        )
        """,
        conn
    ).iloc[0,0]
    active = _safe_float(active_result)
    
    # Get completed MW from completed projects
    completed_result = pd.read_sql(
        """
        SELECT SUM(mw_1) AS total_mw
        FROM completed_projects
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM completed_projects
        )
        """,
        conn
    ).iloc[0,0]
    completed = _safe_float(completed_result)

    # Get total MW from withdrawn projects
    withdrawn_result = pd.read_sql(
        """
        SELECT SUM(mw_1) AS withdrawn_mw
        FROM withdrawn_projects
        WHERE ingestion_date = (
            SELECT MAX(ingestion_date) FROM withdrawn_projects
        )
        """,
        conn
    ).iloc[0,0]
    withdrawn = _safe_float(withdrawn_result)
    
    total = active + completed + withdrawn
    rate = withdrawn / total if total else None
    os.makedirs(REPORTS_DIR, exist_ok=True)
    pd.DataFrame([{'cancellation_rate': rate}]).to_csv(os.path.join(REPORTS_DIR, 'cancellation_rate.csv'), index=False)
    print(f"Generated cancellation rate report: {rate:.2%}" if rate else "Generated cancellation rate report: N/A")

# 5: Average lead time (days)
def average_lead_time(conn):
    """
    Measure average days between interconnection request receipt and queue position assignment
    """
    df = pd.read_sql(
        "SELECT queue_date AS Queue_Date, "
        "request_receive_date AS Request_Received_Date "
        "FROM grid_generation_queue "
        "WHERE queue_date IS NOT NULL AND "
        "request_receive_date IS NOT NULL AND "
        "ingestion_date = (SELECT MAX(ingestion_date) FROM grid_generation_queue)", conn,
        parse_dates=['Queue_Date','Request_Received_Date']
    )
    df['lead_time'] = (df['Queue_Date'] - df['Request_Received_Date']).dt.days
    avg = df['lead_time'].mean()
    os.makedirs(REPORTS_DIR, exist_ok=True)
    pd.DataFrame([{'average_lead_time_days': avg}]).to_csv(os.path.join(REPORTS_DIR, 'average_lead_time.csv'), index=False)
    print(f"Generated average lead time report: {avg:.1f} days based on {len(df)} projects")

# 6: Top 10 projects by Net MWs to Grid
def top_projects_by_net_mw(conn):
    """
    Identify the largest generation projects based on net MW contribution
    """    
    df = pd.read_sql(
        """
        SELECT 
            project_name,
            queue_position,
            net_mw,
            fuel_types,
            'Active' AS status,
            county,
            state
        FROM grid_generation_queue
        WHERE net_mw IS NOT NULL
        AND ingestion_date = (
            SELECT MAX(ingestion_date) FROM grid_generation_queue
        )
        GROUP BY project_name, queue_position
        ORDER BY MAX(net_mw) DESC
        LIMIT 10
        """, conn
    )
    os.makedirs(REPORTS_DIR, exist_ok=True)
    df.to_csv(os.path.join(REPORTS_DIR, 'top_projects_by_net_mw.csv'), index=False)
    print(f"Generated top projects by net MW report with {len(df)} unique projects")

# 7: Project timeline delay analysis
def timeline_delay_analysis(conn):
    """
    Measure difference between originally proposed and current online dates
    """
    # Using the renamed columns directly now
    query = """
    SELECT
        project_name,
        queue_position,
        proposed_online_date,
        current_online_date,
        fuel_types
    FROM grid_generation_queue
    WHERE proposed_online_date IS NOT NULL
    AND current_online_date IS NOT NULL
    AND ingestion_date = (
        SELECT MAX(ingestion_date) FROM grid_generation_queue
    )
    """
    
    try:
        df = pd.read_sql(query, conn, parse_dates=['proposed_online_date', 'current_online_date'])
        print(f"Retrieved {len(df)} projects with timeline data")
    except Exception as e:
        print(f"Error executing timeline delay query: {str(e)}")
        return
            
    # Calculate the difference in days between proposed and current online dates
    if len(df) == 0:
        print("No data available for timeline delay analysis")
        return
        
    df['delay_days'] = (df['current_online_date'] - df['proposed_online_date']).dt.days
    
    # Generate summary statistics
    delay_stats = {
        'average_delay_days': df['delay_days'].mean(),
        'median_delay_days': df['delay_days'].median(),
        'min_delay_days': df['delay_days'].min(),
        'max_delay_days': df['delay_days'].max(),
        'positive_delay_count': (df['delay_days'] > 0).sum(),
        'negative_delay_count': (df['delay_days'] < 0).sum(),
        'no_change_count': (df['delay_days'] == 0).sum()
    }
    
    # Calculate percentage of projects with delays
    total_projects = len(df)
    delay_stats['delay_percentage'] = (delay_stats['positive_delay_count'] / total_projects) * 100 if total_projects > 0 else 0
    
    # Calculate average delay by fuel type
    fuel_delays = df.groupby('fuel_types')['delay_days'].mean().reset_index()
    fuel_delays.columns = ['fuel_type', 'average_delay_days']

    # Save detailed project-level data
    os.makedirs(REPORTS_DIR, exist_ok=True)
    df.to_csv(os.path.join(REPORTS_DIR, 'project_timeline_delays.csv'), index=False)

    # Save summary statistics
    pd.DataFrame([delay_stats]).to_csv(os.path.join(REPORTS_DIR, 'timeline_delay_summary.csv'), index=False)

    # Save fuel type analysis
    fuel_delays.to_csv(os.path.join(REPORTS_DIR, 'timeline_delay_by_fuel.csv'), index=False)
    
    print(f"Generated timeline delay analysis with data from {len(df)} projects:")
    print(f"  - Average delay: {delay_stats['average_delay_days']:.1f} days")
    print(f"  - Median delay: {delay_stats['median_delay_days']:.1f} days")
    print(f"  - Range: {delay_stats['min_delay_days']:.1f} to {delay_stats['max_delay_days']:.1f} days")
    print(f"  - Projects delayed: {delay_stats['positive_delay_count']} ({delay_stats['delay_percentage']:.1f}%)")
    print(f"  - Projects accelerated: {delay_stats['negative_delay_count']}")
    print(f"  - Projects on schedule: {delay_stats['no_change_count']}")

# Helper function to validate data quality
def validate_data_quality(conn):
    """
    Check for data quality issues before running analyses
    """
    validation_issues = 0
    
    print("\n=== Data Quality Validation ===")
    
    # Check for all main tables
    required_tables = ['grid_generation_queue', 'completed_projects', 'withdrawn_projects']
    existing_tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table'", 
        conn
    )['name'].tolist()
    
    for table in required_tables:
        if table not in existing_tables:
            print(f"ISSUE: Required table '{table}' is missing")
            validation_issues += 1
      # For each existing table, check for key columns
    for table in [t for t in required_tables if t in existing_tables]:
        try:
            # Base key columns that should be present in all tables
            key_columns = ['queue_position', 'ingestion_date', 'mw_1']
            
            # For withdrawn_projects, the project name column might have a different name
            if table == 'withdrawn_projects':
                project_name_cols = ['project_name', 'Unnamed: 0_level_0 Project Name - Confidential']
            else:
                project_name_cols = ['project_name']
                
            cols = pd.read_sql(f"PRAGMA table_info({table})", conn)['name'].tolist()
            
            # Check for standard columns
            for col in key_columns:
                if col not in cols:
                    print(f"ISSUE: Key column '{col}' missing from {table}")
                    validation_issues += 1
                    
            # Check for project name (with special handling for withdrawn_projects)
            if not any(col in cols for col in project_name_cols):
                print(f"ISSUE: No project name column found in {table}")
                validation_issues += 1
              # Check for null values in important columns
            row_count_result = pd.read_sql(
                f"SELECT COUNT(*) as count FROM {table}",
                conn
            ).iloc[0,0]
            row_count = _safe_int(row_count_result)
            if row_count > 0:
                # Check queue_position
                if 'queue_position' in cols:
                    null_count_result = pd.read_sql(
                        f"SELECT COUNT(*) as count FROM {table} WHERE queue_position IS NULL OR queue_position = ''",
                        conn
                    ).iloc[0,0]
                    null_count = _safe_int(null_count_result)
                    if null_count > 0:
                        pct = (null_count / row_count) * 100
                        print(f"ISSUE: {null_count} rows ({pct:.1f}%) have null values in queue_position in {table}")
                        validation_issues += 1
                
                # Check project_name (with special handling for withdrawn_projects)
                if table == 'withdrawn_projects':
                    if 'Unnamed: 0_level_0 Project Name - Confidential' in cols:
                        project_col = 'Unnamed: 0_level_0 Project Name - Confidential'
                    else:
                        project_col = 'project_name'
                else:
                    project_col = 'project_name'
                
                if project_col in cols:
                    null_count_result = pd.read_sql(
                        f"SELECT COUNT(*) as count FROM {table} WHERE {project_col} IS NULL OR {project_col} = ''",
                        conn
                    ).iloc[0,0]
                    null_count = _safe_int(null_count_result)
                    if null_count > 0:
                        pct = (null_count / row_count) * 100
                        print(f"ISSUE: {null_count} rows ({pct:.1f}%) have null values in project name column in {table}")
                        validation_issues += 1
            else:
                print(f"INFO: Table {table} is empty (0 rows)")
        
        except Exception as e:
            print(f"Error validating {table}: {str(e)}")
            validation_issues += 1
    
    if validation_issues > 0:
        print(f"\nFound {validation_issues} data quality issues that may affect analysis results")
    else:
        print("\nNo data quality issues found, proceeding with analysis")
    
    return validation_issues


# Main analysis function
def main():
    """Run all analysis functions and generate reports."""
    print("Starting analysis...")
    
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file not found at {DB_FILE}")
        print(f"Current working directory: {os.getcwd()}")
        return
        
    try:
        conn = sqlite3.connect(DB_FILE)
        print(f"Connected to database: {DB_FILE}")
        
        # Check if tables exist
        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table'", 
            conn
        )
        print(f"Found {len(tables)} tables in database:")
        for idx, table in enumerate(tables['name']):
            print(f"  {idx+1}. {table}")
            
        # Try to get column names from grid_generation_queue table
        try:
            cols = pd.read_sql("PRAGMA table_info(grid_generation_queue)", conn)
            print(f"\nFound {len(cols)} columns in grid_generation_queue table")
            print("First 5 columns:", cols['name'][:5].tolist())
        except Exception as e:
            print(f"Error getting columns: {str(e)}")
            
        # Validate data quality before running analyses
        validate_data_quality(conn)
              # Run all analyses with individual error handling for each
        analysis_functions = [
            ('capacity_by_fuel', capacity_by_fuel),
            ('project_count_by_status', project_count_by_status),
            ('top5_iso_zones', top5_iso_zones),
            ('cancellation_rate', cancellation_rate),
            ('average_lead_time', average_lead_time),
            ('top_projects_by_net_mw', top_projects_by_net_mw),
            ('timeline_delay_analysis', timeline_delay_analysis)
        ]
        
        success_count = 0
        for analysis_name, analysis_func in analysis_functions:
            try:
                print(f"\nRunning analysis: {analysis_name}")
                analysis_func(conn)
                success_count += 1
            except Exception as e:
                print(f"Error in {analysis_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                print(f"Continuing with next analysis...")
        
        print(f"\nAnalysis complete - {success_count} of {len(analysis_functions)} analyses completed successfully")
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()
            print("Connection closed")

if __name__ == '__main__':
    main()
