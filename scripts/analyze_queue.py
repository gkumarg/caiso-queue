import pandas as pd
import sqlite3
import os

DB_FILE = 'data/caiso_queue.db'
REPORTS_DIR = 'reports'

# Ensure reports directory exists
os.makedirs(REPORTS_DIR, exist_ok=True)

# 1: Capacity by fuel types
def capacity_by_fuel(conn):
    """
    This KPI analyzes the total capacity (in MW) by different fuel types 
    in the generation queue. It helps understand the distribution of renewable
    and conventional energy sources, which is valuable for:
    - Tracking renewable energy growth trends
    - Identifying dominant generation technologies
    - Supporting policy analysis and investment planning
    - Monitoring progress toward clean energy goals
    """
    df = pd.read_sql(
        """
        SELECT fuel_types AS fuel, SUM(`MWs MW-1`) AS total_mw
        FROM grid_generation_queue
        GROUP BY fuel_types
        """, conn
    )
    df.to_csv('reports/capacity_by_fuel.csv', index=False)
    print(f"Generated capacity by fuel report with {len(df)} fuel types")

# 2: Project count & capacity by status
def project_count_by_status(conn):
    """
    This KPI provides an overview of projects by their status (Active, Completed, 
    or Withdrawn) along with their total count and MW capacity. It helps track the 
    progression of projects through the interconnection queue, which is useful for:
    - Evaluating the overall health of the queue
    - Assessing project completion and withdrawal rates
    - Forecasting future grid capacity additions
    - Planning for transmission needs based on project status
    """
    df = pd.read_sql(
        """
        SELECT 
            'Active' as status,
            COUNT(*) AS project_count,
            SUM(`MWs MW-1`) AS total_mw
        FROM grid_generation_queue
        
        UNION ALL
        
        SELECT 
            'Completed' as status,
            COUNT(*) AS project_count,
            SUM(`MWs MW-1`) AS total_mw
        FROM completed_projects
        
        UNION ALL
        
        SELECT 
            'Withdrawn' as status,
            COUNT(*) AS project_count,
            SUM(`MWs MW-1`) AS total_mw
        FROM withdrawn_projects
        """, conn
    )
    df.to_csv('reports/project_count_by_status.csv', index=False)
    print(f"Generated project count by status report with {len(df)} statuses")

# 3: Top 5 ISO Zones by active capacity
def top5_iso_zones(conn):
    """
    This KPI identifies the top 5 ISO regions with the highest active generation 
    capacity in the queue. This geographical distribution analysis is crucial for:
    - Identifying regions with high development activity
    - Anticipating transmission congestion areas
    - Supporting locational investment decisions
    - Highlighting areas with potential reliability concerns
    - Informing regional economic impact assessments
    """
    df = pd.read_sql(
        """
        SELECT `Point of Interconnection PTO Study Region` AS iso_zone, SUM(`MWs MW-1`) AS total_mw
        FROM grid_generation_queue  -- This table only contains active projects
        GROUP BY `Point of Interconnection PTO Study Region`
        ORDER BY total_mw DESC
        LIMIT 5
        """, conn
    )
    df.to_csv('reports/top5_iso_zones.csv', index=False)
    print(f"Generated top 5 ISO zones report with the highest capacity regions")

# 4: TODO Annual project status by fuel type
def annual_project_status(conn):
    """
    This KPI tracks the number of projects added, withdrawn, and completed by year,
    with additional breakdown by primary fuel type. This annual analysis provides:
    - Year-over-year trends in project development activity
    - Technology adoption patterns over time
    - Comparative analysis of project success rates by fuel type
    - Long-term market evolution insights for generation technologies
    - Historical context for current interconnection patterns
    """
    pass

# 5: Cancellation rate
def cancellation_rate(conn):
    """
    This KPI calculates the ratio of withdrawn project capacity to total capacity
    in the interconnection queue. This metric is a key indicator of:
    - Project viability and development risk
    - Market barriers and friction points in the interconnection process
    - Efficiency of the queue management system
    - Potential concerns with the interconnection process that may need reform
    - Resource adequacy planning reliability (accounting for project attrition)
    """
    # Get active MW from active projects
    active = pd.read_sql(
        "SELECT SUM(`MWs MW-1`) AS total_mw FROM grid_generation_queue", 
        conn
    ).iloc[0,0] or 0
    
    # Get completed MW from completed projects
    completed = pd.read_sql(
        "SELECT SUM(`MWs MW-1`) AS total_mw FROM completed_projects", 
        conn
    ).iloc[0,0] or 0

    # Get total MW from withdrawn projects
    withdrawn = pd.read_sql(
        "SELECT SUM(`MWs MW-1`) AS withdrawn_mw FROM withdrawn_projects", 
        conn
    ).iloc[0,0] or 0
    
    total = active + completed + withdrawn
    rate = withdrawn / total if total else None
    pd.DataFrame([{'cancellation_rate': rate}]).to_csv('reports/cancellation_rate.csv', index=False)
    print(f"Generated cancellation rate report: {rate:.2%}" if rate else "Generated cancellation rate report: N/A")

# 6: Average lead time (days)
def average_lead_time(conn):
    """
    This KPI measures the average number of days between when an interconnection 
    request is received and when it is assigned a queue position. This lead time 
    analysis provides insights into:
    - Processing efficiency of the ISO's interconnection procedures
    - Administrative bottlenecks in the application review process
    - Trends in application processing times 
    - Predictability of queue position assignment for developers
    - Overall health of the interconnection request system
    """
    df = pd.read_sql(
        "SELECT `Unnamed: 3_level_0 Queue Date` AS Queue_Date, "
        "`Unnamed: 2_level_0 Interconnection Request\nReceive Date` AS Request_Received_Date "
        "FROM grid_generation_queue "
        "WHERE `Unnamed: 3_level_0 Queue Date` IS NOT NULL AND "
        "`Unnamed: 2_level_0 Interconnection Request\nReceive Date` IS NOT NULL", conn,
        parse_dates=['Queue_Date','Request_Received_Date']
    )
    df['lead_time'] = (df['Queue_Date'] - df['Request_Received_Date']).dt.days
    avg = df['lead_time'].mean()
    pd.DataFrame([{'average_lead_time_days': avg}]).to_csv('reports/average_lead_time.csv', index=False)
    print(f"Generated average lead time report: {avg:.1f} days based on {len(df)} projects")

# 7: Top 10 projects by Net MWs to Grid
def top_projects_by_net_mw(conn):
    """
    This KPI identifies the largest generation projects in the queue based on 
    their net MW contribution to the grid. It highlights major upcoming capacity
    additions and their locations, which is valuable for:
    - Transmission planning
    - Market forecasting
    - Resource adequacy assessments
    - Regional capacity projections
    """    
    df = pd.read_sql(
        """
        SELECT 
            `Unnamed: 0_level_0 Project Name` AS project_name,
            `Unnamed: 1_level_0 Queue Position` AS queue_position,
            `MWs Net MWs to Grid` AS net_mw,
            fuel_types,
            'Active' AS status,  -- All projects in grid_generation_queue are active
            `Location County` AS county,
            `Location State` AS state
        FROM grid_generation_queue
        WHERE `MWs Net MWs to Grid` IS NOT NULL
        GROUP BY `Unnamed: 0_level_0 Project Name`, `Unnamed: 1_level_0 Queue Position`
        ORDER BY MAX(`MWs Net MWs to Grid`) DESC
        LIMIT 10        """, conn
    )
    df.to_csv('reports/top_projects_by_net_mw.csv', index=False)
    print(f"Generated top projects by net MW report with {len(df)} unique projects")

# 8: Project timeline delay analysis
def timeline_delay_analysis(conn):
    """
    This KPI measures the difference in days between the originally proposed 
    online date and the current online date for generation projects. This delay 
    analysis provides critical insights into:
    - Project development timeline reliability
    - Systematic delays in the interconnection process
    - Risk factors for project timeline extensions
    - Potential impacts on resource planning and reliability
    - Trends in project development timelines by technology and region
    """
    # First, query the database to get all columns to find the exact names of date columns
    columns_query = pd.read_sql("PRAGMA table_info(grid_generation_queue)", conn)
    
    # Find columns that might contain the proposed and current online date information
    proposed_col = None
    current_col = None
    
    for col in columns_query['name']:
        if 'proposed' in col.lower() and 'on-line' in col.lower():
            proposed_col = col
        if 'current' in col.lower() and 'on-line' in col.lower():
            current_col = col
    
    if not proposed_col or not current_col:
        print("WARNING: Could not find proposed or current online date columns")
        proposed_col = "Point of Interconnection Proposed On-line Date"
        current_col = "Point of Interconnection Current On-line Date"
        print(f"Using default column names: \n - {proposed_col}\n - {current_col}")
    else:
        print(f"Found date columns: \n - {proposed_col}\n - {current_col}")
    
    # Construct and execute the query with the found column names
    query = f"""
    SELECT
        `Unnamed: 0_level_0 Project Name` AS project_name,
        `Unnamed: 1_level_0 Queue Position` AS queue_position,
        `{proposed_col}` AS proposed_online_date,
        `{current_col}` AS current_online_date,
        fuel_types
    FROM grid_generation_queue
    WHERE `{proposed_col}` IS NOT NULL
    AND `{current_col}` IS NOT NULL
    """
    
    try:
        df = pd.read_sql(query, conn, parse_dates=['proposed_online_date', 'current_online_date'])
    except Exception as e:
        print(f"Error executing timeline delay query: {str(e)}")
        print("Attempting alternative approach...")
        
        # Get all data and process in pandas
        df = pd.read_sql("SELECT * FROM grid_generation_queue", conn)
        
        # Look for date columns in the dataframe
        date_cols = [col for col in df.columns if ('proposed' in col.lower() and 'on-line' in col.lower()) 
                     or ('current' in col.lower() and 'on-line' in col.lower())]
        
        if len(date_cols) >= 2:
            proposed_cols = [col for col in date_cols if 'proposed' in col.lower()]
            current_cols = [col for col in date_cols if 'current' in col.lower()]
            
            if proposed_cols and current_cols:
                df = df[[
                    'Unnamed: 0_level_0 Project Name', 
                    'Unnamed: 1_level_0 Queue Position',
                    proposed_cols[0], 
                    current_cols[0], 
                    'fuel_types'
                ]].copy()
                
                df.columns = ['project_name', 'queue_position', 'proposed_online_date', 
                             'current_online_date', 'fuel_types']
                
                # Convert date columns
                df['proposed_online_date'] = pd.to_datetime(df['proposed_online_date'], errors='coerce')
                df['current_online_date'] = pd.to_datetime(df['current_online_date'], errors='coerce')
                
                # Filter for non-null dates
                df = df.dropna(subset=['proposed_online_date', 'current_online_date'])
            else:
                print("Could not identify proposed and current date columns")
                return
        else:
            print("Could not find date columns for analysis")
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
    df.to_csv('reports/project_timeline_delays.csv', index=False)
    
    # Save summary statistics
    pd.DataFrame([delay_stats]).to_csv('reports/timeline_delay_summary.csv', index=False)
    
    # Save fuel type analysis
    fuel_delays.to_csv('reports/timeline_delay_by_fuel.csv', index=False)
    
    print(f"Generated timeline delay analysis with data from {len(df)} projects:")
    print(f"  - Average delay: {delay_stats['average_delay_days']:.1f} days")
    print(f"  - Median delay: {delay_stats['median_delay_days']:.1f} days")
    print(f"  - Range: {delay_stats['min_delay_days']:.1f} to {delay_stats['max_delay_days']:.1f} days")
    print(f"  - Projects delayed: {delay_stats['positive_delay_count']} ({delay_stats['delay_percentage']:.1f}%)")
    print(f"  - Projects accelerated: {delay_stats['negative_delay_count']}")
    print(f"  - Projects on schedule: {delay_stats['no_change_count']}")

# Main analysis function
def main():
    """Run all analysis functions and generate reports."""
    conn = sqlite3.connect(DB_FILE)
    capacity_by_fuel(conn)
    project_count_by_status(conn)
    top5_iso_zones(conn)
    annual_project_status(conn)  
    cancellation_rate(conn)
    average_lead_time(conn)
    top_projects_by_net_mw(conn)
    timeline_delay_analysis(conn)
    conn.close()

if __name__ == '__main__':
    main()