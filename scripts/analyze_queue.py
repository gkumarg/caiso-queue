import pandas as pd
import sqlite3

DB_FILE = 'data/caiso_queue.db'

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

# 4: Weekly queue growth (MW)
def weekly_queue_growth(conn):
    """
    This KPI tracks the weekly growth in megawatts (MW) of projects in the 
    generation queue. By resampling data to weekly intervals, it provides:
    - Trend analysis of queue growth over time
    - Identification of seasonal patterns in project submissions
    - Early indicators of market response to policy changes
    - Data for forecasting future interconnection request volumes
    - Insights into market sentiment and investment cycles
    """
    df = pd.read_sql(
        "SELECT ingestion_date, SUM(`MWs MW-1`) AS weekly_mw "
        "FROM grid_generation_queue "
        "GROUP BY ingestion_date", conn,
        parse_dates=['ingestion_date']
    )
    df = (
        df
        .set_index('ingestion_date')
        .resample('W-MON')
        .sum()
        .reset_index()
    )
    df.to_csv('reports/weekly_queue_growth.csv', index=False)
    print(f"Generated weekly queue growth report with {len(df)} weeks of data")

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
    # Get total MW from active projects
    total = pd.read_sql(
        "SELECT SUM(`MWs MW-1`) AS total_mw FROM grid_generation_queue", 
        conn
    ).iloc[0,0] or 0
    
    # Get total MW from withdrawn projects
    withdrawn = pd.read_sql(
        "SELECT SUM(`MWs MW-1`) AS withdrawn_mw FROM withdrawn_projects", 
        conn
    ).iloc[0,0] or 0
    
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

# Main analysis function
def main():
    """Run all analysis functions and generate reports."""
    conn = sqlite3.connect(DB_FILE)
    capacity_by_fuel(conn)
    project_count_by_status(conn)
    top5_iso_zones(conn)
    weekly_queue_growth(conn)
    cancellation_rate(conn)
    average_lead_time(conn)
    top_projects_by_net_mw(conn)  # Add the new KPI to the execution
    conn.close()

if __name__ == '__main__':
    main()