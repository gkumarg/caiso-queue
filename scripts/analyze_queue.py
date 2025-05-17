import pandas as pd
import sqlite3

DB_FILE = 'data/caiso_queue.db'

# 1: Capacity by fuel types
def capacity_by_fuel(conn):
    df = pd.read_sql(
        """
        SELECT fuel_types AS fuel, SUM(`MWs MW-1`) AS total_mw
        FROM grid_generation_queue
        GROUP BY fuel_types
        """, conn
    )
    df.to_csv('reports/capacity_by_fuel.csv', index=False)

# 2: Project count & capacity by status
def project_count_by_status(conn):
    df = pd.read_sql(
        """
        SELECT `Unnamed: 4_level_0 Application Status` as status, 
               COUNT(*) AS project_count, 
               SUM(`MWs MW-1`) AS total_mw
        FROM grid_generation_queue
        GROUP BY `Unnamed: 4_level_0 Application Status`
        """, conn
    )
    df.to_csv('reports/project_count_by_status.csv', index=False)

# 3: Top 5 ISO Zones by active capacity
def top5_iso_zones(conn):
    df = pd.read_sql(
        """
        SELECT `Point of Interconnection PTO Study Region` AS iso_zone, SUM(`MWs MW-1`) AS total_mw
        FROM grid_generation_queue
        WHERE `Unnamed: 4_level_0 Application Status` = 'Active'
        GROUP BY `Point of Interconnection PTO Study Region`
        ORDER BY total_mw DESC
        LIMIT 5
        """, conn
    )
    df.to_csv('reports/top5_iso_zones.csv', index=False)

# 4: Weekly queue growth (MW)
def weekly_queue_growth(conn):
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

# 5: Cancellation rate
def cancellation_rate(conn):
    total = pd.read_sql("SELECT SUM(`MWs MW-1`) AS total_mw FROM grid_generation_queue", conn).iloc[0,0] or 0
    withdrawn = pd.read_sql(
        "SELECT SUM(`MWs MW-1`) AS withdrawn_mw FROM withdrawn_projects", conn
    ).iloc[0,0] or 0
    rate = withdrawn / total if total else None
    pd.DataFrame([{'cancellation_rate': rate}]).to_csv('reports/cancellation_rate.csv', index=False)

# 6: Average lead time (days)
def average_lead_time(conn):
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

# Main analysis
if __name__ == '__main__':
    conn = sqlite3.connect(DB_FILE)
    capacity_by_fuel(conn)
    project_count_by_status(conn)
    top5_iso_zones(conn)
    weekly_queue_growth(conn)
    cancellation_rate(conn)
    average_lead_time(conn)
    conn.close()