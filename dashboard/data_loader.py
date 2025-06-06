"""
Data loading utilities for the CAISO Generator Interconnection Queue Dashboard
"""
import sqlite3
import pandas as pd
import os

class DataLoader:
    """Handles data loading from the CAISO queue database"""
    
    def __init__(self, db_path='data/caiso_queue.db'):
        """Initialize with the path to the SQLite database"""
        self.db_path = db_path
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found at: {db_path}")
            
    def get_conn(self):
        """Get a database connection"""
        return sqlite3.connect(self.db_path)
    
    def capacity_by_fuel(self):
        """Load capacity by fuel type data"""
        try:
            return pd.read_csv('reports/capacity_by_fuel.csv')
        except FileNotFoundError:
            conn = self.get_conn()
            df = pd.read_sql(
                """
                SELECT fuel_types AS fuel, SUM(net_mw) AS total_mw
                FROM grid_generation_queue
                GROUP BY fuel_types
                """, conn
            )
            conn.close()
            return df
    
    def project_count_by_status(self):
        """Load project count by status data"""
        try:
            return pd.read_csv('reports/project_count_by_status.csv')
        except FileNotFoundError:
            conn = self.get_conn()
            df = pd.read_sql(
                """
                SELECT 
                    'Active' as status,
                    COUNT(*) AS project_count,
                    SUM(net_mw) AS total_mw
                FROM grid_generation_queue
                
                UNION ALL
                
                SELECT 
                    'Completed' as status,
                    COUNT(*) AS project_count,
                    SUM(net_mw) AS total_mw
                FROM completed_projects
                
                UNION ALL
                
                SELECT 
                    'Withdrawn' as status,
                    COUNT(*) AS project_count,
                    SUM(net_mw) AS total_mw
                FROM withdrawn_projects
                """, conn
            )
            conn.close()
            return df
    
    def top5_iso_zones(self):
        """Load top 5 ISO zones data"""
        try:
            # First try to read from CSV
            try:
                df = pd.read_csv('reports/top5_iso_zones.csv')
                # Ensure the dataframe has the required columns
                if 'iso_zone' in df.columns and 'total_mw' in df.columns:
                    return df
            except FileNotFoundError:
                pass
                
            # If CSV read fails or has wrong format, query from database
            conn = self.get_conn()
            df = pd.read_sql(
                """
                SELECT pto_study_region AS iso_zone, SUM(net_mw) AS total_mw
                FROM grid_generation_queue
                GROUP BY pto_study_region
                ORDER BY total_mw DESC
                LIMIT 5
                """, conn
            )
            conn.close()
            
            # Validate the dataframe before returning
            if df is not None and not df.empty and 'iso_zone' in df.columns and 'total_mw' in df.columns:
                return df
            else:
                # Return an empty dataframe with the expected schema
                return pd.DataFrame(columns=['iso_zone', 'total_mw'])
                
        except Exception as e:
            print(f"Error in top5_iso_zones: {str(e)}")
            # Return an empty dataframe with the expected schema
            return pd.DataFrame(columns=['iso_zone', 'total_mw'])
    
    def cancellation_rate(self):
        """Load cancellation rate data"""
        try:
            return pd.read_csv('reports/cancellation_rate.csv')
        except FileNotFoundError:
            conn = self.get_conn()
            # Get active MW from active projects
            active = pd.read_sql(
                "SELECT SUM(net_mw) AS total_mw FROM grid_generation_queue", 
                conn
            ).iloc[0,0] or 0
            
            # Get completed MW from completed projects
            completed = pd.read_sql(
                "SELECT SUM(net_mw) AS total_mw FROM completed_projects", 
                conn
            ).iloc[0,0] or 0

            # Get total MW from withdrawn projects
            withdrawn = pd.read_sql(
                "SELECT SUM(net_mw) AS withdrawn_mw FROM withdrawn_projects", 
                conn
            ).iloc[0,0] or 0
            
            conn.close()
            total = active + completed + withdrawn
            rate = withdrawn / total if total else None
            return pd.DataFrame([{'cancellation_rate': rate}])
    
    def average_lead_time(self):
        """Load average lead time data"""
        try:
            return pd.read_csv('reports/average_lead_time.csv')
        except FileNotFoundError:
            conn = self.get_conn()
            df = pd.read_sql(
                "SELECT queue_date AS Queue_Date, "
                "request_receive_date AS Request_Received_Date "
                "FROM grid_generation_queue "
                "WHERE queue_date IS NOT NULL AND "
                "request_receive_date IS NOT NULL", conn,
                parse_dates=['Queue_Date','Request_Received_Date']
            )
            conn.close()
            df['lead_time'] = (df['Queue_Date'] - df['Request_Received_Date']).dt.days
            avg = df['lead_time'].mean()
            return pd.DataFrame([{'average_lead_time_days': avg}])
    
    def top_projects_by_net_mw(self):
        """Load top projects by net MW data"""
        try:
            return pd.read_csv('reports/top_projects_by_net_mw.csv')
        except FileNotFoundError:
            conn = self.get_conn()
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
                GROUP BY project_name, queue_position
                ORDER BY MAX(net_mw) DESC
                LIMIT 10
                """, conn            )
            conn.close()
            return df
    
    def timeline_delay_by_fuel(self):
        """Load timeline delay by fuel data"""
        try:
            df = pd.read_csv('reports/timeline_delay_by_fuel.csv')
            # Rename columns to match what the dashboard expects
            if 'fuel_type' in df.columns and 'average_delay_days' in df.columns:
                df = df.rename(columns={
                    'fuel_type': 'fuel',
                    'average_delay_days': 'avg_delay_days'
                })
            return df
        except FileNotFoundError:
            conn = self.get_conn()
            df = pd.read_sql(
                """
                SELECT
                    fuel_types AS fuel,
                    AVG(timeline_delay) AS avg_delay_days
                FROM (
                    SELECT
                        fuel_types,
                        (
                            julianday(actual_online_date) - 
                            julianday(planned_online_date)
                        ) AS timeline_delay
                    FROM grid_generation_queue
                    WHERE 
                        actual_online_date IS NOT NULL AND
                        planned_online_date IS NOT NULL AND
                        actual_online_date > planned_online_date
                )
                GROUP BY fuel_types
                ORDER BY avg_delay_days DESC
                """, conn
            )
            conn.close()
            return df
    
    def get_active_projects(self):
        """Get all active projects data for filtering"""
        conn = self.get_conn()
        df = pd.read_sql(
            """
            SELECT * FROM grid_generation_queue
            """, conn
        )
        conn.close()
        return df

    def get_project_locations(self, status='all'):
        """Load project location data with optional status filter
        
        Args:
            status (str): Filter by status ('active', 'completed', 'withdrawn', or 'all')
        """
        try:
            conn = self.get_conn()
            
            # Base query for active projects
            active_query = """
                SELECT 
                    project_name,
                    county,
                    state,
                    net_mw as capacity,
                    CAST(latitude AS FLOAT) as latitude,
                    CAST(longitude AS FLOAT) as longitude,
                    'Active' as status
                FROM grid_generation_queue
                WHERE county IS NOT NULL AND state IS NOT NULL
            """
            
            # Base query for completed projects
            completed_query = """
                SELECT 
                    project_name,
                    county,
                    state,
                    net_mw as capacity,
                    CAST(latitude AS FLOAT) as latitude,
                    CAST(longitude AS FLOAT) as longitude,
                    'Completed' as status
                FROM completed_projects
                WHERE county IS NOT NULL AND state IS NOT NULL
            """
            
            # Base query for withdrawn projects
            withdrawn_query = """
                SELECT 
                    project_name,
                    county,
                    state,
                    net_mw as capacity,
                    CAST(latitude AS FLOAT) as latitude,
                    CAST(longitude AS FLOAT) as longitude,
                    'Withdrawn' as status
                FROM withdrawn_projects
                WHERE county IS NOT NULL AND state IS NOT NULL
            """
            
            # Combine queries based on status filter
            if status == 'active':
                query = active_query
            elif status == 'completed':
                query = completed_query
            elif status == 'withdrawn':
                query = withdrawn_query
            else:  # 'all'
                query = f"{active_query} UNION ALL {completed_query} UNION ALL {withdrawn_query}"
            
            df = pd.read_sql(query, conn)
            conn.close()
            
            # Clean up county names (remove 'County' suffix and standardize)
            df['county'] = df['county'].str.replace(' County', '', case=False)
            df['county'] = df['county'].str.strip()
            
            # Ensure latitude and longitude are numeric
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
            
            # Drop rows with invalid coordinates
            df = df.dropna(subset=['latitude', 'longitude'])
            
            return df
            
        except Exception as e:
            print(f"Error in get_project_locations: {str(e)}")
            return pd.DataFrame(columns=['project_name', 'county', 'state', 'capacity', 'latitude', 'longitude', 'status'])
