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
    
    def capacity_by_fuel(self, study_processes=None):
        """Load capacity by fuel type data

        Args:
            study_processes (list): Optional list of study processes to filter by
        """
        conn = self.get_conn()

        # Build WHERE clause if filtering
        where_clause = ""
        params = []
        if study_processes:
            placeholders = ','.join(['?' for _ in study_processes])
            where_clause = f"WHERE study_process IN ({placeholders})"
            params = study_processes

        df = pd.read_sql(
            f"""
            SELECT fuel_types AS fuel, SUM(net_mw) AS total_mw
            FROM grid_generation_queue
            {where_clause}
            GROUP BY fuel_types
            """, conn, params=params
        )
        conn.close()
        return df
    
    def project_count_by_status(self, study_processes=None):
        """Load project count by status data

        Args:
            study_processes (list): Optional list of study processes to filter by
        """
        conn = self.get_conn()

        # Build WHERE clause if filtering
        where_clause = ""
        params = []
        if study_processes:
            placeholders = ','.join(['?' for _ in study_processes])
            where_clause = f"WHERE study_process IN ({placeholders})"
            # Need to pass parameters 3 times (once for each table)
            params = study_processes * 3

        df = pd.read_sql(
            f"""
            SELECT
                'Active' as status,
                COUNT(*) AS project_count,
                SUM(net_mw) AS total_mw
            FROM grid_generation_queue
            {where_clause}

            UNION ALL

            SELECT
                'Completed' as status,
                COUNT(*) AS project_count,
                SUM(net_mw) AS total_mw
            FROM completed_projects
            {where_clause}

            UNION ALL

            SELECT
                'Withdrawn' as status,
                COUNT(*) AS project_count,
                SUM(net_mw) AS total_mw
            FROM withdrawn_projects
            {where_clause}
            """, conn, params=params
        )
        conn.close()
        return df
    
    def top5_iso_zones(self, study_processes=None):
        """Load top 5 ISO zones data

        Args:
            study_processes (list): Optional list of study processes to filter by
        """
        try:
            # Build WHERE clause if filtering
            where_clause = ""
            params = []
            if study_processes:
                placeholders = ','.join(['?' for _ in study_processes])
                where_clause = f"WHERE study_process IN ({placeholders})"
                params = study_processes

            # Query from database
            conn = self.get_conn()
            df = pd.read_sql(
                f"""
                SELECT pto_study_region AS iso_zone, SUM(net_mw) AS total_mw
                FROM grid_generation_queue
                {where_clause}
                GROUP BY pto_study_region
                ORDER BY total_mw DESC
                LIMIT 5
                """, conn, params=params
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
    
    def cancellation_rate(self, study_processes=None):
        """Load cancellation rate data

        Args:
            study_processes (list): Optional list of study processes to filter by
        """
        conn = self.get_conn()

        # Build WHERE clause if filtering
        where_clause = ""
        params = []
        if study_processes:
            placeholders = ','.join(['?' for _ in study_processes])
            where_clause = f"WHERE study_process IN ({placeholders})"
            params = study_processes

        # Get active MW from active projects
        active = pd.read_sql(
            f"SELECT SUM(net_mw) AS total_mw FROM grid_generation_queue {where_clause}",
            conn, params=params
        ).iloc[0,0] or 0

        # Get completed MW from completed projects
        completed = pd.read_sql(
            f"SELECT SUM(net_mw) AS total_mw FROM completed_projects {where_clause}",
            conn, params=params
        ).iloc[0,0] or 0

        # Get total MW from withdrawn projects
        withdrawn = pd.read_sql(
            f"SELECT SUM(net_mw) AS withdrawn_mw FROM withdrawn_projects {where_clause}",
            conn, params=params
        ).iloc[0,0] or 0

        conn.close()
        total = active + completed + withdrawn
        rate = withdrawn / total if total else None
        return pd.DataFrame([{'cancellation_rate': rate}])
    
    def average_lead_time(self, study_processes=None):
        """Load average lead time data

        Args:
            study_processes (list): Optional list of study processes to filter by
        """
        conn = self.get_conn()

        # Build WHERE clause if filtering
        where_clause = "WHERE queue_date IS NOT NULL AND request_receive_date IS NOT NULL"
        params = []
        if study_processes:
            placeholders = ','.join(['?' for _ in study_processes])
            where_clause += f" AND study_process IN ({placeholders})"
            params = study_processes

        df = pd.read_sql(
            f"SELECT queue_date AS Queue_Date, "
            "request_receive_date AS Request_Received_Date "
            f"FROM grid_generation_queue {where_clause}", conn,
            parse_dates=['Queue_Date','Request_Received_Date'],
            params=params
        )
        conn.close()
        df['lead_time'] = (df['Queue_Date'] - df['Request_Received_Date']).dt.days
        avg = df['lead_time'].mean()
        return pd.DataFrame([{'average_lead_time_days': avg}])
    
    def top_projects_by_net_mw(self, study_processes=None):
        """Load top projects by net MW data

        Args:
            study_processes (list): Optional list of study processes to filter by
        """
        conn = self.get_conn()

        # Build WHERE clause if filtering
        where_clause = "WHERE net_mw IS NOT NULL"
        params = []
        if study_processes:
            placeholders = ','.join(['?' for _ in study_processes])
            where_clause += f" AND study_process IN ({placeholders})"
            params = study_processes

        df = pd.read_sql(
            f"""
            SELECT
                project_name,
                queue_position,
                net_mw,
                fuel_types,
                study_process,
                'Active' AS status,
                county,
                state
            FROM grid_generation_queue
            {where_clause}
            GROUP BY project_name, queue_position
            ORDER BY MAX(net_mw) DESC
            LIMIT 10
            """, conn, params=params
        )
        conn.close()
        return df
    
    def timeline_delay_by_fuel(self, study_processes=None):
        """Load timeline delay by fuel data

        Args:
            study_processes (list): Optional list of study processes to filter by
        """
        conn = self.get_conn()

        # Build WHERE clause if filtering
        where_clause = "WHERE actual_online_date IS NOT NULL AND planned_online_date IS NOT NULL AND actual_online_date > planned_online_date"
        params = []
        if study_processes:
            placeholders = ','.join(['?' for _ in study_processes])
            where_clause += f" AND study_process IN ({placeholders})"
            params = study_processes

        df = pd.read_sql(
            f"""
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
                {where_clause}
            )
            GROUP BY fuel_types
            ORDER BY avg_delay_days DESC
            """, conn, params=params
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

    def get_all_projects(self, table='all', columns=None, study_processes=None):
        """Get all projects data with optional table and column filtering

        Args:
            table (str): Which table to query ('active', 'completed', 'withdrawn', or 'all')
            columns (list): List of column names to select, or None for all columns
            study_processes (list): Optional list of study processes to filter by

        Returns:
            pd.DataFrame: DataFrame with project data
        """
        conn = None
        try:
            conn = self.get_conn()

            # Map friendly names to actual table names
            table_map = {
                'active': 'grid_generation_queue',
                'completed': 'completed_projects',
                'withdrawn': 'withdrawn_projects'
            }

            # Determine column selection
            col_select = '*' if not columns else ', '.join(columns)

            # Build WHERE clause if filtering by study process
            where_clause = ""
            params = []
            if study_processes:
                placeholders = ','.join(['?' for _ in study_processes])
                where_clause = f"WHERE study_process IN ({placeholders})"

            # Build query based on table selection
            if table == 'all':
                # Get all projects from all tables with a status column
                queries = []
                for status, table_name in table_map.items():
                    if columns:
                        # Add status column to selected columns
                        cols_with_status = columns + [f"'{status.capitalize()}' as status"]
                        col_select = ', '.join(cols_with_status)
                    else:
                        col_select = f"*, '{status.capitalize()}' as status"
                    queries.append(f"SELECT {col_select} FROM {table_name} {where_clause}")

                query = ' UNION ALL '.join(queries)
                # Parameters need to be repeated for each UNION
                if study_processes:
                    params = study_processes * len(table_map)
            else:
                # Single table query
                table_name = table_map.get(table, 'grid_generation_queue')
                status = table.capitalize()
                if columns:
                    cols_with_status = columns + [f"'{status}' as status"]
                    col_select = ', '.join(cols_with_status)
                else:
                    col_select = f"*, '{status}' as status"
                query = f"SELECT {col_select} FROM {table_name} {where_clause}"
                if study_processes:
                    params = study_processes

            df = pd.read_sql(query, conn, params=params)
            return df

        except Exception as e:
            print(f"Error in get_all_projects: {str(e)}")
            return pd.DataFrame()
        finally:
            if conn is not None:
                conn.close()

    def get_table_columns(self, table='grid_generation_queue'):
        """Get column names from a specific table

        Args:
            table (str): Table name

        Returns:
            list: List of column names
        """
        conn = None
        try:
            conn = self.get_conn()
            cursor = conn.execute(f"SELECT * FROM {table} LIMIT 0")
            columns = [description[0] for description in cursor.description]
            return columns
        except Exception as e:
            print(f"Error getting columns from {table}: {str(e)}")
            return []
        finally:
            if conn is not None:
                conn.close()

    def get_latest_ingestion_date(self):
        """Return the most recent ingestion date across known tables."""
        conn = None
        try:
            conn = self.get_conn()
            tables = [
                'grid_generation_queue',
                'completed_projects',
                'withdrawn_projects'
            ]
            latest_dates = []
            for table in tables:
                exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,)
                ).fetchone()
                if not exists:
                    continue
                df = pd.read_sql(
                    f"SELECT MAX(ingestion_date) AS latest_date FROM {table}",
                    conn,
                    parse_dates=['latest_date']
                )
                if df.empty:
                    continue
                latest_value = df['latest_date'].iloc[0]
                if pd.isna(latest_value):
                    continue
                latest_dates.append(latest_value)
            if not latest_dates:
                return None
            latest = max(latest_dates)
            return latest.date() if hasattr(latest, 'date') else latest
        except Exception as e:
            print(f"Error getting latest ingestion date: {str(e)}")
            return None
        finally:
            if conn is not None:
                conn.close()

    def get_study_processes(self):
        """Get list of distinct study process values across all tables

        Returns:
            list: Sorted list of unique study process values
        """
        conn = None
        try:
            conn = self.get_conn()
            query = """
                SELECT DISTINCT study_process
                FROM (
                    SELECT study_process FROM grid_generation_queue
                    UNION
                    SELECT study_process FROM completed_projects
                    UNION
                    SELECT study_process FROM withdrawn_projects
                )
                WHERE study_process IS NOT NULL
                ORDER BY study_process
            """
            df = pd.read_sql(query, conn)
            return df['study_process'].tolist()
        except Exception as e:
            print(f"Error getting study processes: {str(e)}")
            return []
        finally:
            if conn is not None:
                conn.close()

    def get_study_process_summary(self, study_processes=None):
        """Get summary statistics by study process

        Args:
            study_processes (list): Optional list of study processes to filter by

        Returns:
            pd.DataFrame: Summary with columns [study_process, project_count, total_mw]
        """
        conn = None
        try:
            conn = self.get_conn()

            # Build WHERE clause if filtering
            where_clause = ""
            if study_processes:
                placeholders = ','.join(['?' for _ in study_processes])
                where_clause = f"WHERE study_process IN ({placeholders})"

            # Query all tables
            query = f"""
                SELECT
                    study_process,
                    COUNT(*) as project_count,
                    SUM(net_mw) as total_mw
                FROM (
                    SELECT study_process, net_mw FROM grid_generation_queue {where_clause}
                    UNION ALL
                    SELECT study_process, net_mw FROM completed_projects {where_clause}
                    UNION ALL
                    SELECT study_process, net_mw FROM withdrawn_projects {where_clause}
                )
                WHERE study_process IS NOT NULL
                GROUP BY study_process
                ORDER BY project_count DESC
            """

            # Execute query with parameters if filtering
            if study_processes:
                # Need to pass parameters twice (once for each table)
                params = study_processes * 3
                df = pd.read_sql(query, conn, params=params)
            else:
                df = pd.read_sql(query, conn)

            return df
        except Exception as e:
            print(f"Error getting study process summary: {str(e)}")
            return pd.DataFrame(columns=['study_process', 'project_count', 'total_mw'])
        finally:
            if conn is not None:
                conn.close()
