"""
Tests for parse_queue.py module
"""
import pytest
import pandas as pd
import sqlite3
import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from parse_queue import (
    get_county_coordinates,
    flatten_columns,
    parse_sheet,
    COUNTY_COORDS
)


@pytest.mark.unit
class TestCountyCoordinates:
    """Test suite for county coordinate lookup functionality."""

    def test_get_county_coordinates_valid_california_county(self):
        """Test coordinate lookup for a valid California county."""
        lat, lon = get_county_coordinates('KERN', 'CA')

        assert lat is not None
        assert lon is not None
        assert isinstance(lat, float)
        assert isinstance(lon, float)
        # Validate coordinate ranges
        assert -90 <= lat <= 90
        assert -180 <= lon <= 180

    def test_get_county_coordinates_case_insensitive(self):
        """Test that county lookup is case-insensitive."""
        lat1, lon1 = get_county_coordinates('KERN', 'CA')
        lat2, lon2 = get_county_coordinates('kern', 'ca')
        lat3, lon3 = get_county_coordinates('Kern', 'Ca')

        assert lat1 == lat2 == lat3
        assert lon1 == lon2 == lon3

    def test_get_county_coordinates_invalid_county(self):
        """Test that invalid county returns None."""
        lat, lon = get_county_coordinates('INVALID_COUNTY', 'CA')

        assert lat is None
        assert lon is None

    def test_get_county_coordinates_null_input(self):
        """Test that null inputs return None."""
        lat, lon = get_county_coordinates(None, None)
        assert lat is None
        assert lon is None

        lat, lon = get_county_coordinates('KERN', None)
        assert lat is None
        assert lon is None

    def test_get_county_coordinates_multiple_counties(self):
        """Test coordinate lookup for multiple counties (averages)."""
        # Test with counties separated by /
        lat, lon = get_county_coordinates('KERN/RIVERSIDE', 'CA')

        assert lat is not None
        assert lon is not None
        # Should be average of two counties
        kern_lat, kern_lon = COUNTY_COORDS['KERN']
        riverside_lat, riverside_lon = COUNTY_COORDS['RIVERSIDE']
        expected_lat = (kern_lat + riverside_lat) / 2
        expected_lon = (kern_lon + riverside_lon) / 2

        assert abs(lat - expected_lat) < 0.01
        assert abs(lon - expected_lon) < 0.01

    def test_get_county_coordinates_with_whitespace(self):
        """Test that whitespace is handled correctly."""
        lat, lon = get_county_coordinates('  KERN  ', '  CA  ')

        assert lat is not None
        assert lon is not None

    def test_county_coords_dictionary_not_empty(self):
        """Test that COUNTY_COORDS dictionary is populated."""
        assert len(COUNTY_COORDS) > 0
        assert 'KERN' in COUNTY_COORDS
        assert 'LOS ANGELES' in COUNTY_COORDS
        assert 'SAN DIEGO' in COUNTY_COORDS

    def test_county_coords_all_valid_coordinates(self):
        """Test that all coordinates in COUNTY_COORDS are valid."""
        for county, (lat, lon) in COUNTY_COORDS.items():
            assert isinstance(lat, (int, float))
            assert isinstance(lon, (int, float))
            assert -90 <= lat <= 90
            assert -180 <= lon <= 180

    def test_neighboring_state_counties(self):
        """Test that neighboring state counties are included."""
        # Arizona
        lat, lon = get_county_coordinates('YUMA', 'AZ')
        assert lat is not None

        # Nevada
        lat, lon = get_county_coordinates('CLARK', 'NV')
        assert lat is not None


@pytest.mark.unit
class TestFlattenColumns:
    """Test suite for column flattening functionality."""

    def test_flatten_columns_basic(self):
        """Test basic column flattening."""
        # Create DataFrame with multi-level columns
        columns = pd.MultiIndex.from_tuples([
            ('Level1', 'Level2A'),
            ('Level1', 'Level2B'),
            ('Other', 'Column'),
        ])
        df = pd.DataFrame([[1, 2, 3]], columns=columns)

        df_flat = flatten_columns(df)

        assert 'Level1 Level2A' in df_flat.columns
        assert 'Level1 Level2B' in df_flat.columns
        assert 'Other Column' in df_flat.columns

    def test_flatten_columns_single_level(self):
        """Test that single-level columns remain unchanged."""
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

        df_flat = flatten_columns(df)

        assert list(df_flat.columns) == ['col1', 'col2']

    def test_flatten_columns_preserves_data(self):
        """Test that data is preserved during flattening."""
        columns = pd.MultiIndex.from_tuples([('A', 'B'), ('C', 'D')])
        df = pd.DataFrame([[1, 2], [3, 4]], columns=columns)

        df_flat = flatten_columns(df)

        assert df_flat.iloc[0, 0] == 1
        assert df_flat.iloc[0, 1] == 2
        assert df_flat.iloc[1, 0] == 3
        assert df_flat.iloc[1, 1] == 4


@pytest.mark.unit
class TestParseSheet:
    """Test suite for sheet parsing functionality."""

    def test_parse_sheet_adds_ingestion_date(self, sample_queue_data):
        """Test that parse_sheet adds ingestion_date column."""
        df = parse_sheet(sample_queue_data.copy())

        assert 'ingestion_date' in df.columns
        assert df['ingestion_date'].iloc[0] == pd.to_datetime('today').date()

    def test_parse_sheet_creates_fuel_types_column(self, sample_queue_data):
        """Test that fuel_types column is created from fuel columns."""
        df = parse_sheet(sample_queue_data.copy())

        assert 'fuel_types' in df.columns
        # Check that fuel types are derived from fuel columns
        assert 'Solar' in df['fuel_types'].values or 'solar' in df['fuel_types'].values.astype(str).str.lower()

    def test_parse_sheet_adds_coordinates(self, sample_queue_data):
        """Test that latitude and longitude columns are added."""
        df = parse_sheet(sample_queue_data.copy())

        assert 'latitude' in df.columns
        assert 'longitude' in df.columns

    def test_parse_sheet_maps_columns(self, sample_queue_data):
        """Test that columns are mapped to simpler names."""
        df = parse_sheet(sample_queue_data.copy())

        # Check for mapped column names
        assert 'queue_position' in df.columns
        assert 'mw_1' in df.columns
        assert 'net_mw' in df.columns

    def test_parse_sheet_coordinate_validation(self, sample_queue_data):
        """Test that coordinates are validated and converted to float."""
        df = parse_sheet(sample_queue_data.copy())

        # Check that coordinates that were found are valid floats
        valid_coords = df[df['latitude'].notna()]
        if len(valid_coords) > 0:
            for lat, lon in zip(valid_coords['latitude'], valid_coords['longitude']):
                assert isinstance(lat, (int, float))
                assert isinstance(lon, (int, float))
                assert -90 <= lat <= 90
                assert -180 <= lon <= 180


@pytest.mark.integration
class TestParseQueueIntegration:
    """Integration tests for parse_queue module."""

    def test_parse_and_store_to_database(self, sample_queue_data, temp_db):
        """Test parsing data and storing to SQLite database."""
        df = parse_sheet(sample_queue_data.copy())

        # Store to database
        conn = sqlite3.connect(temp_db)
        df.to_sql('test_queue', conn, if_exists='replace', index=False)

        # Read back and verify
        df_read = pd.read_sql('SELECT * FROM test_queue', conn)
        conn.close()

        assert len(df_read) == len(df)
        assert 'queue_position' in df_read.columns
        assert 'latitude' in df_read.columns

    def test_duplicate_queue_positions_handling(self, temp_db):
        """Test that duplicate queue positions can be detected."""
        conn = sqlite3.connect(temp_db)

        # Insert data with duplicate queue positions
        data = pd.DataFrame({
            'queue_position': ['Z001', 'Z002', 'Z001'],  # Z001 is duplicate
            'project_name': ['Project A', 'Project B', 'Project A Updated'],
            'ingestion_date': ['2025-01-01', '2025-01-01', '2025-01-02'],
        })
        data.to_sql('test_queue', conn, if_exists='replace', index=False)

        # Query for duplicates
        query = """
            SELECT queue_position, COUNT(*) as count
            FROM test_queue
            GROUP BY queue_position
            HAVING COUNT(*) > 1
        """
        duplicates = pd.read_sql(query, conn)
        conn.close()

        assert len(duplicates) > 0
        assert 'Z001' in duplicates['queue_position'].values


@pytest.mark.unit
class TestCluster15Parsing:
    """Tests for Cluster 15 data ingestion into SQLite."""

    def test_ingest_cluster15_function_exists(self):
        """Test that ingest_cluster15 function is importable and callable."""
        from parse_queue import ingest_cluster15
        assert callable(ingest_cluster15)

    def test_cluster15_raw_file_constant_exists(self):
        """Test that CLUSTER15_RAW_FILE constant is defined."""
        from parse_queue import CLUSTER15_RAW_FILE
        assert 'cluster-15' in CLUSTER15_RAW_FILE
        assert CLUSTER15_RAW_FILE.endswith('.xlsx')

    def test_parse_cluster15_creates_table(self, temp_db):
        """Test that a cluster_15_requests table can be created and holds rows."""
        conn = sqlite3.connect(temp_db)
        sample_df = pd.DataFrame({
            'project_name': ['Solar C15-001', 'Wind C15-002'],
            'queue_position': ['C15-001', 'C15-002'],
            'net_mw': [150.0, 300.0],
            'fuel_types': ['Solar', 'Wind'],
            'county': ['KERN', 'RIVERSIDE'],
            'state': ['CA', 'CA'],
            'study_process': ['C15', 'C15'],
            'ingestion_date': ['2026-03-23', '2026-03-23'],
            'latitude': [35.3425, 33.9534],
            'longitude': [-118.7299, -117.3962],
        })
        sample_df.to_sql('cluster_15_requests', conn, if_exists='replace', index=False)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert 'cluster_15_requests' in tables
        count = conn.execute("SELECT COUNT(*) FROM cluster_15_requests").fetchone()[0]
        assert count == 2
        conn.close()

    def test_ingest_cluster15_skips_missing_file(self, temp_db):
        """Test that ingest_cluster15 skips gracefully when file not found."""
        from unittest.mock import patch
        from parse_queue import ingest_cluster15
        with patch('parse_queue.CLUSTER15_RAW_FILE', '/nonexistent/path.xlsx'), \
             patch('parse_queue.DB_FILE', temp_db):
            ingest_cluster15()  # Should not raise

    def test_ingest_cluster15_idempotent(self, temp_db):
        """Test that running ingest twice on same date does not duplicate rows."""
        import tempfile as tmpmod
        from unittest.mock import patch
        from parse_queue import ingest_cluster15

        sample_data = pd.DataFrame({
            'Queue Number': ['2207'],
            'Project Name': ['Solar C15-001'],
            'NET MW POI': [500.0],
            'Generation/Fuel 1': ['Photovoltaic/Solar'],
            'PROJECT COUNTY': ['Kern'],
            'Project State': ['CA'],
        })

        tmp = tmpmod.NamedTemporaryFile(suffix='.xlsx', delete=False)
        tmp.close()
        # Write with sheet name matching what ingest_cluster15 expects
        with pd.ExcelWriter(tmp.name, engine='openpyxl') as writer:
            sample_data.to_excel(writer, sheet_name='Cluster 15 ', index=False)

        try:
            with patch('parse_queue.CLUSTER15_RAW_FILE', tmp.name), \
                 patch('parse_queue.DB_FILE', temp_db):
                ingest_cluster15()
                ingest_cluster15()  # Second run — should not duplicate

            conn = sqlite3.connect(temp_db)
            count = conn.execute(
                "SELECT COUNT(*) FROM cluster_15_requests"
            ).fetchone()[0]
            conn.close()
            assert count == 1, f"Expected 1 row after idempotent run, got {count}"
        finally:
            import os as _os
            try:
                _os.unlink(tmp.name)
            except PermissionError:
                pass  # Windows may hold file handle briefly
