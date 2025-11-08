"""
Tests for analyze_queue.py module
"""
import pytest
import pandas as pd
import sqlite3
import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from analyze_queue import (
    _safe_float,
    _safe_int,
    capacity_by_fuel,
    project_count_by_status,
    top5_iso_zones,
)


@pytest.mark.unit
class TestHelperFunctions:
    """Test suite for helper functions in analyze_queue."""

    def test_safe_float_with_valid_number(self):
        """Test _safe_float with valid numeric inputs."""
        assert _safe_float(100.5) == 100.5
        assert _safe_float(100) == 100.0
        assert _safe_float('100.5') == 100.5
        assert _safe_float('100') == 100.0

    def test_safe_float_with_invalid_input(self):
        """Test _safe_float with invalid inputs returns default."""
        assert _safe_float(None) == 0.0
        assert _safe_float('invalid') == 0.0
        assert _safe_float('') == 0.0
        assert _safe_float([]) == 0.0

    def test_safe_float_with_custom_default(self):
        """Test _safe_float with custom default value."""
        assert _safe_float(None, default=-1.0) == -1.0
        assert _safe_float('invalid', default=999.0) == 999.0

    def test_safe_int_with_valid_number(self):
        """Test _safe_int with valid numeric inputs."""
        assert _safe_int(100) == 100
        assert _safe_int(100.9) == 100
        assert _safe_int('100') == 100

    def test_safe_int_with_invalid_input(self):
        """Test _safe_int with invalid inputs returns default."""
        assert _safe_int(None) == 0
        assert _safe_int('invalid') == 0
        assert _safe_int('') == 0

    def test_safe_int_with_custom_default(self):
        """Test _safe_int with custom default value."""
        assert _safe_int(None, default=-1) == -1
        assert _safe_int('invalid', default=999) == 999


@pytest.mark.integration
class TestCapacityByFuel:
    """Test suite for capacity_by_fuel analysis function."""

    def test_capacity_by_fuel_basic(self, mock_db_with_data, temp_dir):
        """Test capacity_by_fuel generates correct CSV output."""
        conn = sqlite3.connect(mock_db_with_data)

        # Mock REPORTS_DIR
        with patch('analyze_queue.REPORTS_DIR', temp_dir):
            capacity_by_fuel(conn)

        conn.close()

        # Check that output file was created
        output_file = os.path.join(temp_dir, 'capacity_by_fuel.csv')
        assert os.path.exists(output_file)

        # Read and validate the output
        df = pd.read_csv(output_file)
        assert 'fuel' in df.columns
        assert 'total_mw' in df.columns
        assert len(df) > 0

    def test_capacity_by_fuel_aggregates_correctly(self, temp_db, temp_dir):
        """Test that capacity_by_fuel correctly aggregates MW by fuel type."""
        # Create test data with known fuel types and capacities
        test_data = pd.DataFrame({
            'queue_position': ['Z001', 'Z002', 'Z003'],
            'fuel_types': ['Solar', 'Solar', 'Wind'],
            'mw_1': [100.0, 150.0, 200.0],
            'ingestion_date': ['2025-01-01', '2025-01-01', '2025-01-01'],
        })

        conn = sqlite3.connect(temp_db)
        test_data.to_sql('grid_generation_queue', conn, if_exists='replace', index=False)

        # Run analysis
        with patch('analyze_queue.REPORTS_DIR', temp_dir):
            capacity_by_fuel(conn)

        conn.close()

        # Verify output
        output_file = os.path.join(temp_dir, 'capacity_by_fuel.csv')
        df = pd.read_csv(output_file)

        # Check aggregation
        solar_row = df[df['fuel'] == 'Solar']
        assert len(solar_row) == 1
        assert solar_row['total_mw'].iloc[0] == 250.0  # 100 + 150

        wind_row = df[df['fuel'] == 'Wind']
        assert len(wind_row) == 1
        assert wind_row['total_mw'].iloc[0] == 200.0


@pytest.mark.integration
class TestProjectCountByStatus:
    """Test suite for project_count_by_status analysis function."""

    def test_project_count_by_status_basic(self, temp_db, temp_dir):
        """Test project_count_by_status generates correct output."""
        # Create test data for all three tables
        active_data = pd.DataFrame({
            'queue_position': ['Z001', 'Z002'],
            'mw_1': [100.0, 200.0],
            'ingestion_date': ['2025-01-01', '2025-01-01'],
        })

        completed_data = pd.DataFrame({
            'queue_position': ['C001'],
            'mw_1': [150.0],
            'ingestion_date': ['2025-01-01'],
        })

        withdrawn_data = pd.DataFrame({
            'queue_position': ['W001'],
            'mw_1': [75.0],
            'ingestion_date': ['2025-01-01'],
        })

        conn = sqlite3.connect(temp_db)
        active_data.to_sql('grid_generation_queue', conn, if_exists='replace', index=False)
        completed_data.to_sql('completed_projects', conn, if_exists='replace', index=False)
        withdrawn_data.to_sql('withdrawn_projects', conn, if_exists='replace', index=False)

        # Run analysis
        with patch('analyze_queue.REPORTS_DIR', temp_dir):
            project_count_by_status(conn)

        conn.close()

        # Verify output
        output_file = os.path.join(temp_dir, 'project_count_by_status.csv')
        assert os.path.exists(output_file)

        df = pd.read_csv(output_file)
        assert 'status' in df.columns
        assert 'project_count' in df.columns
        assert 'total_mw' in df.columns

        # Check all three statuses are present
        statuses = df['status'].tolist()
        assert 'Active' in statuses
        assert 'Completed' in statuses
        assert 'Withdrawn' in statuses


@pytest.mark.integration
class TestTop5IsoZones:
    """Test suite for top5_iso_zones analysis function."""

    def test_top5_iso_zones_basic(self, temp_db, temp_dir):
        """Test top5_iso_zones generates correct output."""
        # Create test data with different ISO zones
        test_data = pd.DataFrame({
            'queue_position': ['Z001', 'Z002', 'Z003', 'Z004', 'Z005', 'Z006'],
            'pto_study_region': ['ISO-1', 'ISO-1', 'ISO-2', 'ISO-2', 'ISO-3', 'ISO-4'],
            'mw_1': [100.0, 200.0, 150.0, 250.0, 300.0, 50.0],
            'ingestion_date': ['2025-01-01'] * 6,
        })

        conn = sqlite3.connect(temp_db)
        test_data.to_sql('grid_generation_queue', conn, if_exists='replace', index=False)

        # Run analysis
        with patch('analyze_queue.REPORTS_DIR', temp_dir):
            top5_iso_zones(conn)

        conn.close()

        # Verify output
        output_file = os.path.join(temp_dir, 'top5_iso_zones.csv')
        assert os.path.exists(output_file)

        df = pd.read_csv(output_file)
        assert len(df) <= 5  # Should return top 5 or fewer

        # Check that results are sorted by capacity (descending)
        assert df['total_mw'].is_monotonic_decreasing or len(df) == 1


@pytest.mark.unit
class TestAnalysisConfiguration:
    """Test configuration and constants in analyze_queue."""

    def test_db_file_constant_exists(self):
        """Test that DB_FILE constant is defined."""
        from analyze_queue import DB_FILE
        assert DB_FILE is not None
        assert isinstance(DB_FILE, str)

    def test_reports_dir_constant_exists(self):
        """Test that REPORTS_DIR constant is defined."""
        from analyze_queue import REPORTS_DIR
        assert REPORTS_DIR is not None
        assert isinstance(REPORTS_DIR, str)


# Import patch for mocking
from unittest.mock import patch
