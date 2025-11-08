"""
Pytest configuration and shared fixtures for the CAISO Queue test suite.
"""
import os
import sys
import sqlite3
import tempfile
import shutil
from pathlib import Path

import pytest
import pandas as pd

# Add the parent directory to the path so we can import from scripts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


@pytest.fixture(scope="session")
def test_data_dir():
    """Returns the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def sample_queue_data():
    """Returns a sample DataFrame mimicking CAISO queue data structure."""
    data = {
        'Unnamed: 0_level_0 Project Name': ['Solar Farm A', 'Wind Park B', 'Battery Storage C'],
        'Unnamed: 1_level_0 Queue Position': ['Z001', 'Z002', 'Z003'],
        'Unnamed: 4_level_0 Application Status': ['Active', 'Active', 'In Progress'],
        'Generating Facility Fuel-1': ['Solar', 'Wind', 'Storage'],
        'Generating Facility Fuel-2': [None, None, None],
        'Generating Facility Fuel-3': [None, None, None],
        'MWs MW-1': [100.0, 200.0, 50.0],
        'MWs MW-2': [0.0, 0.0, 0.0],
        'MWs MW-3': [0.0, 0.0, 0.0],
        'MWs Net MWs to Grid': [100.0, 200.0, 50.0],
        'Location County': ['KERN', 'RIVERSIDE', 'LOS ANGELES'],
        'Location State': ['CA', 'CA', 'CA'],
        'Point of Interconnection Utility': ['SCE', 'SDG&E', 'SCE'],
        'Point of Interconnection PTO Study Region': ['ISO-1', 'ISO-2', 'ISO-1'],
    }
    return pd.DataFrame(data)


@pytest.fixture(scope="session")
def sample_withdrawn_data():
    """Returns a sample DataFrame for withdrawn projects."""
    data = {
        'Unnamed: 0_level_0 Project Name - Confidential': ['Failed Project X', 'Cancelled Y'],
        'Unnamed: 1_level_0 Queue Position': ['W001', 'W002'],
        'Generating Facility Fuel-1': ['Solar', 'Wind'],
        'Generating Facility Fuel-2': [None, None],
        'Generating Facility Fuel-3': [None, None],
        'MWs MW-1': [150.0, 75.0],
        'Location County': ['FRESNO', 'KERN'],
        'Location State': ['CA', 'CA'],
    }
    return pd.DataFrame(data)


@pytest.fixture
def temp_db():
    """Creates a temporary SQLite database for testing."""
    # Create a temporary file
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    yield db_path

    # Cleanup
    try:
        os.remove(db_path)
    except OSError:
        pass


@pytest.fixture
def temp_dir():
    """Creates a temporary directory for testing."""
    temp_path = tempfile.mkdtemp()

    yield temp_path

    # Cleanup
    try:
        shutil.rmtree(temp_path)
    except OSError:
        pass


@pytest.fixture
def mock_db_with_data(temp_db):
    """Creates a temporary database with sample data."""
    conn = sqlite3.connect(temp_db)

    # Create a sample table
    sample_data = pd.DataFrame({
        'queue_position': ['Z001', 'Z002', 'Z003'],
        'project_name': ['Project A', 'Project B', 'Project C'],
        'fuel_types': ['Solar', 'Wind', 'Storage'],
        'mw_1': [100.0, 200.0, 50.0],
        'net_mw': [100.0, 200.0, 50.0],
        'county': ['KERN', 'RIVERSIDE', 'LOS ANGELES'],
        'state': ['CA', 'CA', 'CA'],
        'application_status': ['Active', 'Active', 'In Progress'],
        'ingestion_date': ['2025-01-01', '2025-01-01', '2025-01-01'],
        'latitude': [35.3425, 33.9534, 34.3200],
        'longitude': [-118.7299, -117.3962, -118.2250],
    })

    sample_data.to_sql('grid_generation_queue', conn, if_exists='replace', index=False)

    # Create indexes
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_queue_position ON grid_generation_queue(queue_position)'
    )
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_ingestion_date ON grid_generation_queue(ingestion_date)'
    )

    conn.close()

    return temp_db


@pytest.fixture
def sample_excel_data():
    """Returns sample data structured like CAISO Excel file with multi-level headers."""
    # Create multi-level column headers
    columns = pd.MultiIndex.from_tuples([
        ('Unnamed: 0_level_0', 'Project Name'),
        ('Unnamed: 1_level_0', 'Queue Position'),
        ('Unnamed: 4_level_0', 'Application Status'),
        ('Generating Facility', 'Fuel-1'),
        ('MWs', 'MW-1'),
        ('Location', 'County'),
        ('Location', 'State'),
    ])

    data = [
        ['Solar Farm A', 'Z001', 'Active', 'Solar', 100.0, 'KERN', 'CA'],
        ['Wind Park B', 'Z002', 'Active', 'Wind', 200.0, 'RIVERSIDE', 'CA'],
    ]

    return pd.DataFrame(data, columns=columns)


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Sets up mock environment variables for testing."""
    monkeypatch.setenv('SMTP_HOST', 'smtp.test.com')
    monkeypatch.setenv('SMTP_USER', 'test@test.com')
    monkeypatch.setenv('SMTP_PASS', 'test_password')
    monkeypatch.setenv('NOTIFICATION_EMAIL', 'notify@test.com')
