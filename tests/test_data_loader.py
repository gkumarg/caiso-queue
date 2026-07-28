"""Tests for dashboard/data_loader.py"""
import pytest
import sqlite3
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dashboard'))

from data_loader import DataLoader


@pytest.fixture
def db_with_cluster15(temp_db):
    """Create a temp DB that includes a cluster_15_requests table."""
    conn = sqlite3.connect(temp_db)
    sample = pd.DataFrame({
        'queue_position': ['C15-001', 'C15-002', 'C15-003'],
        'project_name': ['Solar C15', 'Wind C15', 'Battery C15'],
        'fuel_types': ['Solar', 'Wind', 'Storage'],
        'net_mw': [100.0, 200.0, 50.0],
        'county': ['KERN', 'RIVERSIDE', 'LOS ANGELES'],
        'state': ['CA', 'CA', 'CA'],
        'ingestion_date': ['2026-03-23'] * 3,
        'latitude': [35.3425, 33.9534, 34.3200],
        'longitude': [-118.7299, -117.3962, -118.2250],
        'study_process': ['Cluster 15'] * 3,
    })
    sample.to_sql('cluster_15_requests', conn, if_exists='replace', index=False)
    sample.to_sql('grid_generation_queue', conn, if_exists='replace', index=False)
    sample.to_sql('completed_projects', conn, if_exists='replace', index=False)
    withdrawn = sample.copy()
    withdrawn.rename(columns={'study_process': 'Unnamed: 6_level_0 Study\nProcess'}, inplace=True)
    withdrawn.to_sql('withdrawn_projects', conn, if_exists='replace', index=False)
    conn.close()
    return temp_db


@pytest.mark.unit
class TestCluster15DataLoader:
    """Tests for Cluster 15 DataLoader methods."""

    def test_get_cluster15_projects_returns_dataframe(self, db_with_cluster15):
        loader = DataLoader(db_path=db_with_cluster15)
        df = loader.get_cluster15_projects()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_get_cluster15_projects_has_required_columns(self, db_with_cluster15):
        loader = DataLoader(db_path=db_with_cluster15)
        df = loader.get_cluster15_projects()
        required = {'queue_position', 'project_name', 'fuel_types', 'net_mw'}
        assert required.issubset(set(df.columns))

    def test_get_cluster15_summary_returns_totals(self, db_with_cluster15):
        loader = DataLoader(db_path=db_with_cluster15)
        summary = loader.get_cluster15_summary()
        assert 'total_projects' in summary
        assert 'total_mw' in summary
        assert summary['total_projects'] == 3
        assert summary['total_mw'] == 350.0

    def test_get_cluster15_capacity_by_fuel(self, db_with_cluster15):
        loader = DataLoader(db_path=db_with_cluster15)
        df = loader.get_cluster15_capacity_by_fuel()
        assert isinstance(df, pd.DataFrame)
        assert 'fuel' in df.columns
        assert 'total_mw' in df.columns
        assert len(df) == 3  # Solar, Wind, Storage

    def test_get_latest_ingestion_date_includes_cluster15(self, db_with_cluster15):
        loader = DataLoader(db_path=db_with_cluster15)
        latest = loader.get_latest_ingestion_date()
        assert latest is not None

    def test_get_cluster15_projects_empty_when_no_table(self, temp_db):
        loader = DataLoader(db_path=temp_db)
        df = loader.get_cluster15_projects()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_get_cluster15_summary_zeros_when_no_table(self, temp_db):
        loader = DataLoader(db_path=temp_db)
        summary = loader.get_cluster15_summary()
        assert summary['total_projects'] == 0
        assert summary['total_mw'] == 0.0
