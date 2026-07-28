"""
Tests for dashboard.data_loader module.
"""
import sqlite3

import pandas as pd

from dashboard.data_loader import DataLoader


def _seed_dashboard_tables(db_path):
    conn = sqlite3.connect(db_path)
    try:
        pd.DataFrame(
            {
                "study_process": ["C01", "C14", "TC"],
                "net_mw": [10.0, 20.0, 30.0],
                "queue_date": ["2026-01-01", "2026-01-02", "2026-01-03"],
                "request_receive_date": ["2025-12-01", "2025-12-02", "2025-12-03"],
            }
        ).to_sql("grid_generation_queue", conn, if_exists="replace", index=False)

        pd.DataFrame(
            {
                "study_process": ["C02"],
                "net_mw": [5.0],
            }
        ).to_sql("completed_projects", conn, if_exists="replace", index=False)

        pd.DataFrame(
            {
                "Unnamed: 6_level_0 Study\nProcess": ["C03"],
                "net_mw": [7.0],
            }
        ).to_sql("withdrawn_projects", conn, if_exists="replace", index=False)
    finally:
        conn.close()


def test_get_study_processes_includes_cluster_15_and_sorts_clusters(temp_db):
    _seed_dashboard_tables(temp_db)
    loader = DataLoader(db_path=temp_db)

    study_processes = loader.get_study_processes()

    assert "C15" in study_processes
    assert study_processes.index("C14") < study_processes.index("C15")
    assert study_processes.index("C15") < study_processes.index("TC")


def test_filtered_metrics_return_zero_values_for_empty_cluster(temp_db):
    _seed_dashboard_tables(temp_db)
    loader = DataLoader(db_path=temp_db)

    status_df = loader.project_count_by_status(study_processes=["C15"])
    cancellation_df = loader.cancellation_rate(study_processes=["C15"])
    lead_time_df = loader.average_lead_time(study_processes=["C15"])

    assert status_df["project_count"].sum() == 0
    assert status_df["total_mw"].sum() == 0
    assert cancellation_df["cancellation_rate"].iloc[0] == 0
    assert lead_time_df["average_lead_time_days"].iloc[0] == 0
