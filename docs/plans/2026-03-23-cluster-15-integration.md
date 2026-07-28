# Cluster 15 Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Download, parse, and surface CAISO Cluster 15 Interconnection Requests data in the existing dashboard and CI/CD pipeline alongside the weekly public queue report.

**Architecture:** The Cluster 15 Excel file is a separate CAISO publication with an unknown but likely different schema from the main queue report. We store it in a dedicated SQLite table (`cluster_15_requests`) to avoid schema collisions. The dashboard gains a "Cluster 15" section (new `DataLoader` methods + new Streamlit view). GitHub Actions downloads both files on the same Monday schedule.

**Tech Stack:** Python 3.10, pandas, openpyxl, SQLite3, Streamlit, GitHub Actions

---

## Pre-work: Inspect the Cluster 15 File

Before writing any parsers, you must understand what you're parsing.

### Task 0: Inspect File Structure

**Files:**
- No changes yet — inspection only

**Step 1: Download the file manually**

```bash
curl -L -o /tmp/cluster15.xlsx \
  "https://www.caiso.com/documents/cluster-15-interconnection-requests.xlsx"
```

**Step 2: Inspect sheet names and column structure**

```python
import pandas as pd

xl = pd.ExcelFile('/tmp/cluster15.xlsx')
print("Sheets:", xl.sheet_names)

for sheet in xl.sheet_names:
    df = pd.read_excel('/tmp/cluster15.xlsx', sheet_name=sheet, nrows=5)
    print(f"\n--- Sheet: {sheet} ---")
    print("Columns:", df.columns.tolist())
    print(df.head(2))
```

Run: `python /tmp/inspect_cluster15.py`

**Step 3: Document findings**

Record:
- Sheet names (one sheet? Multiple?)
- Whether headers are multi-level (like the main report uses `header=[2,3]`)
- Which columns overlap with `column_mapping.py` (e.g. `Queue Position`, `Project Name`, `Net MWs to Grid`)
- Any cluster-study-specific columns (e.g. `Cluster Study Phase`, `ITC Cluster Status`, `Provisional Allocation MW`)

**Step 4: Update this plan if the schema is radically different**

If the file has the same schema as the main queue report sheets — the `parse_sheet()` function in `parse_queue.py` can be reused as-is. If it has unique columns, you will need a custom column mapping (see Task 2).

---

## Task 1: Add Cluster 15 Download to data_collection.py

**Files:**
- Modify: `scripts/data_collection.py`
- Test: `tests/test_data_collection.py`

**Step 1: Write the failing tests**

Open `tests/test_data_collection.py` and add at the end of the `TestDataCollection` class:

```python
def test_cluster15_url_is_defined(self):
    """Test that CLUSTER15_URL constant is defined."""
    from data_collection import CLUSTER15_URL
    assert CLUSTER15_URL is not None
    assert isinstance(CLUSTER15_URL, str)
    assert CLUSTER15_URL.startswith('http')
    assert '.xlsx' in CLUSTER15_URL
    assert 'cluster-15' in CLUSTER15_URL

@patch('data_collection.requests.get')
def test_download_cluster15_report_success(self, mock_get):
    """Test successful download of Cluster 15 report."""
    import tempfile, shutil
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.raw = Mock()
    mock_response.raw.decode_content = True
    mock_response.raw.read = Mock(return_value=b'fake excel data')
    mock_get.return_value = mock_response

    temp_dir = tempfile.mkdtemp()
    try:
        with patch('data_collection.RAW_DIR', temp_dir):
            from data_collection import download_cluster15_report
            output_path = download_cluster15_report()
            assert output_path is not None
            assert 'cluster-15' in output_path.lower()
            assert output_path.endswith('.xlsx')
    finally:
        shutil.rmtree(temp_dir)

@patch('data_collection.requests.get')
def test_download_cluster15_report_creates_latest_copy(self, mock_get):
    """Test that download creates both dated and 'latest' files."""
    import tempfile, shutil
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.raw = Mock()
    mock_response.raw.decode_content = True
    mock_response.raw.read = Mock(return_value=b'fake excel data')
    mock_get.return_value = mock_response

    temp_dir = tempfile.mkdtemp()
    try:
        with patch('data_collection.RAW_DIR', temp_dir):
            from data_collection import download_cluster15_report
            download_cluster15_report()
            files = os.listdir(temp_dir)
            # Expect both the dated file and the 'latest' copy
            latest = [f for f in files if f == 'cluster-15-interconnection-requests.xlsx']
            dated = [f for f in files if 'cluster-15' in f and f != 'cluster-15-interconnection-requests.xlsx']
            assert len(latest) == 1, f"Expected latest copy, found: {files}"
            assert len(dated) == 1, f"Expected dated file, found: {files}"
    finally:
        shutil.rmtree(temp_dir)
```

**Step 2: Run tests to verify they fail**

```bash
cd d:/DataScience/caiso-queue
python -m pytest tests/test_data_collection.py::TestDataCollection::test_cluster15_url_is_defined -v
```

Expected: `ImportError: cannot import name 'CLUSTER15_URL'`

**Step 3: Implement the changes in data_collection.py**

In `scripts/data_collection.py`, after the `CAISO_URL` line:

```python
CLUSTER15_URL = "https://www.caiso.com/documents/cluster-15-interconnection-requests.xlsx"
CLUSTER15_FILENAME = "cluster-15-interconnection-requests"
```

Then add a new function after `download_queue_report()`:

```python
def download_cluster15_report():
    """
    Download the CAISO Cluster 15 Interconnection Requests Excel file.
    Returns the path to the downloaded file.
    """
    os.makedirs(RAW_DIR, exist_ok=True)

    today = datetime.now()
    date_suffix = today.strftime("-%m%d%Y")
    output_filename = f"{CLUSTER15_FILENAME}{date_suffix}.xlsx"
    output_path = os.path.join(RAW_DIR, output_filename)

    print(f"Downloading CAISO Cluster 15 report from {CLUSTER15_URL}")
    try:
        response = requests.get(CLUSTER15_URL, stream=True)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, f)

        print(f"Successfully downloaded to: {output_path}")

        base_file = os.path.join(RAW_DIR, f"{CLUSTER15_FILENAME}.xlsx")
        if os.path.exists(base_file):
            os.remove(base_file)
        shutil.copy2(output_path, base_file)
        print(f"Created latest file copy at: {base_file}")

        return output_path

    except requests.exceptions.RequestException as e:
        print(f"Error downloading Cluster 15 file: {str(e)}")
        sys.exit(1)
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_data_collection.py -v -k "cluster15"
```

Expected: All 3 new cluster15 tests PASS

**Step 5: Commit**

```bash
git add scripts/data_collection.py tests/test_data_collection.py
git commit -m "feat: add Cluster 15 download function to data_collection"
```

---

## Task 2: Add Cluster 15 Column Mapping

> **Note:** Complete Task 0 first. If the Cluster 15 file uses the same multi-level header format as the main queue sheets, skip this task. If it has unique columns, create the mapping here.

**Files:**
- Modify: `scripts/column_mapping.py`
- Test: `tests/test_column_mapping.py`

**Step 1: Write failing tests for any new Cluster 15 columns found in Task 0**

Open `tests/test_column_mapping.py` and add a new test class:

```python
class TestCluster15ColumnMapping:
    """Tests for Cluster 15 specific column mappings."""

    def test_cluster15_columns_present_in_mapping(self):
        """Test that Cluster 15 specific columns are in the mapping."""
        from column_mapping import get_column_mapping
        mapping = get_column_mapping()
        # Replace these with actual column names found during Task 0 inspection
        # Example placeholders — update after inspecting the file:
        # assert 'Queue Position' in mapping or 'queue_position' in mapping.values()
        assert mapping is not None
        assert isinstance(mapping, dict)
```

> **Important:** After Task 0 inspection, replace the placeholder assertions with actual Cluster 15 column names. If Cluster 15 columns exactly match the existing mapping, this test just verifies the mapping dict is non-empty and this task is done.

**Step 2: Run test to see current state**

```bash
python -m pytest tests/test_column_mapping.py -v
```

**Step 3: Add any new Cluster 15 columns to COLUMN_MAPPING in column_mapping.py**

Only add entries for columns unique to Cluster 15. If the file has the same headers as the main report, no changes needed. Example (adjust based on actual inspection):

```python
# Cluster 15 specific columns (if any found during inspection)
# 'Cluster 15 Study Phase': 'cluster_study_phase',
# 'Provisional Allocation MW': 'provisional_allocation_mw',
```

**Step 4: Run tests to confirm they pass**

```bash
python -m pytest tests/test_column_mapping.py -v
```

**Step 5: Commit**

```bash
git add scripts/column_mapping.py tests/test_column_mapping.py
git commit -m "feat: add Cluster 15 column mappings"
```

---

## Task 3: Parse Cluster 15 Data into Database

**Files:**
- Modify: `scripts/parse_queue.py`
- Test: `tests/test_parse_queue.py`

**Step 1: Write failing tests**

Open `tests/test_parse_queue.py` and add a new test class at the end:

```python
class TestCluster15Parsing:
    """Tests for Cluster 15 data ingestion into SQLite."""

    def test_parse_cluster15_creates_table(self, temp_db):
        """Test that parsing Cluster 15 data creates the cluster_15_requests table."""
        import sqlite3
        conn = sqlite3.connect(temp_db)

        # Minimal sample DataFrame matching Cluster 15 structure
        # Update columns based on Task 0 findings
        sample_df = pd.DataFrame({
            'project_name': ['Solar C15-001', 'Wind C15-002'],
            'queue_position': ['C15-001', 'C15-002'],
            'net_mw': [150.0, 300.0],
            'fuel_types': ['Solar', 'Wind'],
            'county': ['KERN', 'RIVERSIDE'],
            'state': ['CA', 'CA'],
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

    def test_ingest_cluster15_function_exists(self):
        """Test that ingest_cluster15 function exists in parse_queue."""
        from parse_queue import ingest_cluster15
        assert callable(ingest_cluster15)

    def test_ingest_cluster15_idempotent(self, temp_db):
        """Test that running ingest twice on same date does not duplicate rows."""
        import sqlite3
        from unittest.mock import patch

        # Build a minimal fake cluster15 Excel file
        import tempfile
        import pandas as pd

        # Create sample data as a simple single-header DataFrame
        sample_data = pd.DataFrame({
            'Project Name': ['Solar C15-001'],
            'Queue Position': ['C15-001'],
            'Net MWs to Grid': [150.0],
            'Fuel-1': ['Solar'],
            'County': ['KERN'],
            'State': ['CA'],
        })

        temp_xlsx = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        temp_xlsx.close()
        sample_data.to_excel(temp_xlsx.name, index=False)

        try:
            with patch('parse_queue.CLUSTER15_RAW_FILE', temp_xlsx.name), \
                 patch('parse_queue.DB_FILE', temp_db):
                from parse_queue import ingest_cluster15
                ingest_cluster15()
                ingest_cluster15()  # Run twice

            conn = sqlite3.connect(temp_db)
            count = conn.execute(
                "SELECT COUNT(*) FROM cluster_15_requests"
            ).fetchone()[0]
            conn.close()
            assert count == 1, f"Expected 1 row after idempotent run, got {count}"
        finally:
            import os
            os.unlink(temp_xlsx.name)
```

**Step 2: Run to verify tests fail**

```bash
python -m pytest tests/test_parse_queue.py::TestCluster15Parsing -v
```

Expected: `ImportError: cannot import name 'ingest_cluster15'`

**Step 3: Add CLUSTER15_RAW_FILE constant and ingest_cluster15() to parse_queue.py**

At the top of `scripts/parse_queue.py`, after `RAW_FILE`:

```python
CLUSTER15_RAW_FILE = 'raw/cluster-15-interconnection-requests.xlsx'
```

Then add this function after the existing `main()` function:

```python
def ingest_cluster15():
    """
    Parse and ingest the CAISO Cluster 15 Interconnection Requests Excel file
    into the cluster_15_requests SQLite table.

    The Cluster 15 file may have a simpler (single-level) header row vs the
    multi-level headers in the main queue report. Adjust header= below if needed
    based on Task 0 inspection findings.
    """
    ensure_dirs()

    if not os.path.exists(CLUSTER15_RAW_FILE):
        print(f"Cluster 15 file not found at {CLUSTER15_RAW_FILE}, skipping.")
        return

    print(f"Processing Cluster 15 file: {CLUSTER15_RAW_FILE}")

    # -----------------------------------------------------------------------
    # IMPORTANT: Update header= here after Task 0 inspection.
    # If single-level headers: header=0
    # If multi-level like the main report: header=[2,3]
    # -----------------------------------------------------------------------
    xl = pd.ExcelFile(CLUSTER15_RAW_FILE)
    print(f"Cluster 15 sheets: {xl.sheet_names}")

    # Use the first sheet by default; adjust if the file has named sheets
    df = pd.read_excel(
        CLUSTER15_RAW_FILE,
        sheet_name=0,       # change to sheet name string if needed after Task 0
        header=0,           # change to [2,3] if multi-level headers found
        engine='openpyxl'
    )
    print(f"Cluster 15 loaded with {len(df)} rows")
    df = parse_sheet(df)

    # Drop rows with no queue position
    queue_pos_col = 'queue_position'
    if queue_pos_col in df.columns:
        before = len(df)
        df = df.dropna(subset=[queue_pos_col])
        df = df[df[queue_pos_col].astype(str).str.strip() != '']
        print(f"Dropped {before - len(df)} rows with empty Queue Position")

    table = 'cluster_15_requests'
    today = pd.to_datetime('today').date()
    conn = sqlite3.connect(DB_FILE)

    try:
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        ).fetchone() is not None

        if table_exists:
            # Remove today's records to allow re-runs
            conn.execute(
                f"DELETE FROM {table} WHERE ingestion_date = ?",
                (today.strftime('%Y-%m-%d'),)
            )
            conn.commit()
            print(f"Cleared today's existing records from {table}")

            # Remove older records for same queue positions (keep latest snapshot)
            if queue_pos_col in df.columns:
                queue_positions = df[queue_pos_col].dropna().unique().tolist()
                for batch in [queue_positions[i:i+500] for i in range(0, len(queue_positions), 500)]:
                    placeholders = ','.join(['?' for _ in batch])
                    conn.execute(
                        f"DELETE FROM {table} WHERE {queue_pos_col} IN ({placeholders})"
                        f" AND ingestion_date < ?",
                        (*batch, today.strftime('%Y-%m-%d'))
                    )
                conn.commit()

        df.to_sql(table, conn, if_exists='append', index=False)
        print(f"Wrote {len(df)} rows to {table}")

        # Create indexes
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{table}_queue_position '
            f'ON {table}(queue_position)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{table}_ingestion_date '
            f'ON {table}(ingestion_date)'
        )
        conn.commit()
        print(f"Indexes created on {table}")
    finally:
        conn.close()
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_parse_queue.py::TestCluster15Parsing -v
```

Expected: All 3 tests PASS

> **Note:** The idempotency test uses a simplified single-header DataFrame. If Task 0 shows the real file uses multi-level headers, adjust the test fixture accordingly — the core assertion (no duplicate rows on second run) stays the same.

**Step 5: Commit**

```bash
git add scripts/parse_queue.py tests/test_parse_queue.py
git commit -m "feat: add ingest_cluster15 function and CLUSTER15_RAW_FILE constant"
```

---

## Task 4: Wire Cluster 15 into the Pipeline

**Files:**
- Modify: `scripts/run_pipeline.py`
- Test: `tests/test_run_pipeline.py`

**Step 1: Write failing tests**

Open `tests/test_run_pipeline.py` and add a test verifying the cluster15 steps are called:

```python
def test_pipeline_calls_cluster15_download(monkeypatch):
    """Test that run_pipeline calls download_cluster15_report."""
    from unittest.mock import patch, MagicMock
    import sys

    calls = []

    with patch('data_collection.download_queue_report', return_value='/fake/path.xlsx'), \
         patch('data_collection.download_cluster15_report',
               side_effect=lambda: calls.append('cluster15_download') or '/fake/cluster15.xlsx'), \
         patch('parse_queue.main'), \
         patch('parse_queue.ingest_cluster15',
               side_effect=lambda: calls.append('cluster15_ingest')), \
         patch('analyze_queue.main'):
        import run_pipeline
        run_pipeline.run_pipeline()

    assert 'cluster15_download' in calls, "download_cluster15_report was not called"
    assert 'cluster15_ingest' in calls, "ingest_cluster15 was not called"
```

**Step 2: Run to verify it fails**

```bash
python -m pytest tests/test_run_pipeline.py::test_pipeline_calls_cluster15_download -v
```

Expected: FAIL (run_pipeline doesn't call cluster15 functions yet)

**Step 3: Update run_pipeline.py**

In `scripts/run_pipeline.py`, add Cluster 15 steps after the existing download and parse steps:

```python
def run_pipeline():
    print(f"Starting CAISO Queue pipeline at {datetime.now()}")

    # Step 1a: Download latest main queue report
    print("\n=== Downloading latest queue report ===")
    try:
        from data_collection import download_queue_report
        latest_file = download_queue_report()
        print(f"Download successful: {latest_file}")
    except Exception as e:
        print(f"Error downloading queue report: {str(e)}")
        sys.exit(1)

    # Step 1b: Download Cluster 15 report
    print("\n=== Downloading Cluster 15 report ===")
    try:
        from data_collection import download_cluster15_report
        cluster15_file = download_cluster15_report()
        print(f"Cluster 15 download successful: {cluster15_file}")
    except Exception as e:
        print(f"Error downloading Cluster 15 report: {str(e)}")
        sys.exit(1)

    # Step 2a: Parse and load main queue data
    print("\n=== Parsing and loading main queue data ===")
    try:
        from parse_queue import main as parse_main
        parse_main()
        print("Main queue parsing completed successfully")
    except Exception as e:
        print(f"Error parsing queue data: {str(e)}")
        sys.exit(1)

    # Step 2b: Parse and load Cluster 15 data
    print("\n=== Parsing and loading Cluster 15 data ===")
    try:
        from parse_queue import ingest_cluster15
        ingest_cluster15()
        print("Cluster 15 parsing completed successfully")
    except Exception as e:
        print(f"Error parsing Cluster 15 data: {str(e)}")
        sys.exit(1)

    # Step 3: Analyze data and generate reports
    print("\n=== Analyzing data and generating reports ===")
    try:
        from analyze_queue import main as analyze_main
        analyze_main()
        print("Data analysis and report generation completed successfully")
    except Exception as e:
        print(f"Error analyzing queue data: {str(e)}")
        sys.exit(1)

    print(f"\nPipeline completed successfully at {datetime.now()}")
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_run_pipeline.py -v
```

Expected: All tests PASS including the new cluster15 test

**Step 5: Commit**

```bash
git add scripts/run_pipeline.py tests/test_run_pipeline.py
git commit -m "feat: wire Cluster 15 download and ingest into run_pipeline"
```

---

## Task 5: Add Cluster 15 Methods to DataLoader

**Files:**
- Modify: `dashboard/data_loader.py`
- Test: `tests/test_data_loader.py` (create if not exists — check with `ls tests/`)

**Step 1: Write failing tests**

Create or open `tests/test_data_loader.py`:

```python
"""Tests for dashboard/data_loader.py"""
import pytest
import sqlite3
import pandas as pd
import sys, os

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
    # Also need the regular tables so DataLoader doesn't break on other queries
    sample.to_sql('cluster_15_requests', conn, if_exists='replace', index=False)
    sample.to_sql('grid_generation_queue', conn, if_exists='replace', index=False)
    sample.to_sql('completed_projects', conn, if_exists='replace', index=False)
    # withdrawn_projects needs slightly different schema
    withdrawn = sample.rename(columns={'study_process': 'Unnamed: 6_level_0 Study\nProcess'})
    withdrawn.to_sql('withdrawn_projects', conn, if_exists='replace', index=False)
    conn.close()
    return temp_db


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
```

**Step 2: Run to verify they fail**

```bash
python -m pytest tests/test_data_loader.py::TestCluster15DataLoader -v
```

Expected: `AttributeError: 'DataLoader' object has no attribute 'get_cluster15_projects'`

**Step 3: Add methods to dashboard/data_loader.py**

Add the following at the end of the `DataLoader` class (before the final closing):

```python
def get_cluster15_projects(self):
    """Get all Cluster 15 interconnection request projects."""
    conn = None
    try:
        conn = self.get_conn()
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='cluster_15_requests'"
        ).fetchone()
        if not table_exists:
            return pd.DataFrame()
        df = pd.read_sql("SELECT * FROM cluster_15_requests", conn)
        return df
    except Exception as e:
        print(f"Error in get_cluster15_projects: {str(e)}")
        return pd.DataFrame()
    finally:
        if conn is not None:
            conn.close()

def get_cluster15_summary(self):
    """Get summary statistics for Cluster 15 projects.

    Returns:
        dict with keys: total_projects, total_mw
    """
    conn = None
    try:
        conn = self.get_conn()
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='cluster_15_requests'"
        ).fetchone()
        if not table_exists:
            return {'total_projects': 0, 'total_mw': 0.0}
        row = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(net_mw), 0) FROM cluster_15_requests"
        ).fetchone()
        return {'total_projects': row[0], 'total_mw': row[1]}
    except Exception as e:
        print(f"Error in get_cluster15_summary: {str(e)}")
        return {'total_projects': 0, 'total_mw': 0.0}
    finally:
        if conn is not None:
            conn.close()

def get_cluster15_capacity_by_fuel(self):
    """Get Cluster 15 capacity broken down by fuel type."""
    conn = None
    try:
        conn = self.get_conn()
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='cluster_15_requests'"
        ).fetchone()
        if not table_exists:
            return pd.DataFrame(columns=['fuel', 'total_mw'])
        df = pd.read_sql(
            """
            SELECT fuel_types AS fuel, SUM(net_mw) AS total_mw
            FROM cluster_15_requests
            GROUP BY fuel_types
            ORDER BY total_mw DESC
            """, conn
        )
        return df
    except Exception as e:
        print(f"Error in get_cluster15_capacity_by_fuel: {str(e)}")
        return pd.DataFrame(columns=['fuel', 'total_mw'])
    finally:
        if conn is not None:
            conn.close()
```

Also update `get_latest_ingestion_date()` to include `cluster_15_requests` in its tables list:

```python
# In get_latest_ingestion_date(), find:
tables = [
    'grid_generation_queue',
    'completed_projects',
    'withdrawn_projects'
]
# Change to:
tables = [
    'grid_generation_queue',
    'completed_projects',
    'withdrawn_projects',
    'cluster_15_requests'
]
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_data_loader.py::TestCluster15DataLoader -v
```

Expected: All 5 tests PASS

**Step 5: Commit**

```bash
git add dashboard/data_loader.py tests/test_data_loader.py
git commit -m "feat: add Cluster 15 methods to DataLoader"
```

---

## Task 6: Add Cluster 15 Section to Dashboard

**Files:**
- Modify: `dashboard/app.py`

No automated tests for Streamlit UI — verify manually by running the dashboard.

**Step 1: Add Cluster 15 to KPI_OPTIONS**

In `dashboard/app.py`, find:

```python
KPI_OPTIONS = [
    "Overview",
    ...
    "Data Table"
]
```

Add `"Cluster 15"` to the list (just before `"Data Table"`):

```python
KPI_OPTIONS = [
    "Overview",
    "Capacity by Fuel Type",
    "Project Status",
    "Top ISO Zones",
    "Lead Time Analysis",
    "Timeline Delays",
    "Top Projects",
    "Project Map",
    "Cluster 15",
    "Data Table"
]
```

**Step 2: Add create_cluster15_view() function**

Add this function before the `main()` function (or wherever `create_overview` is defined):

```python
def create_cluster15_view():
    """Create the Cluster 15 Interconnection Requests view."""
    loader = get_data_loader()
    if not loader:
        return

    st.header("Cluster 15 Interconnection Requests")
    st.caption(
        "Data sourced from CAISO's Cluster 15 Interconnection Requests publication. "
        "This is a dedicated cluster study dataset separate from the weekly Public Queue Report."
    )

    summary = loader.get_cluster15_summary()

    if summary['total_projects'] == 0:
        st.warning(
            "No Cluster 15 data found in the database. "
            "Run the pipeline to download and ingest the Cluster 15 file."
        )
        return

    # KPI row
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Cluster 15 Projects", f"{summary['total_projects']:,}")
    with col2:
        mw = summary['total_mw']
        st.metric("Cluster 15 Total Capacity", format_mw(mw) if mw else "N/A")

    st.divider()

    # Capacity by fuel chart
    fuel_df = loader.get_cluster15_capacity_by_fuel()
    if not fuel_df.empty:
        st.subheader("Capacity by Fuel Type")
        fig = px.bar(
            fuel_df,
            x='fuel',
            y='total_mw',
            title="Cluster 15 — Capacity by Fuel Type",
            labels={'fuel': 'Fuel Type', 'total_mw': 'Total MW'},
            color='fuel'
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Full project table
    st.subheader("All Cluster 15 Projects")
    projects_df = loader.get_cluster15_projects()
    if not projects_df.empty:
        # Show a clean subset of columns
        display_cols = [c for c in [
            'queue_position', 'project_name', 'fuel_types', 'net_mw',
            'county', 'state', 'application_status', 'study_process',
            'proposed_online_date', 'current_online_date'
        ] if c in projects_df.columns]
        st.dataframe(projects_df[display_cols], use_container_width=True)
    else:
        st.info("No project records available.")
```

**Step 3: Wire the new view into the routing logic**

Find the section in `app.py` that handles KPI option routing (a block of `if selected_kpi == "..."` or `elif` statements) and add:

```python
elif selected_kpi == "Cluster 15":
    create_cluster15_view()
```

**Step 4: Manual verification**

```bash
cd d:/DataScience/caiso-queue
streamlit run dashboard/app.py
```

- Navigate to the "Cluster 15" menu item
- If no data is yet ingested, you should see the "No Cluster 15 data found" warning (correct)
- After running the pipeline, the view should display metrics, a bar chart, and a data table

**Step 5: Commit**

```bash
git add dashboard/app.py
git commit -m "feat: add Cluster 15 view to dashboard"
```

---

## Task 7: Update GitHub Actions to Download Cluster 15

**Files:**
- Modify: `.github/workflows/download.yml`

**Step 1: No automated test — review manually**

GitHub Actions workflows can't be unit-tested easily. Read the current workflow:

```bash
cat .github/workflows/download.yml
```

**Step 2: Update download.yml**

The `Run complete pipeline` step already calls `run_pipeline.py` which (after Task 4) now downloads and ingests Cluster 15. The only required change is ensuring the `git add` step commits the new `cluster-15-*.xlsx` files from `raw/`.

Find this line in `.github/workflows/download.yml`:

```yaml
git add raw/ data/ reports/
```

No change needed — `raw/` already captures the new cluster15 xlsx files. However, verify the commit message is still appropriate. If you want to make it explicit, you can update it:

```yaml
git commit -m "Update CAISO Queue + Cluster 15 data and reports $(date +'%Y-%m-%d')" || echo "No changes to commit"
```

**Step 3: (Optional) Add a separate workflow for Cluster 15 if update frequency differs**

The Cluster 15 file is a static study document — it may not update weekly like the queue report. To avoid unnecessary re-downloads, you can add a check in `download_cluster15_report()`:

```python
def download_cluster15_report(force=False):
    """
    Download Cluster 15 report. If the dated file for today already exists
    and force=False, skip the download.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    today = datetime.now()
    date_suffix = today.strftime("-%m%d%Y")
    output_filename = f"{CLUSTER15_FILENAME}{date_suffix}.xlsx"
    output_path = os.path.join(RAW_DIR, output_filename)

    if os.path.exists(output_path) and not force:
        print(f"Cluster 15 file for today already exists at {output_path}, skipping download.")
        return output_path

    # ... rest of download logic unchanged
```

This prevents re-downloading if the Monday pipeline runs multiple times. Update the test `test_download_cluster15_report_success` to patch or temporarily delete any pre-existing file.

**Step 4: Commit**

```bash
git add .github/workflows/download.yml scripts/data_collection.py
git commit -m "feat: update GitHub Actions and add idempotent Cluster 15 download"
```

---

## Task 8: End-to-End Smoke Test

**Step 1: Run the full test suite**

```bash
cd d:/DataScience/caiso-queue
python -m pytest tests/ -v --tb=short
```

Expected: All existing tests pass + all new tests pass. Note any failures and fix before proceeding.

**Step 2: Run the pipeline locally end-to-end**

```bash
cd d:/DataScience/caiso-queue
python scripts/run_pipeline.py
```

Expected output includes:
```
=== Downloading latest queue report ===
Successfully downloaded to: raw/publicqueuereport-MMDDYYYY.xlsx
=== Downloading Cluster 15 report ===
Successfully downloaded to: raw/cluster-15-interconnection-requests-MMDDYYYY.xlsx
=== Parsing and loading main queue data ===
...
=== Parsing and loading Cluster 15 data ===
Cluster 15 sheets: [...]
Cluster 15 loaded with N rows
Wrote N rows to cluster_15_requests
=== Analyzing data and generating reports ===
...
Pipeline completed successfully
```

**Step 3: Verify database**

```python
import sqlite3
conn = sqlite3.connect('data/caiso_queue.db')
tables = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table'"
).fetchall()]
print("Tables:", tables)  # Should include cluster_15_requests

count = conn.execute("SELECT COUNT(*) FROM cluster_15_requests").fetchone()[0]
print(f"Cluster 15 rows: {count}")  # Should be > 0
conn.close()
```

**Step 4: Verify dashboard**

```bash
streamlit run dashboard/app.py
```

Navigate to "Cluster 15" — should show real data with metrics and charts.

**Step 5: Final commit**

```bash
git add -A
git commit -m "feat: complete Cluster 15 integration — pipeline, DB, and dashboard"
```

---

## Adjustment Notes

These will be filled in after Task 0 file inspection:

| Question | Answer (fill in after Task 0) |
|---|---|
| Number of sheets in Cluster 15 file | ? |
| Header row format (single or multi-level) | ? |
| `header=` parameter needed for `pd.read_excel` | ? (0 or [2,3]) |
| Column names that differ from main report | ? |
| New column mappings needed in column_mapping.py | ? |
| Projects in Cluster 15 also in main queue report? | ? (overlap check) |
