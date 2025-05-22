# CAISO Queue ETL & Analysis

This project automates the collection, processing, and analysis of CAISO's interconnection queue data.

## Features

- Automated weekly data collection from CAISO website
- Historical data tracking with dated snapshots
- Comprehensive analysis and KPI generation
- GitHub Actions automation for consistent data updates
- Email notifications for pipeline status

## Setup

### Local Development

1. Ensure Docker & pipenv are installed
2. Clone the repository
3. Build: `docker build -t caiso-queue:latest .`
4. Run the complete pipeline:   
   ```bash
   docker run --rm \
     -v %CD%/reports:/app/reports \
     -v %CD%/raw:/app/raw \
     -e SMTP_HOST=... \
     -e SMTP_USER=... \
     -e SMTP_PASS=... \
     -e NOTIFICATION_EMAIL=... \
     caiso-queue:latest \
     sh -c "python scripts/run_pipeline.py && python scripts/analyze_queue.py && python scripts/cleanup_raw.py"
   ```

### Automated Updates

The project includes a GitHub Actions workflow that:
1. Downloads the latest CAISO queue report every Monday
2. Processes and analyzes the data
3. Generates updated reports
4. Commits changes back to the repository

## Directory Structure

Refer to the top of this document.

## Available KPIs and Reports

The analysis generates the following KPIs in the `reports/` directory:

1. **Capacity by Fuel Type** (`capacity_by_fuel.csv`)  
   Aggregated capacity in MW for each fuel type combination in the queue.

2. **Project Count by Status** (`project_count_by_status.csv`)  
   Number of projects and total MW capacity grouped by application status.

3. **Top 5 ISO Zones** (`top5_iso_zones.csv`)  
   The 5 ISO zones with the highest active capacity.

4. **Weekly Queue Growth** (`weekly_queue_growth.csv`)  
   Weekly growth in MW capacity added to the queue.

5. **Cancellation Rate** (`cancellation_rate.csv`)  
   Ratio of withdrawn projects to active projects, measured in MW.

6. **Average Lead Time** (`average_lead_time.csv`)  
   Average days between interconnection request reception and queue date.

7. **Top Projects by Net MW** (`top_projects_by_net_mw.csv`)  
   The 10 largest projects by net MW contribution to the grid, including project name,
   location, fuel type, and status.

## Pipeline Components

1. **Data Collection** (`data_collection.py`)
   - Downloads the latest queue report from CAISO website
   - Saves with date suffix for historical tracking
   - Maintains a standard filename for compatibility

2. **Data Processing** (`parse_queue.py`)
   - Parses multi-sheet Excel workbook
   - Handles complex header structures
   - Loads data into SQLite database

3. **Analysis** (`analyze_queue.py`)
   - Generates standardized reports and KPIs
   - Outputs CSV files for further analysis
   - Tracks changes over time

4. **Maintenance** (`cleanup_raw.py`)
   - Manages historical data retention
   - Cleans up old raw files
   - Maintains optimal storage usage

## Environment Setup

### GitHub Actions Setup

1. Go to your repository on GitHub
2. Navigate to Settings → Secrets and variables → Actions
3. Add the following secrets:
   - `SMTP_HOST`: Your SMTP server address
   - `SMTP_USER`: SMTP username
   - `SMTP_PASS`: SMTP password
   - `NOTIFICATION_EMAIL`: Notification recipient

### Local Development

1. Create a `.env` file:
   ```ini
   SMTP_HOST=smtp.example.com
   SMTP_USER=your_user
   SMTP_PASS=your_pass
   NOTIFICATION_EMAIL=you@example.com
   ```

2. Run with environment file:
   ```cmd
   docker run --rm ^
     --env-file .env ^
     -v %CD%/reports:/app/reports ^
     -v %CD%/raw:/app/raw ^
     caiso-queue:latest ^
     sh -c "python scripts/run_pipeline.py"
   ```

## Testing

1. Place a sample XLSX file in `raw/` directory
2. Run the pipeline locally to verify:
   - Data collection and parsing
   - Report generation
   - Database updates
3. Check `data/caiso_queue.db` for processed data
4. Verify reports in `reports/` directory