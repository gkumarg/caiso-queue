# CAISO Queue ETL & Analysis

## Setup

1. Ensure Docker & pipenv are installed.
2. Copy your `publicqueuereport.xlsx` into `raw/`.
3. Build: `docker build -t caiso-queue:latest .`
4. Run ingestion & analysis:   
   ```bash
   docker run --rm \
     -e SMTP_HOST=... \
     -e SMTP_USER=... \
     -e SMTP_PASS=... \
     -e NOTIFICATION_EMAIL=... \
     caiso-queue:latest
   ```

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

## Testing Offline

- Place sample XLSX in `raw/`.
- Build & run container; verify `data/caiso_queue.db` and CSVs in `reports/`.

## Storing Secrets

**GitHub Actions**  
1. Go to your repository on GitHub.  
2. Navigate to Settings → Secrets and variables → Actions.  
3. Click "New repository secret" and add the following keys with your values:  
   - `SMTP_HOST`  
   - `SMTP_USER`  
   - `SMTP_PASS`  
   - `NOTIFICATION_EMAIL`  
4. These will be automatically injected into your workflows via `${{ secrets.SMTP_HOST }}`, etc.

**Local Testing**  
- Create a `.env` file in your project root containing:  
  ```ini
  SMTP_HOST=smtp.example.com
  SMTP_USER=your_user
  SMTP_PASS=your_pass
  NOTIFICATION_EMAIL=you@example.com
  ```
- Install `python-dotenv` or use your shell to load env vars:  
  ```bash
  export $(grep -v '^#' .env | xargs)
  ```
- Then run Docker with `--env-file .env`:  
  ```bash
  docker run --rm --env-file .env caiso-queue:latest
  ```