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