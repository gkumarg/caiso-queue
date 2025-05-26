# How to Run the CAISO Generator Interconnection Queue Dashboard

This document provides instructions for running the CAISO Generator Interconnection Queue Dashboard.

## Using Docker (Recommended)

The recommended way to run the dashboard is using Docker Compose, which ensures all dependencies are properly installed and configured:

```bash
# Start the dashboard service
docker-compose up caiso-queue-dashboard

# To rebuild the image if you make changes
docker build -t caiso-queue:latest .

# To run the data pipeline for new data
docker-compose up caiso-queue-pipeline
```

The dashboard will be available at: http://localhost:8501

### Troubleshooting Docker Import Errors

If you encounter module import errors when running the dashboard in Docker, you can use the diagnostic service to help identify the issue:

```bash
# Run diagnostics to check Python environment and imports
docker-compose run caiso-queue-diagnose

# Validate the setup without starting the dashboard
docker-compose run caiso-queue-dashboard validate
```

Common solutions to import issues:

1. Make sure the dashboard directory is properly mounted in docker-compose.yml
2. Ensure the PYTHONPATH environment variable is set correctly
3. Check that the dashboard/__init__.py file exists
4. Rebuild the Docker image after making changes: `docker-compose build`

## Running Locally (for Development)

If you prefer to run the dashboard locally for development:

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Run the validation script to ensure everything is set up correctly:
   ```
   python dashboard/validate_setup.py
   ```

3. Run the dashboard:
   ```
   # On Windows
   run_dashboard.bat
   
   # Or directly with streamlit
   streamlit run dashboard/app.py
   ```

## Dashboard Features

The dashboard provides the following views:

- Overview with key metrics
- Capacity by Fuel Type analysis
- Project Status breakdown 
- Top ISO Zones by capacity
- Lead Time Analysis
- Timeline Delays analysis
- Top Projects by capacity

Each view includes interactive filters and visualizations to help analyze the CAISO Generator Interconnection Queue data.

## Data Sources

The data is sourced from CAISO's public queue reports and processed through a data pipeline that:

1. Downloads reports from the CAISO website
2. Processes the raw data 
3. Stores it in a SQLite database
4. Generates analysis reports

The dashboard visualizes both the raw queue data and the derived KPIs from the analysis reports.
