# CAISO Generator Interconnection Queue Dashboard

This dashboard provides interactive visualizations of the CAISO Generator Interconnection Queue data, including key performance indicators (KPIs) and analysis of renewable energy projects in California.

## Features

- Interactive visualizations using Plotly
- Multiple KPI views:
  - Overview with key metrics
  - Capacity by Fuel Type analysis
  - Project Status breakdown
  - Top ISO Zones by capacity
  - Lead Time Analysis
  - Timeline Delays analysis
  - Top Projects by capacity
- Filters for customizing visualizations
- Responsive design for different screen sizes

## Running the Dashboard

### Using Docker (recommended)

The dashboard is containerized and can be run using Docker:

```bash
# Build and run the container
docker-compose up
```

This will start the dashboard at http://localhost:8501

### Running Locally (for development)

To run the dashboard locally:

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Run the dashboard:
   ```
   streamlit run dashboard/app.py
   ```
   
   Or use the convenience script:
   ```
   run_dashboard.bat
   ```

## Data Sources

The dashboard uses data from the CAISO Generator Interconnection Queue, which is processed and stored in the following locations:

- SQLite database: `data/caiso_queue.db`
- CSV reports: `reports/*.csv`

## Dashboard Structure

- `dashboard/app.py`: Main Streamlit application
- `dashboard/data_loader.py`: Utilities for loading data from the database

## Available KPIs

1. **Overview**: Summary of key metrics
2. **Capacity by Fuel Type**: Analysis of generation capacity by different fuel types
3. **Project Status**: Breakdown of projects by status (Active, Completed, Withdrawn)
4. **Top ISO Zones**: The regions with highest generation capacity
5. **Lead Time Analysis**: Analysis of project lead times
6. **Timeline Delays**: Analysis of project timeline delays by fuel type
7. **Top Projects**: List of the largest generation projects by capacity
