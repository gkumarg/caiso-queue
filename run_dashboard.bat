@echo off
REM Run the Streamlit dashboard locally

echo Starting CAISO Generation Interconnection Queue Dashboard...
cd /d %~dp0 && set PYTHONPATH=%~dp0 && streamlit run dashboard\app.py
