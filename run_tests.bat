@echo off
REM Windows batch script to run tests

echo Running CAISO Queue Test Suite...
echo.

REM Check if pytest is installed
python -m pytest --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: pytest is not installed
    echo Please install test dependencies: pip install -r requirements.txt
    exit /b 1
)

REM Run tests with coverage
echo Running tests with coverage...
python -m pytest -v --cov=scripts --cov-report=term --cov-report=html

if errorlevel 1 (
    echo.
    echo Tests FAILED
    exit /b 1
) else (
    echo.
    echo Tests PASSED
    echo Coverage report generated in htmlcov/index.html
    exit /b 0
)
