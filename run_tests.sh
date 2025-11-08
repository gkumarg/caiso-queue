#!/bin/bash
# Shell script to run tests (Linux/Mac)

echo "Running CAISO Queue Test Suite..."
echo

# Check if pytest is installed
if ! python -m pytest --version > /dev/null 2>&1; then
    echo "ERROR: pytest is not installed"
    echo "Please install test dependencies: pip install -r requirements.txt"
    exit 1
fi

# Run tests with coverage
echo "Running tests with coverage..."
python -m pytest -v --cov=scripts --cov-report=term --cov-report=html

if [ $? -eq 0 ]; then
    echo
    echo "Tests PASSED"
    echo "Coverage report generated in htmlcov/index.html"
    exit 0
else
    echo
    echo "Tests FAILED"
    exit 1
fi
