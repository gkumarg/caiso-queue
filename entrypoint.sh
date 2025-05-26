#!/bin/bash

# This entrypoint script allows running different commands based on the CMD parameter

# Default to dashboard if no command provided
CMD=${1:-dashboard}

case "$CMD" in
  # Run the complete data pipeline
  pipeline)
    echo "Running complete data pipeline..."
    python scripts/run_pipeline.py && python scripts/analyze_queue.py && python scripts/cleanup_raw.py
    ;;
  
  # Run just the data collection
  collect)
    echo "Running data collection..."
    python scripts/run_pipeline.py
    ;;
    
  # Run just the analysis
  analyze)
    echo "Running data analysis..."
    python scripts/analyze_queue.py
    ;;
  
  # Run validation to check imports are working
  validate)
    echo "Validating setup..."
    cd /app && python dashboard/validate_setup.py
    ;;
    
  # Run diagnostic mode to debug environment issues
  diagnose)
    echo "Running diagnostic mode..."
    cd /app && python /app/debug_env.py
    echo "Checking Python module imports..."
    python -c "import sys; print('Python Path:', sys.path)"
    echo "Trying to import from dashboard..."
    python -c "try:\n  from dashboard.data_loader import DataLoader\n  print('Import successful')\nexcept Exception as e:\n  print(f'Import error: {e}')" || true
    echo "Trying direct import..."
    cd /app/dashboard && python -c "try:\n  from data_loader import DataLoader\n  print('Import successful')\nexcept Exception as e:\n  print(f'Import error: {e}')" || true
    ;;
  
  # Run the Streamlit dashboard
  dashboard)
    echo "Starting CAISO Generator Interconnection Queue Dashboard..."
    # First validate that imports are working
    cd /app && python dashboard/validate_setup.py
    if [ $? -eq 0 ]; then
      echo "Imports validated successfully, starting dashboard..."
      cd /app && PYTHONPATH=/app:/app/dashboard streamlit run dashboard/app.py --server.port=8501 --server.address=0.0.0.0
    else
      echo "Import validation failed. Running diagnostics..."
      cd /app && python /app/debug_env.py
      echo "You can run 'docker-compose run caiso-queue-dashboard diagnose' for more detailed diagnostics."
      exit 1
    fi
    ;;
    
  # Help command
  help)
    echo "Available commands:"
    echo "  pipeline  - Run the complete data pipeline (collection -> analysis -> cleanup)"
    echo "  collect   - Run just the data collection script"
    echo "  analyze   - Run just the data analysis script"
    echo "  dashboard - Run the Streamlit dashboard (default)"
    echo "  help      - Show this help message"
    ;;
    
  # Default case: run the specified command
  *)
    echo "Running custom command: $@"
    exec "$@"
    ;;
esac
