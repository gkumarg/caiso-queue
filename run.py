#!/usr/bin/env python
"""
Helper script to run the CAISO Generator Interconnection Queue Dashboard
"""
import streamlit.web.cli as stcli
import sys
import os

if __name__ == "__main__":
    # Get the directory where this script is located
    dir_path = os.path.dirname(os.path.realpath(__file__))
    
    # Path to the dashboard app
    dashboard_path = os.path.join(dir_path, "dashboard", "app.py")
    
    # Add the current directory to the Python path
    sys.path.insert(0, dir_path)
    
    # Run the Streamlit app
    sys.argv = ["streamlit", "run", dashboard_path, "--server.port=8501", "--server.address=0.0.0.0"]
    sys.exit(stcli.main())
