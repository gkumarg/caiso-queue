#!/usr/bin/env python
"""
Run the complete CAISO Queue data pipeline:
1. Download the latest queue report
2. Parse the data and load it into the database
3. Analyze the data and generate reports
"""

import os
import sys
from datetime import datetime

def run_pipeline():
    print(f"Starting CAISO Queue pipeline at {datetime.now()}")

    # Step 1a: Download latest main queue report
    print("\n=== Downloading latest queue report ===")
    try:
        from data_collection import download_queue_report
        latest_file = download_queue_report()
        print(f"Download successful: {latest_file}")
    except Exception as e:
        print(f"Error downloading queue report: {str(e)}")
        sys.exit(1)

    # Step 1b: Download Cluster 15 report
    print("\n=== Downloading Cluster 15 report ===")
    try:
        from data_collection import download_cluster15_report
        cluster15_file = download_cluster15_report()
        print(f"Cluster 15 download successful: {cluster15_file}")
    except Exception as e:
        print(f"Error downloading Cluster 15 report: {str(e)}")
        sys.exit(1)

    # Step 2a: Parse and load main queue data
    print("\n=== Parsing and loading main queue data ===")
    try:
        from parse_queue import main as parse_main
        parse_main()
        print("Main queue parsing completed successfully")
    except Exception as e:
        print(f"Error parsing queue data: {str(e)}")
        sys.exit(1)

    # Step 2b: Parse and load Cluster 15 data
    print("\n=== Parsing and loading Cluster 15 data ===")
    try:
        from parse_queue import ingest_cluster15
        ingest_cluster15()
        print("Cluster 15 parsing completed successfully")
    except Exception as e:
        print(f"Error parsing Cluster 15 data: {str(e)}")
        sys.exit(1)

    # Step 3: Analyze data and generate reports
    print("\n=== Analyzing data and generating reports ===")
    try:
        from analyze_queue import main as analyze_main
        analyze_main()
        print("Data analysis and report generation completed successfully")
    except Exception as e:
        print(f"Error analyzing queue data: {str(e)}")
        sys.exit(1)

    print(f"\nPipeline completed successfully at {datetime.now()}")

if __name__ == '__main__':
    run_pipeline()
