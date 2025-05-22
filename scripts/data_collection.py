#!/usr/bin/env python
"""
Download the latest CAISO Queue Report Excel file from the CAISO website.
The file will be saved with a date suffix for tracking historical data.
"""

import os
import sys
import requests
from datetime import datetime
import shutil

# Constants
CAISO_URL = "https://www.caiso.com/documents/publicqueuereport.xlsx"
RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'raw')

def download_queue_report():
    """
    Download the latest queue report from CAISO website.
    Returns the path to the downloaded file.
    """
    # Create raw directory if it doesn't exist
    os.makedirs(RAW_DIR, exist_ok=True)
    
    # Generate filename with date suffix
    today = datetime.now()
    date_suffix = today.strftime("-%m%d%Y")
    output_filename = f"publicqueuereport{date_suffix}.xlsx"
    output_path = os.path.join(RAW_DIR, output_filename)
    
    # Download the file
    print(f"Downloading CAISO Queue Report from {CAISO_URL}")
    try:
        response = requests.get(CAISO_URL, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Save the file
        with open(output_path, 'wb') as f:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, f)
        
        print(f"Successfully downloaded to: {output_path}")
        
        # Create a symlink or copy for the base filename
        base_file = os.path.join(RAW_DIR, "publicqueuereport.xlsx")
        if os.path.exists(base_file):
            os.remove(base_file)
        
        # On Windows, we'll create a copy instead of a symlink
        shutil.copy2(output_path, base_file)
        print(f"Created latest file copy at: {base_file}")
        
        return output_path
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    download_queue_report()
