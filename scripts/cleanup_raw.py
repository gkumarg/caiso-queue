import os, glob
from datetime import datetime, timedelta

RAW_DIR = 'raw'

def main():
    """Clean up old XLSX files in the raw directory that are older than 1 year."""
    cutoff = datetime.now() - timedelta(weeks=52)
    files_cleaned = 0

    for path in glob.glob(os.path.join(RAW_DIR, '*.xlsx')):
        fname = os.path.basename(path)
        if fname == 'publicqueuereport.xlsx':  # Skip the main file
            continue
            
        try:
            date_str = fname.split('-')[1].split('.')[0]  # Extract date from publicqueuereport-MMDDYYYY.xlsx
            file_date = datetime.strptime(date_str, '%m%d%Y')
            if file_date < cutoff:
                os.remove(path)
                print(f"Deleted old file: {path}")
                files_cleaned += 1
        except (IndexError, ValueError) as e:
            print(f"Skipping {fname}: Unable to parse date")
            continue
        except Exception as e:
            print(f"Error processing {fname}: {str(e)}")
            continue
    
    print(f"Cleanup complete. Removed {files_cleaned} old files.")

if __name__ == '__main__':
    main()