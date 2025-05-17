import os, glob
from datetime import datetime, timedelta

RAW_DIR = 'raw'
CUTOFF = datetime.now() - timedelta(weeks=52)

if __name__ == '__main__':
    for path in glob.glob(os.path.join(RAW_DIR, '*.xlsx')):
        fname = os.path.basename(path)
        date_str = fname.split('-')[0]
        try:
            file_date = datetime.strptime(date_str, '%Y-%m-%d')
            if file_date < CUTOFF:
                os.remove(path)
                print(f"Deleted: {path}")
        except Exception:
            continue