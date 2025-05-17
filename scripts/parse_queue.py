import pandas as pd
import sqlite3
from datetime import date

RAW_FILE = 'raw/publicqueuereport.xlsx'
DB_FILE  = 'data/caiso_queue.db'

# Flatten multi‐level headers
def flatten_columns(df):
    df.columns = [
        ' '.join(filter(None, map(str, col))).strip()
        for col in df.columns.values
    ]
    return df

# Parse a single sheet
def parse_sheet(df):
    df = flatten_columns(df)
    df['ingestion_date'] = pd.to_datetime('today').date()
    # Derive combined fuel_types
    fuel_cols = ['Fuel-1', 'Fuel-2', 'Fuel-3']
    df['fuel_types'] = (
        df[fuel_cols]
        .fillna('')
        .apply(lambda row: '/'.join([f for f in row if f]), axis=1)
    )
    return df

# Main ingestion
def main():
    # Load workbook
    sheets = {
        'grid_generation_queue': 'Grid GenerationQueue',
        'completed_projects'    : 'Completed Generation Projects',
        'withdrawn_projects'    : 'Withdrawn Generation Projects'
    }
    conn = sqlite3.connect(DB_FILE)
    for table, sheet_name in sheets.items():
        df = pd.read_excel(
            RAW_FILE,
            header=[2,3],
            sheet_name=sheet_name,
            engine='openpyxl'
        )
        df = parse_sheet(df)
        df.to_sql(table, conn, if_exists='append', index=False)
    # Create indexes
    with conn:
        for idx in ['status', 'county', 'state', 'utility']:
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS idx_{idx} '
                f'ON grid_generation_queue({idx})'
            )
    conn.close()

if __name__ == '__main__':
    main()