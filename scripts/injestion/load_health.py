# script/load_health_data.py
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from pathlib import Path

# Load environment variables
load_dotenv()

# Database connection
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)

# File paths (relative to project root)
BASE_DIR = Path(__file__).parent.parent
FACILITIES_FILE = BASE_DIR / "data/health/health_facilities.xlsx"
FACILITY_TYPE_FILE = BASE_DIR / "data/health/health_facility_type.xlsx"

def load_excel_to_db(file_path, table_name, schema):
    """Load Excel file to PostgreSQL table"""
    print(f"Loading {file_path.name}...")
    
    # Read Excel
    df = pd.read_excel(file_path)
    
    # Load to database
    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists='replace',  # Use 'append' if you want to keep existing data
        index=False,
        method='multi'
    )
    
    print(f"✓ Loaded {len(df)} rows to {schema}.{table_name}")

def main():
    # Create schema if it doesn't exist
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {os.getenv('DB_SCHEMA')}"))
        conn.commit()
    
    # Load data
    load_excel_to_db(FACILITIES_FILE, 'health_facilities', os.getenv('DB_SCHEMA'))
    load_excel_to_db(FACILITY_TYPE_FILE, 'health_facility_type', os.getenv('DB_SCHEMA'))
    
    print("\n Data loaded successfully!")

if __name__ == "__main__":
    main()