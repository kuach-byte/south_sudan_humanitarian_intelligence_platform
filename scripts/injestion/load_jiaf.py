# script/load_jiaf_data.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'schema': os.getenv('DB_SCHEMA')
}

# File path
DATA_FILE = '../data/humanitarian_needs/jiaf_south_sudan_2026.xlsx'

def create_db_engine():
    """Create database connection engine"""
    connection_string = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
    return create_engine(connection_string)

def load_jiaf_data():
    """Load JIAF Excel data to PostgreSQL"""
    try:
        # Read Excel file
        print(f"Reading data from {DATA_FILE}...")
        df = pd.read_excel(DATA_FILE)
        
        # Clean column names
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('-', '_').str.lower()
        
        print(f"Loaded {len(df)} rows with {len(df.columns)} columns")
        
        # Create connection
        engine = create_db_engine()
        
        # Set schema
        with engine.connect() as conn:
            conn.execute(text(f"SET search_path TO {DB_PARAMS['schema']}"))
            conn.commit()
        
        # Load to database
        table_name = 'jiaf_south_sudan_2026'
        
        print(f"Loading data to {DB_PARAMS['schema']}.{table_name}...")
        
        df.to_sql(
            table_name,
            engine,
            schema=DB_PARAMS['schema'],
            if_exists='replace',
            index=False,
            method='multi'
        )
        
        # Verify
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {DB_PARAMS['schema']}.{table_name}"))
            count = result.scalar()
            print(f"Successfully loaded {count} rows into {DB_PARAMS['schema']}.{table_name}")
        
        print("Data load completed!")
        
    except FileNotFoundError:
        print(f"Error: File not found at {DATA_FILE}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    load_jiaf_data()