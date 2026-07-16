# script/load_population_data.py

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

# File paths
DATA_FILE = '../data/population/ssd_2024_population_estimates_data.xlsx'

def create_db_connection():
    """Create database connection string and engine"""
    connection_string = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
    return create_engine(connection_string)

def load_data_to_postgres():
    """Load Excel data to PostgreSQL database"""
    try:
        # Read Excel file
        print(f"Reading data from {DATA_FILE}...")
        df = pd.read_excel(DATA_FILE)
        
        # Clean column names (replace spaces with underscores)
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
        df.columns = (
            df.columns
            .str.strip()
            .str.replace(r'\s+', '_', regex=True)  # replaces spaces, tabs, newlines, etc.
            .str.lower()
        )        



        print(f"Loaded {len(df)} rows with {len(df.columns)} columns")
        print(f"Columns: {', '.join(df.columns)}")
        
        # Create database connection
        engine = create_db_connection()
        
        # Set schema in search path
        with engine.connect() as conn:
            conn.execute(text(f"SET search_path TO {DB_PARAMS['schema']}"))
            conn.commit()
        
        # Load data to database
        table_name = 'population_estimates_2024'
        
        print(f"Loading data to {DB_PARAMS['schema']}.{table_name}...")
        
        df.to_sql(
            table_name, 
            engine, 
            schema=DB_PARAMS['schema'],
            if_exists='replace',  # Use 'append' if you want to add to existing table
            index=False,
            method='multi'  # Faster loading for large datasets
        )
        print(df.head(5))
        # Verify the load
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {DB_PARAMS['schema']}.{table_name}"))
            count = result.scalar()
            print(f"Successfully loaded {count} rows into {DB_PARAMS['schema']}.{table_name}")
        
        print("Data load completed successfully!")
        
    except FileNotFoundError:
        print(f"Error: Data file not found at {DATA_FILE}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    load_data_to_postgres()