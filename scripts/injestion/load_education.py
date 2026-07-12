# script/load_education_data.py
import geopandas as gpd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from pathlib import Path

# Load environment variables
load_dotenv()

# Database connection
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)

# File path
BASE_DIR = Path(__file__).parent.parent
GPKG_FILE = BASE_DIR / "data/education/education_facilities.gpkg"

def load_gpkg_to_db(file_path, table_name, schema):
    """Load GeoPackage to PostgreSQL using GeoPandas' to_postgis"""
    print(f"Loading {file_path.name}...")
    
    # Read GeoPackage
    gdf = gpd.read_file(file_path)
    
    # Ensure geometry column is active
    if gdf.geometry.name != 'geometry':
        gdf = gdf.rename_geometry('geometry')
    
    # Load to PostGIS using GeoPandas' native method
    gdf.to_postgis(
        table_name,
        engine,
        schema=schema,
        if_exists='replace',
        index=False
    )
    
    print(f"✓ Loaded {len(gdf)} rows to {schema}.{table_name}")

def main():
    # Create schema if it doesn't exist
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {os.getenv('DB_SCHEMA')}"))
        conn.commit()
    
    # Enable PostGIS if not already enabled
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.commit()
    
    # Load data
    load_gpkg_to_db(GPKG_FILE, 'education_facilities', os.getenv('DB_SCHEMA'))
    
    print("\nEducation data loaded successfully!")

if __name__ == "__main__":
    main()