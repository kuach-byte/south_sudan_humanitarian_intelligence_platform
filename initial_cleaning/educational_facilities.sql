-- ============================================================
-- CLEANING: raw_data.education_facilities
-- ============================================================
-- Priority: HIGHEST
-- Issues: 5 empty columns, 6+ columns with >20% missing
-- ============================================================

-- Step 1: Create cleaned table (preserve original)
DROP TABLE IF EXISTS cleaned_data.education_facilities_clean;

CREATE TABLE cleaned_data.education_facilities AS
SELECT 
    id,
    name,
    -- Drop name_en (99.1% missing) - not useful
    -- Drop building (80.6% missing) - not useful
    -- Drop operator_type (85.7% missing) - not useful  
    -- Drop addr_city (82.4% missing) - not useful
    -- Drop capacity_persons (100% missing) - DROPPED
    -- Drop addr_full (100% missing) - DROPPED
    -- Drop source (100% missing) - DROPPED
    -- Drop adm4_pcode (100% missing) - DROPPED
    -- Drop adm4_name (100% missing) - DROPPED
    amenity,
    adm1_pcode,
    adm1_name,
    adm2_pcode,
    adm2_name,
    adm3_pcode,
    adm3_name,
    name_latin,
    geometry
FROM raw_data.education_facilities
WHERE name IS NOT NULL;  -- Keep only records with a name

-- Step 2: Add index for performance
CREATE INDEX idx_education_facilities_adm2_pcode 
ON cleaned_data.education_facilities(adm2_pcode);

CREATE INDEX idx_education_facilities_geometry 
ON cleaned_data.education_facilities 
USING GIST (geometry);

-- Step 3: Verify the cleaning
SELECT 
    'education_facilities' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS unique_ids
FROM cleaned_data.education_facilities;