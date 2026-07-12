DROP TABLE IF EXISTS cleaned_data.health_facilities;

CREATE TABLE cleaned_data.health_facilities AS
SELECT 
    old_state AS state,
    state_code,
    county,
    county_code,
    payam,
    payam_code,
    site AS facility_name,
    -- Drop site_dhis2_name (32.1% missing, standardized version of site)
    latitude,
    longitude,
    -- Flag missing coordinates
    CASE 
        WHEN latitude IS NULL OR longitude IS NULL THEN TRUE
        ELSE FALSE
    END AS missing_coordinates
FROM raw_data.health_facilities;

-- Add indexes
CREATE INDEX idx_health_county_code 
ON cleaned_data.health_facilities(county_code);

CREATE INDEX idx_health_payam_code 
ON cleaned_data.health_facilities(payam_code);

CREATE INDEX idx_health_missing_coords 
ON cleaned_data.health_facilities(missing_coordinates);

-- Verify the cleaning
SELECT 
    'health_facilities' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT facility_name) AS unique_facilities,
    SUM(CASE WHEN missing_coordinates THEN 1 ELSE 0 END) AS missing_coords_count,
    ROUND(100.0 * SUM(CASE WHEN missing_coordinates THEN 1 ELSE 0 END) / COUNT(*), 2) AS missing_coords_percent
FROM cleaned_data.health_facilities;