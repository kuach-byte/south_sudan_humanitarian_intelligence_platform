DROP TABLE IF EXISTS cleaned_data.health_facility_type;

CREATE TABLE cleaned_data.health_facility_type AS
SELECT
    "State",
    "State_Code",
    "County",
    "County_Code",
    "Payam",
    "Payam_Code ",
    "Facility_Name",
    "Type" AS "Facility_type",
    "Facilities_Code",
    "Latitude",
    "Longitude",
    CASE
        WHEN "Latitude" IS NULL OR "Longitude" IS NULL THEN TRUE
        ELSE FALSE
    END AS missing_coordinates
FROM raw_data.health_facility_type;

-- Add indexes
CREATE INDEX idx_health_type_clean_county_code 
ON cleaned_data.health_facility_type("County_Code");

CREATE INDEX idx_health_type_clean_payam_code 
ON cleaned_data.health_facility_type("Payam_Code ");

CREATE INDEX idx_health_type_clean_facility_type 
ON cleaned_data.health_facility_type("Facility_type");

-- Verify the cleaning
SELECT 
    'health_facility_type' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT "Facility_Name") AS unique_facilities,
    COUNT(DISTINCT "Facility_type") AS unique_types,
    SUM(CASE WHEN "missing_coordinates" THEN 1 ELSE 0 END) AS missing_coords_count,
    ROUND(100.0 * SUM(CASE WHEN "missing_coordinates" THEN 1 ELSE 0 END) / COUNT(*), 2) AS missing_coords_percent
FROM cleaned_data.health_facility_type;