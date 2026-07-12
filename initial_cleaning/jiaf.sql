DROP TABLE IF EXISTS cleaned_data.jiaf_south_sudan_2026_clean;

CREATE TABLE cleaned_data.jiaf_south_sudan_2026_clean AS
SELECT 
    location AS col_1,
    "unnamed:_1" AS col_2,
    "unnamed:_2" AS col_3,
    "unnamed:_3" AS col_4,
    -- DROP "unnamed:_4" (99.6% missing)
    -- DROP "unnamed:_5" (99.6% missing)
    -- DROP "unnamed:_6" (99.6% missing)
    populaton AS col_5,
    "unnamed:_8" AS col_6,
    "sectoral_pin_(number)" AS sectoral_pin_number,  -- KEEP with clean name
    "unnamed:_10" AS col_7,
    "unnamed:_11" AS col_8,
    "unnamed:_12" AS col_9,
    "unnamed:_13" AS col_10,
    "unnamed:_14" AS col_11,
    "unnamed:_15" AS col_12,
    "unnamed:_16" AS col_13,
    "unnamed:_17" AS col_14,
    "unnamed:_18" AS col_15,
    "unnamed:_19" AS col_16
    -- DROP "unnamed:_20" (99.6% missing)
FROM raw_data.jiaf_south_sudan_2026;

-- Add indexes
CREATE INDEX idx_jiaf_clean_col_1 
ON cleaned_data.jiaf_south_sudan_2026_clean(col_1);

CREATE INDEX idx_jiaf_clean_sectoral_pin 
ON cleaned_data.jiaf_south_sudan_2026_clean(sectoral_pin_number);

SELECT 
    'jiaf_south_sudan_2026_clean' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT col_1) AS unique_locations,
    SUM(CASE WHEN sectoral_pin_number IS NULL THEN 1 ELSE 0 END) AS missing_sectoral_pin,
    ROUND(100.0 * SUM(CASE WHEN sectoral_pin_number IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS sectoral_pin_missing_pct
FROM cleaned_data.jiaf_south_sudan_2026_clean;