DROP TABLE IF EXISTS cleaned_data.population_estimates_2024_clean;

CREATE TABLE cleaned_data.population_estimates_2024_clean AS
SELECT 
    admin1,
    admin1_pcode,
    admin2,
    admin2_pcode,
    "population_-_2025" AS population_2025,
    "%_male_children__under_5" AS pct_male_under_5,
    "no._of_male_children_under_5" AS male_under_5,
    "%_female_children_under_5" AS pct_female_under_5,
    "no._of_female_children_under_5" AS female_under_5,
    "%_male_children__aged_5_-_17_years" AS pct_male_5_17,
    "no._of_male_children__aged_5_-_17_years" AS male_5_17,
    "%_female_children__aged_5_-_17_years" AS pct_female_5_17,
    "no._of_female__children_aged_5_-_17_years" AS female_5_17,
    "%_male_adults__aged_18_-_60" AS pct_male_18_60,
    "no._of__male__adults_aged_18_-_60" AS male_18_60,
    "%_female_adults__aged_18_-_60" AS pct_female_18_60,
    "no._of_female__adults_aged_18_-_60" AS female_18_60,
    "%_male_adults__aged_over_60" AS pct_male_over_60,
    "no._of_male_adults__aged_over_60" AS male_over_60,
    "%_female_adults__aged_over_60" AS pct_female_over_60,
    "no._female_adults__aged_over_60" AS female_over_60
FROM raw_data.population_estimates_2024;

-- Add indexes
CREATE INDEX idx_pop_clean_admin1_pcode 
ON cleaned_data.population_estimates_2024_clean(admin1_pcode);

CREATE INDEX idx_pop_clean_admin2_pcode 
ON cleaned_data.population_estimates_2024_clean(admin2_pcode);

-- Verify the cleaning
SELECT 
    'population_estimates_2024_clean' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT admin2_pcode) AS unique_admin2,
    COUNT(DISTINCT admin1_pcode) AS unique_admin1,
    ROUND(AVG(population_2025)::numeric, 0) AS avg_population
FROM cleaned_data.population_estimates_2024_clean;