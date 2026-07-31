{{
    config(
        materialized='view',
        schema='staging',
        alias='jiaf_south_sudan_2026_clean',
        tags=['staging', 'jiaf']
    )
}}

-- ============================================================================
-- Source note: a load error caused physical columns to be named col_1..col_16
-- (plus sectoral_pin_number), with the TRUE field names sitting in the first
-- data row instead of the header. The col_N -> real name mapping below is
-- fixed/hardcoded from the source data dictionary, not inferred at runtime,
-- since the physical layout doesn't change between loads.
-- ============================================================================

with source as (

    select
        col_1,
        col_2,
        col_3,
        col_4,
        col_5,
        col_6,
        sectoral_pin_number,
        col_7,
        col_8,
        col_9,
        col_10,
        col_11,
        col_12,
        col_13,
        col_14,
        col_15,
        col_16
    from {{ source('cleaned_data', 'jiaf_south_sudan_2026_clean') }}

),

-- The first row holds the real column names as data values
-- (col_1 = 'Admin 1', etc). It's metadata, not a record, so drop it.
-- Matching on the known header value for col_1 is deterministic and
-- doesn't depend on row order/ctid.
remove_header_row as (

    select *
    from source
    where col_1 is distinct from 'Admin 1'
      and col_2 is not null  -- drops trailing national/summary rollup row (null admin p-codes, aggregate sectoral totals)

),
-- Positional rename to real field names (hardcoded per source spec):
--   col_1  -> Admin 1                              col_11 -> Overarching Protection
--   col_2  -> Admin 1 P-Code                        col_12 -> Shelter
--   col_3  -> Admin 2                                col_13 -> WASH
--   col_4  -> Admin 2 P-Code                         col_14 -> Severity
--   col_5  -> Affected population projection 2026    col_15 -> Preliminary PiN
--   col_6  -> Population Group                        col_16 -> Final PiN
--   sectoral_pin_number -> CCCM
--   col_7  -> Education   col_8 -> Nutrition   col_9 -> Food Security   col_10 -> Health
rename_columns as (

    select
        col_1                as admin_1,
        col_2                as admin_1_p_code,
        col_3                as admin_2,
        col_4                as admin_2_p_code,
        col_5                as affected_population_projection_2026,
        col_6                as population_group,
        sectoral_pin_number  as cccm,
        col_7                as education,
        col_8                as nutrition,
        col_9                as food_security,
        col_10               as health,
        col_11               as overarching_protection,
        col_12               as shelter,
        col_13               as wash,
        col_14               as severity,
        col_15               as preliminary_pin,
        col_16               as final_pin
    from remove_header_row

),

-- Text standardization, applied to every column while everything is still
-- text: known placeholder tokens -> NULL, trim, collapse internal spaces.
-- Administrative/descriptive text gets INITCAP, P-codes get UPPER.
-- Sector/PiN columns are numeric-bound: placeholders cleared and thousands
-- separators stripped here, but they stay TEXT until cast_types casts them
-- safely (so one bad value can't fail the whole model build).
clean_text as (

    select
        nullif(initcap(regexp_replace(
            trim(case when upper(trim(admin_1)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else admin_1 end),
            '\s+', ' ', 'g'
        )), '') as admin_1,

        nullif(upper(regexp_replace(
            trim(case when upper(trim(admin_1_p_code)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else admin_1_p_code end),
            '\s+', ' ', 'g'
        )), '') as admin_1_p_code,

        nullif(initcap(regexp_replace(
            trim(case when upper(trim(admin_2)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else admin_2 end),
            '\s+', ' ', 'g'
        )), '') as admin_2,

        nullif(upper(regexp_replace(
            trim(case when upper(trim(admin_2_p_code)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else admin_2_p_code end),
            '\s+', ' ', 'g'
        )), '') as admin_2_p_code,

        nullif(replace(trim(
            case when upper(trim(affected_population_projection_2026)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else affected_population_projection_2026 end
        ), ',', ''), '') as affected_population_projection_2026,

        nullif(initcap(regexp_replace(
            trim(case when upper(trim(population_group)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else population_group end),
            '\s+', ' ', 'g'
        )), '') as population_group,

        nullif(replace(trim(case when upper(trim(cccm)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else cccm end), ',', ''), '') as cccm,
        nullif(replace(trim(case when upper(trim(education)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else education end), ',', ''), '') as education,
        nullif(replace(trim(case when upper(trim(nutrition)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else nutrition end), ',', ''), '') as nutrition,
        nullif(replace(trim(case when upper(trim(food_security)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else food_security end), ',', ''), '') as food_security,
        nullif(replace(trim(case when upper(trim(health)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else health end), ',', ''), '') as health,
        nullif(replace(trim(case when upper(trim(overarching_protection)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else overarching_protection end), ',', ''), '') as overarching_protection,
        nullif(replace(trim(case when upper(trim(shelter)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else shelter end), ',', ''), '') as shelter,
        nullif(replace(trim(case when upper(trim(wash)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else wash end), ',', ''), '') as wash,
        nullif(replace(trim(case when upper(trim(severity)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else severity end), ',', ''), '') as severity,
        nullif(replace(trim(case when upper(trim(preliminary_pin)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else preliminary_pin end), ',', ''), '') as preliminary_pin,
        nullif(replace(trim(case when upper(trim(final_pin)) in ('N/A','NA','-','UNKNOWN','NULL','') then '' else final_pin end), ',', ''), '') as final_pin

    from rename_columns

),

-- Safe numeric casting: only cast values that actually look numeric
-- (regex-validated), everything else becomes NULL instead of failing
-- the build. P-codes and admin names stay TEXT (identifiers, not
-- quantities). Severity uses NUMERIC(3,2) since it's a bounded score.
cast_types as (

    select
        admin_1,                -- TEXT: administrative name, not a quantity
        admin_1_p_code,          -- TEXT: identifier, never cast to numeric
        admin_2,                 -- TEXT: administrative name
        admin_2_p_code,          -- TEXT: identifier

        case when affected_population_projection_2026 ~ '^-?[0-9]+(\.[0-9]+)?$'
             then affected_population_projection_2026::numeric end as affected_population_projection_2026,  -- NUMERIC: population count

        population_group,        -- TEXT: categorical label

        case when cccm ~ '^-?[0-9]+(\.[0-9]+)?$' then cccm::numeric end as cccm,                                       -- NUMERIC: sectoral PiN count
        case when education ~ '^-?[0-9]+(\.[0-9]+)?$' then education::numeric end as education,                       -- NUMERIC: sectoral PiN count
        case when nutrition ~ '^-?[0-9]+(\.[0-9]+)?$' then nutrition::numeric end as nutrition,                       -- NUMERIC: sectoral PiN count
        case when food_security ~ '^-?[0-9]+(\.[0-9]+)?$' then food_security::numeric end as food_security,           -- NUMERIC: sectoral PiN count
        case when health ~ '^-?[0-9]+(\.[0-9]+)?$' then health::numeric end as health,                                 -- NUMERIC: sectoral PiN count
        case when overarching_protection ~ '^-?[0-9]+(\.[0-9]+)?$' then overarching_protection::numeric end as overarching_protection, -- NUMERIC: sectoral PiN count
        case when shelter ~ '^-?[0-9]+(\.[0-9]+)?$' then shelter::numeric end as shelter,                             -- NUMERIC: sectoral PiN count
        case when wash ~ '^-?[0-9]+(\.[0-9]+)?$' then wash::numeric end as wash,                                       -- NUMERIC: sectoral PiN count

        case when severity ~ '^-?[0-9]+(\.[0-9]+)?$' then severity::numeric(3,2) end as severity,                    -- NUMERIC(3,2): bounded severity score, decimal-valued

        case when preliminary_pin ~ '^-?[0-9]+(\.[0-9]+)?$' then preliminary_pin::numeric end as preliminary_pin,     -- NUMERIC: people-in-need count
        case when final_pin ~ '^-?[0-9]+(\.[0-9]+)?$' then final_pin::numeric end as final_pin                       -- NUMERIC: people-in-need count

    from clean_text

),

final as (

    select
        admin_1,
        admin_1_p_code,
        admin_2,
        admin_2_p_code,
        affected_population_projection_2026,
        population_group,
        cccm,
        education,
        nutrition,
        food_security,
        health,
        overarching_protection,
        shelter,
        wash,
        severity,
        preliminary_pin,
        final_pin
    from cast_types

)

select * from final