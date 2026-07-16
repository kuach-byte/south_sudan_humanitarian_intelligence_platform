-- models/staging/gis/stg_county_boundary.sql
-- Staging model for county boundary GIS data
-- Purpose: Lightweight cleaning and standardization only
-- No business logic or joins

{{ config(
    schema='staging',
    materialized='view',
    tags=['gis', 'staging', 'reference']
) }}

with source as (
    select *
    from {{ source('gis', 'county_boundary') }}
),

renamed as (
    select
        -- Primary identifier - county code
        trim(adm2_pcode) as county_code,
        
        -- County names (prefer ref_name as it's the most reliable)
        trim(adm2_ref_name) as county_name,
        trim(adm2_name) as county_name_alt,
        
        -- Parent state context
        trim(adm1_pcode) as state_code,
        trim(adm1_name) as state_name,
        
        -- Country context
        trim(adm0_pcode) as country_code,
        trim(adm0_name) as country_name,
        
        -- Geometry and coordinates
        geometry,
        center_lat,
        center_lon,
        area_sqkm,
        
        -- Metadata
        trim(version) as version,
        trim(lang) as language,
        valid_on,
        valid_to,
        
        -- Load timestamp for traceability
        current_timestamp as _staged_at

    from source
)

select *
from renamed

-- Filter out invalid records
where county_code is not null
  and county_code != ''
  and geometry is not null
  and center_lat is not null
  and center_lon is not null