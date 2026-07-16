-- models/intermediate/gis/int_county_lookup.sql
-- Purpose: Lean, authoritative county reference table with state context
-- Usage: Join to any dataset using county_code to get canonical county_name and state_name
-- This is the SINGLE SOURCE OF TRUTH for county information

{{ config(
    schema='intermediate',
    materialized='table',
    tags=['gis', 'reference', 'lookup'],
    description='Authoritative county lookup derived from GIS boundaries'
) }}

with source as (
    select
        county_code,
        county_name,          -- Canonical name from adm2_ref_name
        county_name_alt,      -- Alternative name for QA
        state_code,
        state_name,
        country_code,
        country_name,
        center_lat,
        center_lon,
        area_sqkm,
        geometry,
        version,
        valid_on,
        valid_to,
        _staged_at
    from {{ ref('stg_county_boundary') }}
),

-- Keep it lean - only what's needed for joins
final as (
    select
        -- Primary identifier
        county_code,
        
        -- Canonical names
        county_name,
        state_code,
        state_name,
        country_code,
        country_name,
        
        -- Keep geometry for potential spatial operations
        geometry,
        
        -- Keep centroids for mapping/visualization
        center_lat,
        center_lon,
        
        -- Metadata for traceability
        version,
        valid_on,
        valid_to,
        _staged_at
    from source
)

select *
from final
order by state_name, county_name