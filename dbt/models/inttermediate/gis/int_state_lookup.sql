-- models/intermediate/gis/int_state_lookup.sql
-- Purpose: Lean, authoritative state reference table
-- Usage: Join to any dataset using state_code to get canonical state_name
-- This is the SINGLE SOURCE OF TRUTH for state information

{{ config(
    schema='intermediate',
    materialized='table',
    tags=['gis', 'reference', 'lookup'],
    description='Authoritative state lookup derived from GIS boundaries'
) }}

with source as (
    select
        state_code,
        state_name,           -- Canonical name from adm1_ref_name
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
    from {{ ref('stg_state_boundary') }}
),

-- Keep it lean - only what's needed for joins
final as (
    select
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
order by state_name