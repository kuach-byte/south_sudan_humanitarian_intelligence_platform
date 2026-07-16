{{
    config(
        materialized='view',
        schema='staging',
        alias='health_facilities',
        tags=['staging', 'health']
    )
}}

with source as (
    select
        state,
        state_code,
        county,
        county_code,
        payam,
        payam_code,
        facility_name,
        latitude,
        longitude,
        missing_coordinates
    from {{ source('cleaned_data', 'health_facilities') }}
),

-- Join with authoritative lookup tables
with_lookups as (
    select
        s.*,
        
        -- State lookup (LEFT JOIN to preserve all records)
        sl.state_name as canonical_state,
        sl.country_code,
        sl.country_name,
        
        -- County lookup (LEFT JOIN to preserve all records)
        cl.county_name as canonical_county,
        cl.state_name as county_state_name,
        
        -- QA flags for admin code validity
        case 
            when s.state_code is not null and sl.state_code is not null then true
            when s.state_code = 'SS00' then true  -- Special case: Abyei
            else false
        end as has_valid_state_code,
        
        case 
            when s.county_code is not null and cl.county_code is not null then true
            else false
        end as has_valid_county_code

    from source s
    left join {{ ref('int_state_lookup') }} sl
        on s.state_code = sl.state_code
    left join {{ ref('int_county_lookup') }} cl
        on s.county_code = cl.county_code
),

cleaned as (
    select
        -- generate a stable facility key for entity resolution
        md5(
            lower(trim(facility_name)) || 
            coalesce(trim(payam_code), 'unknown') || 
            coalesce(trim(county_code), 'unknown')
        ) as facility_hash,

        -- facility name standardization
        nullif(
            regexp_replace(
                trim(both from regexp_replace(facility_name, '[\u200B\u200C\u200D\uFEFF\u00A0]', ' ', 'g')),
                '\s+', ' ', 'g'
            ), ''
        ) as facility_name,

        -- Raw admin names (preserved for QA)
        lower(trim(state)) as state_raw,
        lower(trim(county)) as county_raw,
        lower(trim(payam)) as payam_raw,

        -- Canonical admin names from lookups
        -- Use canonical if available, fallback to raw (with lowercase)
        coalesce(
            lower(canonical_state),
            lower(trim(state))
        ) as state,
        
        coalesce(
            lower(canonical_county),
            lower(trim(county))
        ) as county,
        
        lower(trim(payam)) as payam,

        -- Admin codes (keep original)
        trim(state_code) as state_code,
        trim(county_code) as county_code,
        trim(payam_code) as payam_code,

        -- QA flags
        has_valid_state_code,
        has_valid_county_code,

        -- coordinates
        latitude,
        longitude,

        -- flag for coordinate validity
        case
            when latitude is null or longitude is null then 'MISSING'
            when latitude between 3 and 13 and longitude between 24 and 36 then 'VALID'
            else 'SUSPICIOUS'
        end as coordinate_quality,

        -- facility completeness score (max 100)
        (
            case when latitude is not null and longitude is not null then 40 else 0 end +
            case when payam_code is not null then 30 else 0 end +
            case when county_code is not null then 20 else 0 end +
            case when state_code is not null then 10 else 0 end
        ) as completeness_score

    from with_lookups
),

final as (
    select
        facility_hash,
        facility_name,
        
        -- Admin hierarchy (canonical names)
        state,
        state_code,
        county,
        county_code,
        payam,
        payam_code,
        
        -- Raw values for QA
        state_raw,
        county_raw,
        payam_raw,
        
        -- QA flags
        has_valid_state_code,
        has_valid_county_code,
        
        -- Location data
        latitude,
        longitude,
        coordinate_quality,
        completeness_score,
        
        -- location confidence based on completeness and coordinate quality
        case
            when completeness_score >= 90 and coordinate_quality = 'VALID' then 'HIGH'
            when completeness_score >= 60 then 'MEDIUM'
            else 'LOW'
        end as location_confidence

    from cleaned
)

select * from final