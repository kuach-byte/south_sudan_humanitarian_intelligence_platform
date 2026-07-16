{{
    config(
        materialized='view',
        schema='staging',
        alias='education_facilities',
        tags=['staging', 'education']
    )
}}

with source as (

    select
        id,
        name,
        amenity,
        adm1_pcode,
        adm1_name,
        adm2_pcode,
        adm2_name,
        adm3_pcode,
        adm3_name,
        name_latin,
        geometry
    from {{ source('cleaned_data', 'education_facilities') }}

),

cleaned as (

    select
        trim(id) as facility_id,

        -- preserve original casing (proper nouns / acronyms); strip invisible
        -- unicode chars (zero-width space, BOM, NBSP) before collapsing whitespace
        nullif(
            regexp_replace(
                trim(both from regexp_replace(name, '[\u200B\u200C\u200D\uFEFF\u00A0]', ' ', 'g')),
                '\s+', ' ', 'g'
            ), ''
        ) as facility_name,
        nullif(
            regexp_replace(
                trim(both from regexp_replace(name_latin, '[\u200B\u200C\u200D\uFEFF\u00A0]', ' ', 'g')),
                '\s+', ' ', 'g'
            ), ''
        ) as facility_name_latin,

        -- closed-domain categorical: lowercase + trim, keep true NULLs as NULL (no guessing)
        case
            when amenity is null or trim(amenity) = '' then null
            else lower(trim(amenity))
        end as amenity_type,

        trim(adm1_pcode) as adm1_pcode,
        regexp_replace(trim(both from adm1_name), '\s+', ' ', 'g') as adm1_name,
        trim(adm2_pcode) as adm2_pcode,
        regexp_replace(trim(both from adm2_name), '\s+', ' ', 'g') as adm2_name,
        trim(adm3_pcode) as adm3_pcode,
        regexp_replace(trim(both from adm3_name), '\s+', ' ', 'g') as adm3_name,

        geometry::geometry as geometry_native

    from source

),

geo_validated as (

    select
        facility_id,
        facility_name,
        facility_name_latin,
        amenity_type,
        adm1_pcode,
        adm1_name,
        adm2_pcode,
        adm2_name,
        adm3_pcode,
        adm3_name,
        geometry_native,
        ST_SRID(geometry_native)         as geometry_srid,
        ST_GeometryType(geometry_native) as geometry_type,
        ST_IsValid(geometry_native)      as is_geometry_valid_raw
    from cleaned

),

final as (

    select
        facility_id,
        facility_name,
        facility_name_latin,
        amenity_type,

        adm1_pcode,
        adm1_name,
        adm2_pcode,
        adm2_name,
        adm3_pcode,
        adm3_name,

        -- repair invalid geometries where possible; keep the flag so it stays auditable
        case
            when is_geometry_valid_raw then geometry_native
            else ST_MakeValid(geometry_native)
        end as geometry,

        geometry_srid,
        geometry_type,
        is_geometry_valid_raw as was_geometry_valid_before_repair

    from geo_validated

)

select * from final