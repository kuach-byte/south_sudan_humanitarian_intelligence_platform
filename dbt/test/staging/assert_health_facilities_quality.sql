with stg as (
    select * from {{ ref('health_facilities') }}
),

-- 1. Check state_code to canonical state one-to-one mapping
state_code_multiple_names as (
    select
        state_code as failing_key,
        'state_code_multiple_names' as issue,
        count(distinct state) as detail_count
    from stg
    group by state_code
    having count(distinct state) > 1
),

-- 2. Check county_code to canonical county one-to-one mapping
county_code_multiple_names as (
    select
        county_code as failing_key,
        'county_code_multiple_names' as issue,
        count(distinct county) as detail_count
    from stg
    group by county_code
    having count(distinct county) > 1
),

-- 3. Check payam_code to payam one-to-one mapping
payam_code_multiple_names as (
    select
        payam_code as failing_key,
        'payam_code_multiple_names' as issue,
        count(distinct payam) as detail_count
    from stg
    group by payam_code
    having count(distinct payam) > 1
),

-- 4. Check payam belongs to correct county (hierarchy consistency)
payam_multiple_counties as (
    select
        payam_code as failing_key,
        'payam_in_multiple_counties' as issue,
        count(distinct county_code) as detail_count
    from stg
    where payam_code is not null
    group by payam_code
    having count(distinct county_code) > 1
),

-- 5. Check county belongs to correct state
county_multiple_states as (
    select
        county_code as failing_key,
        'county_in_multiple_states' as issue,
        count(distinct state_code) as detail_count
    from stg
    where county_code is not null
    group by county_code
    having count(distinct state_code) > 1
),

-- 6. Check duplicate facilities (same name + payam_code + coordinates)
duplicate_facility_same_location as (
    select
        facility_name as failing_key,
        'duplicate_facility_same_location' as issue,
        count(distinct facility_hash) as detail_count
    from stg
    where latitude is not null and longitude is not null
    group by facility_name, payam_code, latitude, longitude
    having count(distinct facility_hash) > 1
),

-- 7. Check duplicate facilities with same name but different locations
duplicate_facility_different_location as (
    select
        facility_name as failing_key,
        'duplicate_facility_different_location' as issue,
        count(distinct concat(payam_code, '|', latitude, '|', longitude)) as detail_count
    from stg
    group by facility_name
    having count(distinct concat(payam_code, '|', latitude, '|', longitude)) > 1
),

-- 8. Check suspicious coordinates (outside South Sudan bounds)
suspicious_coordinates as (
    select
        facility_name as failing_key,
        'suspicious_coordinates' as issue,
        count(*) as detail_count
    from stg
    where coordinate_quality = 'SUSPICIOUS'
),

-- 9. Check low confidence locations (missing too much data)
low_confidence_locations as (
    select
        facility_name as failing_key,
        'low_location_confidence' as issue,
        count(*) as detail_count
    from stg
    where location_confidence = 'LOW'
),

-- 10. Check payam code present but payam name missing
payam_code_without_name as (
    select
        payam_code as failing_key,
        'payam_code_without_name' as issue,
        count(*) as detail_count
    from stg
    where payam_code is not null and payam is null
),

-- 11. Check payam name present but payam code missing
payam_name_without_code as (
    select
        payam as failing_key,
        'payam_name_without_code' as issue,
        count(*) as detail_count
    from stg
    where payam is not null and payam_code is null
),

-- 12. Check state_raw vs state mismatch (identifies mapping issues)
state_raw_mismatch as (
    select
        state_code as failing_key,
        'state_raw_mismatch' as issue,
        count(distinct state_raw) as detail_count
    from stg
    where has_valid_state_code = true
    group by state_code
    having count(distinct state_raw) > 1
),

-- 13. Check county_raw vs county mismatch (identifies mapping issues)
county_raw_mismatch as (
    select
        county_code as failing_key,
        'county_raw_mismatch' as issue,
        count(distinct county_raw) as detail_count
    from stg
    where has_valid_county_code = true
    group by county_code
    having count(distinct county_raw) > 1
),

-- 14. Check invalid state codes (except SS00 special case)
invalid_state_codes as (
    select
        state_code as failing_key,
        'invalid_state_code' as issue,
        count(*) as detail_count
    from stg
    where has_valid_state_code = false
      and state_code != 'SS00'
    group by state_code
),

-- 15. Check invalid county codes
invalid_county_codes as (
    select
        county_code as failing_key,
        'invalid_county_code' as issue,
        count(*) as detail_count
    from stg
    where has_valid_county_code = false
    group by county_code
)

select * from state_code_multiple_names
union all
select * from county_code_multiple_names
union all
select * from payam_code_multiple_names
union all
select * from payam_multiple_counties
union all
select * from county_multiple_states
union all
select * from duplicate_facility_same_location
union all
select * from duplicate_facility_different_location
union all
select * from suspicious_coordinates
union all
select * from low_confidence_locations
union all
select * from payam_code_without_name
union all
select * from payam_name_without_code
union all
select * from state_raw_mismatch
union all
select * from county_raw_mismatch
union all
select * from invalid_state_codes
union all
select * from invalid_county_codes