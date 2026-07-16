

with stg as (

    select *
    from {{ ref('education_facilities') }}

),

adm1_pcode_multiple_names as (

    select
        adm1_pcode                 as failing_key,
        'adm1_pcode_multiple_names' as issue,
        count(distinct adm1_name)  as detail_count
    from stg
    group by adm1_pcode
    having count(distinct adm1_name) > 1

),

adm2_pcode_multiple_names as (

    select
        adm2_pcode                 as failing_key,
        'adm2_pcode_multiple_names' as issue,
        count(distinct adm2_name)  as detail_count
    from stg
    group by adm2_pcode
    having count(distinct adm2_name) > 1

),

adm3_pcode_multiple_names as (

    select
        adm3_pcode                 as failing_key,
        'adm3_pcode_multiple_names' as issue,
        count(distinct adm3_name)  as detail_count
    from stg
    group by adm3_pcode
    having count(distinct adm3_name) > 1

),

adm3_multiple_parents as (

    select
        adm3_pcode                     as failing_key,
        'adm3_pcode_multiple_adm2_parents' as issue,
        count(distinct adm2_pcode)     as detail_count
    from stg
    group by adm3_pcode
    having count(distinct adm2_pcode) > 1

),

duplicate_facility_same_location as (

    select
        facility_name                       as failing_key,
        'duplicate_facility_same_location'  as issue,
        count(distinct facility_id)         as detail_count
    from stg
    group by facility_name, adm3_pcode, geometry
    having count(distinct facility_id) > 1

),

geometry_still_invalid as (

    select
        facility_id                  as failing_key,
        'geometry_invalid_after_repair' as issue,
        1                             as detail_count
    from stg
    where not ST_IsValid(geometry)

)

select * from adm1_pcode_multiple_names
union all
select * from adm2_pcode_multiple_names
union all
select * from adm3_pcode_multiple_names
union all
select * from adm3_multiple_parents
union all
select * from duplicate_facility_same_location
union all
select * from geometry_still_invalid