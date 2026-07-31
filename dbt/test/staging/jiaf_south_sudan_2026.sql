-- ============================================================================
-- Singular test: GIS administrative hierarchy consistency for
-- jiaf_south_sudan_2026.
--
-- Why this exists as a singular test rather than a generic one:
-- `relationships` tests (in the yml) already confirm each p-code EXISTS
-- somewhere in the GIS lookup. They cannot confirm the code is paired
-- with the RIGHT name, or that admin_2's code actually rolls up under
-- admin_1's code -- both require joining and comparing two columns at
-- once, which needs custom SQL.
--
-- Per source-of-truth policy: this test only FLAGS mismatches. It never
-- repairs or overwrites model data -- GIS lookup tables remain the
-- authority, and any row returned here needs human/GIS-team review.
--
-- A non-empty result set fails the test.
-- ============================================================================

with admin1_name_mismatch as (

    -- adm1_pcode <-> adm1_name: code exists in the lookup, but the name
    -- attached to it in this model doesn't match the lookup's name for
    -- that code.
    select
        'admin_1_pcode_name_mismatch' as check_name,
        m.admin_1_p_code              as p_code,
        m.admin_1                     as model_value,
        sl.state_name                 as lookup_value
    from {{ ref('jiaf_south_sudan_2026') }} m
    inner join {{ ref('int_state_lookup') }} sl
        on m.admin_1_p_code = sl.state_code
    where lower(trim(m.admin_1)) is distinct from lower(trim(sl.state_name))

),

admin2_name_mismatch as (

    -- adm2_pcode <-> adm2_name: same check, one level down.
    select
        'admin_2_pcode_name_mismatch' as check_name,
        m.admin_2_p_code              as p_code,
        m.admin_2                     as model_value,
        cl.county_name                as lookup_value
    from {{ ref('jiaf_south_sudan_2026') }} m
    inner join {{ ref('int_county_lookup') }} cl
        on m.admin_2_p_code = cl.county_code
    where lower(trim(m.admin_2)) is distinct from lower(trim(cl.county_name))

),

admin2_not_child_of_admin1 as (

    -- adm2 belongs to adm1: the county's parent state (per the GIS
    -- lookup) must equal the state code recorded on this row -- a
    -- mismatch means the county is filed under the wrong state.
    select
        'admin_2_not_child_of_admin_1' as check_name,
        m.admin_2_p_code               as p_code,
        m.admin_1_p_code               as model_value,
        cl.state_code                  as lookup_value
    from {{ ref('jiaf_south_sudan_2026') }} m
    inner join {{ ref('int_county_lookup') }} cl
        on m.admin_2_p_code = cl.county_code
    where m.admin_1_p_code is distinct from cl.state_code

)

select * from admin1_name_mismatch
union all
select * from admin2_name_mismatch
union all
select * from admin2_not_child_of_admin1