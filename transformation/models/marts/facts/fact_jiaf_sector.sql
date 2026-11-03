with jiaf as (
    select
        2025 as year, adm2_pcode, population_group,
        pin_cccm, pin_education, pin_nutrition, pin_food_security,
        pin_health, pin_overarching_protection, pin_shelter, pin_wash
    from {{ ref('int_jiaf_2025') }}

    union all

    select
        2026 as year, adm2_pcode, population_group,
        pin_cccm, pin_education, pin_nutrition, pin_food_security,
        pin_health, pin_overarching_protection, pin_shelter, pin_wash
    from {{ ref('int_jiaf_2026') }}
),

unpivoted as (
    select
        j.year,
        j.adm2_pcode,
        j.population_group,
        v.sector_name,
        v.sector_pin_value
    from jiaf j
    cross join lateral (
        values
            ('pin_cccm',                  j.pin_cccm),
            ('pin_education',             j.pin_education),
            ('pin_nutrition',             j.pin_nutrition),
            ('pin_food_security',         j.pin_food_security),
            ('pin_health',                j.pin_health),
            ('pin_overarching_protection', j.pin_overarching_protection),
            ('pin_shelter',               j.pin_shelter),
            ('pin_wash',                  j.pin_wash)
    ) as v(sector_name, sector_pin_value)
)

select
    u.year,
    u.adm2_pcode,
    s.sector_id,
    p.population_id,
    u.sector_pin_value
from unpivoted u
left join {{ ref('dim_sectors') }} s
    on s.sector_name = u.sector_name
left join {{ ref('dim_population') }} p
    on p.population_name = u.population_group
