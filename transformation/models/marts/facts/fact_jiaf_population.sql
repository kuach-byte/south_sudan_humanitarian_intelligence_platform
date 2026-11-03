with jiaf as (
    select
        2025                             as year,
        adm2_pcode,
        population_group,
        affected_population              as affected_population_projection,
        final_pin
    from {{ ref('int_jiaf_2025') }}

    union all

    select
        2026                             as year,
        adm2_pcode,
        population_group,
        affected_population_projection,
        final_pin
    from {{ ref('int_jiaf_2026') }}
)

select
    j.year,
    j.adm2_pcode,
    p.population_id,
    j.affected_population_projection,
    j.final_pin
from jiaf j
left join {{ ref('dim_population') }} p
    on p.population_name = j.population_group
