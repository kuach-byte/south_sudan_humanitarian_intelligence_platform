with categories as (
    select population_group from {{ ref('int_jiaf_2025') }}
    union
    select population_group from {{ ref('int_jiaf_2026') }}
)

select
    row_number() over (order by population_group collate "C")::integer as population_id,
    population_group                                                    as population_name
from categories
where nullif(btrim(population_group), '') is not null