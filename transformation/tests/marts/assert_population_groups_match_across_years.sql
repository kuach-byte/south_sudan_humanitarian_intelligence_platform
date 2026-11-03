with y2025 as (
    select distinct population_group
    from {{ ref('int_jiaf_2025') }}
    where nullif(btrim(population_group), '') is not null
),

y2026 as (
    select distinct population_group
    from {{ ref('int_jiaf_2026') }}
    where nullif(btrim(population_group), '') is not null
)

select population_group, 'missing_in_int_jiaf_2026' as issue
from (select population_group from y2025 except select population_group from y2026) m

union all

select population_group, 'missing_in_int_jiaf_2025' as issue
from (select population_group from y2026 except select population_group from y2025) m