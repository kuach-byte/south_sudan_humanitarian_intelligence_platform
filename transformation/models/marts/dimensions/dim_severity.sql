with categories as (
    select distinct severity::text as severity_level
    from {{ ref('int_jiaf_2026') }}
)

select
    row_number() over (order by severity_level collate "C")::integer as severity_id,
    severity_level
from categories
where nullif(btrim(severity_level), '') is not null