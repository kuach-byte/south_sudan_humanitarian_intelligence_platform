{% set rows = [] %}
{% if execute %}
  {% for node in graph.nodes.values()
       if node.resource_type == 'model' and node.name.startswith('stg_') %}
    {% set tbl = node.name[4:] %}
    {% for col in node.columns.values() %}
      {% set src = col.meta.get('source_column', col.name) %}
      {% do rows.append("('" ~ tbl ~ "','" ~ src ~ "','" ~ ('renamed' if src != col.name else 'kept') ~ "')") %}
    {% endfor %}
    {% for src, why in node.meta.get('excluded_columns', {}).items() %}
      {% do rows.append("('" ~ tbl ~ "','" ~ src ~ "','excluded: " ~ why | replace("'", "''") ~ "')") %}
    {% endfor %}
  {% endfor %}
{% endif %}

with eligible as (
    select distinct cp.table_name, cp.column_name, cp.inferred_type
    from data_profiling.column_profile cp
    where not exists (
        select 1 from data_profiling.profile_issue pi
        where pi.column_profile_id = cp.column_profile_id
          and pi.severity in ('critical', 'error')
    )
),
accounted (table_name, column_name, status) as (
    values {{ rows | join(', ') if rows else "(null, null, null)" }}
)
select e.table_name, e.column_name, e.inferred_type,
       coalesce(a.status, 'OMITTED (unaccounted)') as status
from eligible e
left join accounted a using (table_name, column_name)
order by (a.status is null) desc, e.table_name, e.column_name
