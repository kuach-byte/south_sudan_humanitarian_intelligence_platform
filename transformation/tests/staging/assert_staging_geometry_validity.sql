{{ config(severity='warn') }}
{% for m in ['stg_ssd_admin1', 'stg_ssd_admin2', 'stg_ssd_admin3', 'stg_ssd_adminlines', 'stg_ssd_adminpoints'] %}
select '{{ m }}' as model,
       geometry is null as is_null,
       st_isempty(geometry) as is_empty,
       not st_isvalid(geometry) as is_invalid
from {{ ref(m) }}
where geometry is null or st_isempty(geometry) or not st_isvalid(geometry)
{% if not loop.last %}union all{% endif %}
{% endfor %}
