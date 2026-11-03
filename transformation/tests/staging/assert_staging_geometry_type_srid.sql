{% set expected = {
    'stg_ssd_admin1': ['POLYGON', 'MULTIPOLYGON'],
    'stg_ssd_admin2': ['POLYGON', 'MULTIPOLYGON'],
    'stg_ssd_admin3': ['POLYGON', 'MULTIPOLYGON'],
    'stg_ssd_adminlines': ['LINESTRING', 'MULTILINESTRING'],
    'stg_ssd_adminpoints': ['POINT']
} %}
{% for m, types in expected.items() %}
select '{{ m }}' as model, geometrytype(geometry) as geom_type, st_srid(geometry) as srid
from {{ ref(m) }}
where geometry is not null
  and (st_srid(geometry) <> 4326
       or geometrytype(geometry) not in ('{{ types | join("', '") }}'))
{% if not loop.last %}union all{% endif %}
{% endfor %}
