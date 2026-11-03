{% set years = [
    {'year': '2025', 'source': 'jiaf_south_sudan_2025', 'model': 'int_jiaf_2025', 'adm2_only': false,
     'cols': [('Populaton', 'affected_population'), ('Sectoral PiN (Number)', 'pin_cccm'),
              ('unnamed_8', 'pin_education'), ('unnamed_9', 'pin_nutrition'), ('unnamed_10', 'pin_food_security'),
              ('unnamed_11', 'pin_health'), ('unnamed_12', 'pin_overarching_protection'),
              ('unnamed_13', 'pin_shelter'), ('unnamed_14', 'pin_wash'), ('unnamed_21', 'final_pin')]},
    {'year': '2026', 'source': 'jiaf_south_sudan_2026', 'model': 'int_jiaf_2026', 'adm2_only': true,
     'cols': [('Populaton', 'affected_population_projection'), ('Sectoral PiN (Number)', 'pin_cccm'),
              ('unnamed_11', 'pin_education'), ('unnamed_12', 'pin_nutrition'), ('unnamed_13', 'pin_food_security'),
              ('unnamed_14', 'pin_health'), ('unnamed_15', 'pin_overarching_protection'),
              ('unnamed_16', 'pin_shelter'), ('unnamed_17', 'pin_wash'), ('unnamed_18', 'severity'),
              ('unnamed_20', 'final_pin')]}
] %}

select null::text as reporting_year, null::text as column_name, null::text as issue, null::text as value
where false
{% for y in years %}{% for raw, alias in y.cols %}
union all
select '{{ y.year }}', '{{ alias }}', 'invalid_numeric', "{{ raw }}"::text
from {{ source('humanitarian_db', y.source) }}
where "Location" is distinct from 'Admin 1'
    {% if y.adm2_only %}and nullif(btrim("unnamed_4"), '') is not null{% endif %}
    and nullif(btrim("{{ raw }}"), '') is not null
    and {{ safe_numeric(raw) }} is null
union all
select '{{ y.year }}', '{{ alias }}', 'negative_value', {{ alias }}::text
from {{ ref(y.model) }}
where {{ alias }} < 0
{% endfor %}{% endfor %}