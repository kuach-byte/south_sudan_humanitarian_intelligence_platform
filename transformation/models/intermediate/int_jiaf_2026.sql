select
    upper(adm2_pcode)                                      as adm2_pcode,
    affected_population_projection,
    {{ standardize_population_group('population_group') }} as population_group,
    pin_cccm,
    pin_education,
    pin_nutrition,
    pin_food_security,
    pin_health,
    pin_overarching_protection,
    pin_shelter,
    pin_wash,
    severity,
    final_pin
from {{ ref('stg_jiaf_south_sudan_2026') }}