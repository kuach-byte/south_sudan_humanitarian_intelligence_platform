select
    nullif(btrim("Location"), '')   as adm1_name,
    nullif(btrim("unnamed_2"), '')  as adm1_pcode,
    nullif(btrim("unnamed_3"), '')  as adm2_name,
    nullif(btrim("unnamed_4"), '')  as adm2_pcode,
    {{ safe_numeric('Populaton') }}              as affected_population,
    nullif(btrim("unnamed_6"), '')  as population_group,
    {{ safe_numeric('Sectoral PiN (Number)') }}  as pin_cccm,
    {{ safe_numeric('unnamed_8') }}              as pin_education,
    {{ safe_numeric('unnamed_9') }}              as pin_nutrition,
    {{ safe_numeric('unnamed_10') }}             as pin_food_security,
    {{ safe_numeric('unnamed_11') }}             as pin_health,
    {{ safe_numeric('unnamed_12') }}             as pin_overarching_protection,
    {{ safe_numeric('unnamed_13') }}             as pin_shelter,
    {{ safe_numeric('unnamed_14') }}             as pin_wash,
    {{ safe_numeric('Protection AoRs') }}        as pin_cp,
    {{ safe_numeric('unnamed_16') }}             as pin_gbv,
    {{ safe_numeric('unnamed_17') }}             as pin_mine_action,
    {{ safe_numeric('unnamed_18') }}             as pin_hlp,
    {{ safe_numeric('unnamed_19') }}             as pin_gp,
    {{ safe_numeric('unnamed_20') }}             as preliminary_pin,
    {{ safe_numeric('unnamed_21') }}             as final_pin
from {{ source('humanitarian_db', 'jiaf_south_sudan_2025') }}
where "Location" is distinct from 'Admin 1'
