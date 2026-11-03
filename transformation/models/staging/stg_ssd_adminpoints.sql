select
    admin_level::smallint          as admin_level,
    nullif(btrim(name), '')        as place_name,
    x_coord::double precision      as x_coord,
    y_coord::double precision      as y_coord,
    nullif(btrim(adm3_name), '')   as adm3_name,
    nullif(btrim(adm3_pcode), '')  as adm3_pcode,
    nullif(btrim(adm2_name), '')   as adm2_name,
    nullif(btrim(adm2_pcode), '')  as adm2_pcode,
    nullif(btrim(adm1_name), '')   as adm1_name,
    nullif(btrim(adm1_pcode), '')  as adm1_pcode,
    nullif(btrim(adm0_name), '')   as adm0_name,
    nullif(btrim(adm0_pcode), '')  as adm0_pcode,
    valid_on::date                 as valid_on,
    nullif(btrim(version), '')     as version,
    nullif(btrim(lang), '')        as lang,
    geometry
from {{ source('humanitarian_db', 'ssd_adminpoints') }}
