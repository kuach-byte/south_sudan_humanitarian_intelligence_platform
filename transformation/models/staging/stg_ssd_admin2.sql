select
    nullif(btrim(adm2_name), '')      as adm2_name,
    nullif(btrim(adm2_pcode), '')     as adm2_pcode,
    nullif(btrim(adm2_ref_name), '')  as adm2_ref_name,
    nullif(btrim(adm1_name), '')      as adm1_name,
    nullif(btrim(adm1_pcode), '')     as adm1_pcode,
    nullif(btrim(adm0_name), '')      as adm0_name,
    nullif(btrim(adm0_pcode), '')     as adm0_pcode,
    valid_on::date                    as valid_on,
    area_sqkm::numeric                as area_sqkm,
    center_lat::numeric               as center_lat,
    center_lon::numeric               as center_lon,
    nullif(btrim(version), '')        as version,
    nullif(btrim(lang), '')           as lang,
    geometry
from {{ source('humanitarian_db', 'ssd_admin2') }}
