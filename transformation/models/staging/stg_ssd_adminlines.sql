select
    adm_level::smallint            as adm_level,
    valid_on::date                 as valid_on,
    nullif(btrim(version), '')     as version,
    geometry
from {{ source('humanitarian_db', 'ssd_adminlines') }}
