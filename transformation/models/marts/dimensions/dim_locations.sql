with admin2 as (
    select
        adm2_name,
        upper(adm2_pcode) as adm2_pcode,
        adm1_name,
        upper(adm1_pcode) as adm1_pcode,
        adm0_name,
        upper(adm0_pcode) as adm0_pcode,
        area_sqkm,
        center_lat,
        center_lon,
        geometry
    from {{ ref('stg_ssd_admin2') }}
),

points as (
    select
        upper(adm2_pcode) as adm2_pcode,
        x_coord,
        y_coord
    from {{ ref('stg_ssd_adminpoints') }}
    where admin_level = 2
)

select
    a.adm2_name,
    a.adm2_pcode,
    a.adm1_name,
    a.adm1_pcode,
    a.adm0_name,
    a.adm0_pcode,
    a.area_sqkm,
    a.center_lat,
    a.center_lon,
    p.x_coord,
    p.y_coord,
    a.geometry
from admin2 a
left join points p on p.adm2_pcode = a.adm2_pcode