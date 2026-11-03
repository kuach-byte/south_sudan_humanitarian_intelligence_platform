select 'stg_ssd_admin1' as model, adm1_pcode as id from {{ ref('stg_ssd_admin1') }}
  where center_lat not between -90 and 90 or center_lon not between -180 and 180
union all
select 'stg_ssd_admin2', adm2_pcode from {{ ref('stg_ssd_admin2') }}
  where center_lat not between -90 and 90 or center_lon not between -180 and 180
union all
select 'stg_ssd_admin3', adm3_pcode from {{ ref('stg_ssd_admin3') }}
  where center_lat not between -90 and 90 or center_lon not between -180 and 180
union all
select 'stg_ssd_adminpoints', place_name from {{ ref('stg_ssd_adminpoints') }}
  where y_coord not between -90 and 90 or x_coord not between -180 and 180
