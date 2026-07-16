select c.county_code, c.state_code
from {{ ref('stg_county_boundary') }} c
left join {{ ref('stg_state_boundary') }} s on c.state_code = s.state_code
where s.state_code is null