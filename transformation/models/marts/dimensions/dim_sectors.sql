select
    row_number() over (order by sector_name collate "C")::integer as sector_id,
    sector_name
from (
    values
        ('pin_cccm'),
        ('pin_education'),
        ('pin_nutrition'),
        ('pin_food_security'),
        ('pin_health'),
        ('pin_overarching_protection'),
        ('pin_shelter'),
        ('pin_wash')
) as s(sector_name)