-- Validates that health_score stays within [0, 100].
-- Returns rows that violate the constraint — zero rows = test passes.

select
    date_day,
    team,
    health_score
from {{ ref('fct_platform_health_daily') }}
where health_score < 0
   or health_score > 100
