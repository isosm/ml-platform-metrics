-- Singular test: health_score måste alltid vara mellan 0 och 100.
--
-- Logiken: composite score beräknas av viktade komponenter.
-- Om något i beräkningslogiken är fel kan score hamna utanför [0, 100].
-- Det här testet fångar det — returnerar alla rader som bryter regeln.
-- Noll rader = testet passerar.

SELECT
    date_day,
    team,
    health_score
FROM {{ ref('fct_platform_health_daily') }}
WHERE health_score < 0
   OR health_score > 100
