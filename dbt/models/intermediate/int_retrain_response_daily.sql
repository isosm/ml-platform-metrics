/*
  Retrain Response Time per team per dag.

  Parar ihop drift_detected med nästa retrain_triggered för samma team/modell.
  Samma pairing-logik som fct_incidents (öppnad → stängd).

  Metrik: hur snabbt reagerar ett team när en modell driftar?
  Target: < 24 timmar (analogt med MTTR < 1h för software incidents).
*/

with drift as (
    select
        team,
        model_name,
        event_timestamp as drift_at,
        event_day       as drift_day
    from {{ ref('stg_ml_events') }}
    where is_drift_detected
),

retrain as (
    select
        team,
        model_name,
        event_timestamp as retrain_at
    from {{ ref('stg_ml_events') }}
    where is_retrain_triggered
),

-- Para ihop varje drift med nästa retrain för samma team + modell
paired as (
    select
        d.team,
        d.model_name,
        d.drift_day,
        d.drift_at,
        min(r.retrain_at) as retrain_at
    from drift d
    left join retrain r
        on  d.team       = r.team
        and d.model_name = r.model_name
        and r.retrain_at > d.drift_at
    group by 1, 2, 3, 4
),

final as (
    select
        drift_day                                                as date_day,
        team,
        model_name,
        drift_at,
        retrain_at,
        timestamp_diff(retrain_at, drift_at, hour)              as response_hours,
        retrain_at is null                                       as no_response_yet,

        -- Klassificera svarstid — analogt med DORA MTTR-tiers
        case
            when retrain_at is null                                  then 'no_response'
            when timestamp_diff(retrain_at, drift_at, hour) <= 24   then 'fast'
            when timestamp_diff(retrain_at, drift_at, hour) <= 72   then 'medium'
            else                                                          'slow'
        end as response_tier
    from paired
)

select * from final
