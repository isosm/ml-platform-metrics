/*
  Retrain response time per team per day.

  Pairs each drift_detected event with the next retrain_triggered for the
  same team and model. Same pairing pattern as fct_incidents (open → close).

  response_tier thresholds:
    fast   < 24h  — team reacted same day
    medium < 72h  — acceptable response
    slow   > 72h  — warrants investigation
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
        drift_day                                              as date_day,
        team,
        model_name,
        drift_at,
        retrain_at,
        timestamp_diff(retrain_at, drift_at, hour)            as response_hours,
        retrain_at is null                                     as no_response_yet,

        case
            when retrain_at is null                                 then 'no_response'
            when timestamp_diff(retrain_at, drift_at, hour) <= 24  then 'fast'
            when timestamp_diff(retrain_at, drift_at, hour) <= 72  then 'medium'
            else                                                         'slow'
        end as response_tier

    from paired
)

select * from final
