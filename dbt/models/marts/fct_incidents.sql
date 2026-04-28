/*
  One row per incident with MTTR calculated.

  Pairs incident_opened with the next incident_closed for the same team
  using a LEFT JOIN on team + temporal ordering. Same pattern as matching
  session start/end events in clickstream analytics.
*/

with events as (
    select * from {{ ref('stg_github_events') }}
    where is_incident_opened or is_incident_closed
),

opened as (
    select
        event_id         as incident_id,
        team,
        event_day        as opened_day,
        event_timestamp  as opened_at
    from events
    where is_incident_opened
),

closed as (
    select
        team,
        event_timestamp  as closed_at
    from events
    where is_incident_closed
),

matched as (
    select
        o.incident_id,
        o.team,
        o.opened_day,
        o.opened_at,
        min(c.closed_at)  as closed_at
    from opened o
    left join closed c
        on o.team = c.team
        and c.closed_at > o.opened_at
    group by 1, 2, 3, 4
),

final as (
    select
        incident_id,
        team,
        opened_day,
        opened_at,
        closed_at,
        timestamp_diff(closed_at, opened_at, hour) as mttr_hours,
        closed_at is null                           as is_still_open
    from matched
)

select * from final
