/*
  One row per incident with MTTR calculated.

  Pairs incident_opened with the next incident_closed for the same team.
  Uses LEAD() to find the close timestamp — same pattern as pairing
  session start/end events in clickstream analytics.

  MTTR (Mean Time to Recover) = closed_at - opened_at in hours.
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

-- Match each opening to the next closing for the same team
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
