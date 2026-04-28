/*
  One row per software deployment with DORA lead time attached.

  Lead time is the average PR lead time on the same day for the same team.
  In a real integration this would join on PR ID → deployment ID directly.
*/

with deployments as (
    select * from {{ ref('stg_github_events') }}
    where is_deployment
),

pr_lead_times as (
    select
        team,
        event_day,
        avg(lead_time_days)  as avg_lead_time_days
    from {{ ref('stg_github_events') }}
    where is_pr_merged
    group by 1, 2
),

teams as (
    select * from {{ ref('dim_teams') }}
),

joined as (
    select
        d.event_id                              as deployment_id,
        d.team,
        d.event_day                             as deployment_day,
        d.event_timestamp                       as deployed_at,
        t.domain,
        t.sla_tier,
        coalesce(lt.avg_lead_time_days, 0)      as lead_time_days,

        row_number() over (
            partition by d.team, d.event_day
            order by d.event_timestamp
        )                                       as deployment_seq_in_day

    from deployments d
    left join pr_lead_times lt
        on d.team = lt.team and d.event_day = lt.event_day
    left join teams t
        on d.team = t.team
)

select * from joined
