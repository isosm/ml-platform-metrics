/*
  DORA metrics aggregated per team per day.

  deployment_frequency  — deployments that day
  avg_lead_time_days    — mean PR open-to-merge duration
  ci_failure_rate       — failed CI runs / total CI runs
  incidents_opened      — new incidents (MTTR calculated at fact layer)

  7-day rolling windows pre-computed here for dashboard performance.
*/

with github as (
    select * from {{ ref('stg_github_events') }}
),

daily_agg as (
    select
        event_day                              as date_day,
        team,

        countif(is_deployment)                 as deployment_count,

        avg(case when is_pr_merged then lead_time_days end)   as avg_lead_time_days,
        approx_quantiles(
            case when is_pr_merged then lead_time_days end, 100
        )[offset(50)]                          as p50_lead_time_days,
        approx_quantiles(
            case when is_pr_merged then lead_time_days end, 100
        )[offset(95)]                          as p95_lead_time_days,

        countif(is_ci_run)                     as ci_run_count,
        countif(is_ci_failure)                 as ci_failure_count,
        safe_divide(
            countif(is_ci_failure),
            nullif(countif(is_ci_run), 0)
        )                                      as ci_failure_rate,

        countif(is_incident_opened)            as incidents_opened,
        countif(is_incident_closed)            as incidents_closed

    from github
    group by 1, 2
),

with_rolling as (
    select
        *,
        sum(deployment_count) over (
            partition by team
            order by date_day
            rows between 6 preceding and current row
        )                                      as deployments_last_7d,

        avg(ci_failure_rate) over (
            partition by team
            order by date_day
            rows between 6 preceding and current row
        )                                      as ci_failure_rate_7d_avg

    from daily_agg
)

select * from with_rolling
