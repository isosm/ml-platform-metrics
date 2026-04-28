/*
  Platform health score — one row per team per day.

  Combines all 8 DORA-for-ML metrics into a composite score (0–100).
  Think of it as a credit score for your ML platform.

  Score weights:
    25 pts  Deployment Frequency  — are teams shipping regularly?
    20 pts  CI Failure Rate       — is the quality gate holding?
    20 pts  Model Drift Rate      — are models staying healthy?
    20 pts  Lead Time             — is the feedback loop tight?
    15 pts  MTTR                  — are incidents resolved quickly?

  Thresholds calibrated to DORA Elite benchmarks.
  Below 70 = alert. Above 90 = healthy.

  Schema is contract-enforced. Any column change requires updating the
  contract in _marts_models.yml to prevent silent downstream breakage.
*/

with dora as (
    select * from {{ ref('int_dora_daily') }}
),

ml as (
    select
        date_day,
        team,
        avg(avg_precision_at_10)         as avg_precision_at_10,
        avg(drift_failure_rate)          as avg_drift_failure_rate,
        avg(drift_failure_rate_7d_avg)   as drift_failure_rate_7d_avg,
        sum(training_run_count)          as total_training_runs,
        sum(model_deployment_count)      as total_model_deployments,
        sum(drift_detected_count)        as total_drift_events
    from {{ ref('int_ml_delivery_daily') }}
    group by 1, 2
),

incidents as (
    select
        opened_day  as date_day,
        team,
        count(*)                             as incident_count,
        avg(mttr_hours)                      as avg_mttr_hours,
        countif(mttr_hours > 24)             as high_mttr_count
    from {{ ref('fct_incidents') }}
    where not is_still_open
    group by 1, 2
),

retrain_response as (
    select
        date_day,
        team,
        round(avg(response_hours), 1)        as avg_retrain_response_hours,
        countif(response_tier = 'fast')      as fast_retrain_response_count,
        countif(response_tier = 'slow')      as slow_retrain_response_count
    from {{ ref('int_retrain_response_daily') }}
    where not no_response_yet
    group by 1, 2
),

joined as (
    select
        d.date_day,
        d.team,

        -- DORA: Software Delivery
        d.deployment_count,
        d.deployments_last_7d,
        d.avg_lead_time_days,
        d.p50_lead_time_days,
        d.p95_lead_time_days,
        d.ci_run_count,
        d.ci_failure_count,
        d.ci_failure_rate,
        d.ci_failure_rate_7d_avg,
        d.incidents_opened,
        d.incidents_closed,

        -- ML Delivery
        coalesce(ml.avg_precision_at_10, 0)        as avg_precision_at_10,
        coalesce(ml.avg_drift_failure_rate, 0)     as drift_failure_rate,
        coalesce(ml.drift_failure_rate_7d_avg, 0)  as drift_failure_rate_7d_avg,
        coalesce(ml.total_training_runs, 0)        as training_run_count,
        coalesce(ml.total_model_deployments, 0)    as model_deployment_count,
        coalesce(ml.total_drift_events, 0)         as drift_detected_count,

        -- Incidents
        coalesce(inc.incident_count, 0)            as incident_count,
        coalesce(inc.avg_mttr_hours, 0)            as avg_mttr_hours,
        coalesce(inc.high_mttr_count, 0)           as high_mttr_count,

        -- Retrain Response
        coalesce(rr.avg_retrain_response_hours, 0) as avg_retrain_response_hours,
        coalesce(rr.fast_retrain_response_count, 0) as fast_retrain_response_count,
        coalesce(rr.slow_retrain_response_count, 0) as slow_retrain_response_count

    from dora d
    left join ml
        on d.date_day = ml.date_day and d.team = ml.team
    left join incidents inc
        on d.date_day = inc.date_day and d.team = inc.team
    left join retrain_response rr
        on d.date_day = rr.date_day and d.team = rr.team
),

scored as (
    select
        *,

        -- Deployment Frequency score (25 pts)
        -- Elite: >= 1/day, High: >= 3/week, Medium: >= 1/week
        case
            when deployments_last_7d >= 7  then 100.0
            when deployments_last_7d >= 3  then 75.0
            when deployments_last_7d >= 1  then 50.0
            else 25.0
        end as score_deploy_freq,

        -- CI Failure Rate score (20 pts): elite < 15%
        greatest(0, least(100, (1 - ci_failure_rate_7d_avg) * 100))
            as score_ci_quality,

        -- Drift Failure Rate score (20 pts): target < 5%
        greatest(0, least(100, (1 - drift_failure_rate_7d_avg) * 100))
            as score_drift,

        -- Lead Time score (20 pts): elite < 1 day
        case
            when avg_lead_time_days <= 1  then 100.0
            when avg_lead_time_days <= 3  then 75.0
            when avg_lead_time_days <= 7  then 50.0
            else 25.0
        end as score_lead_time,

        -- MTTR score (15 pts): elite < 1h
        case
            when avg_mttr_hours = 0    then 100.0
            when avg_mttr_hours <= 1   then 100.0
            when avg_mttr_hours <= 4   then 75.0
            when avg_mttr_hours <= 24  then 50.0
            else 25.0
        end as score_mttr

    from joined
),

final as (
    select
        date_day,
        team,

        deployment_count,
        deployments_last_7d,
        avg_lead_time_days,
        p50_lead_time_days,
        p95_lead_time_days,
        ci_run_count,
        ci_failure_count,
        ci_failure_rate,
        ci_failure_rate_7d_avg,
        incidents_opened,
        incidents_closed,
        incident_count,
        avg_mttr_hours,
        high_mttr_count,
        avg_precision_at_10,
        drift_failure_rate,
        drift_failure_rate_7d_avg,
        training_run_count,
        model_deployment_count,
        drift_detected_count,
        avg_retrain_response_hours,
        fast_retrain_response_count,
        slow_retrain_response_count,

        score_deploy_freq,
        score_ci_quality,
        score_drift,
        score_lead_time,
        score_mttr,

        round(
            score_deploy_freq * 0.25
            + score_ci_quality * 0.20
            + score_drift      * 0.20
            + score_lead_time  * 0.20
            + score_mttr       * 0.15,
            1
        ) as health_score,

        case
            when (score_deploy_freq * 0.25 + score_ci_quality * 0.20
                  + score_drift * 0.20 + score_lead_time * 0.20
                  + score_mttr * 0.15) >= 90 then 'healthy'
            when (score_deploy_freq * 0.25 + score_ci_quality * 0.20
                  + score_drift * 0.20 + score_lead_time * 0.20
                  + score_mttr * 0.15) >= 70 then 'warning'
            else 'critical'
        end as health_tier

    from scored
)

select * from final
