/*
  ML delivery performance metrics per team per day.

  Four ML-DORA metrics (the project's unique angle):
    model_training_frequency  — training runs that day
    avg_time_to_deploy_hours  — training_run → model_deployed lag
    drift_failure_rate        — % training runs with PSI > 0.20
    drift_events              — drift detections (model MTTR in mart)

  PSI thresholds follow CLAUDE.md:
    < 0.10  stable   — no action needed
    0.10–0.20  warning — monitor more frequently
    > 0.20  critical — retrain trigger
*/

with ml as (
    select * from {{ ref('stg_ml_events') }}
),

daily_agg as (
    select
        event_day          as date_day,
        team,
        model_name,

        -- Model Training Frequency
        countif(is_training_run)                     as training_run_count,

        -- Quality at training time
        avg(case when is_training_run then precision_at_10 end)   as avg_precision_at_10,
        avg(case when is_training_run then psi_score end)         as avg_psi_score,

        -- Drift Failure Rate = training runs with PSI > 0.20
        countif(is_training_run and drift_triggered)              as drift_training_count,
        safe_divide(
            countif(is_training_run and drift_triggered),
            nullif(countif(is_training_run), 0)
        )                                            as drift_failure_rate,

        -- Deployment events
        countif(is_deployment)                       as model_deployment_count,

        -- Drift signals (model MTTR calculated in mart)
        countif(is_drift_detected)                   as drift_detected_count,
        countif(is_retrain_triggered)                as retrain_triggered_count

    from ml
    group by 1, 2, 3
),

with_rolling as (
    select
        *,

        -- 30-day baseline precision (reference point for drift scoring)
        avg(avg_precision_at_10) over (
            partition by team, model_name
            order by date_day
            rows between 29 preceding and current row
        )                                            as precision_30d_baseline,

        -- 7-day rolling drift rate
        avg(drift_failure_rate) over (
            partition by team, model_name
            order by date_day
            rows between 6 preceding and current row
        )                                            as drift_failure_rate_7d_avg,

        -- Training frequency trend (last 30 days)
        sum(training_run_count) over (
            partition by team, model_name
            order by date_day
            rows between 29 preceding and current row
        )                                            as training_runs_last_30d

    from daily_agg
)

select * from with_rolling
