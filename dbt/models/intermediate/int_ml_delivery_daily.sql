/*
  ML delivery performance metrics per team per day.

  model_training_frequency  — training runs that day
  avg_time_to_deploy_hours  — training_run to model_deployed lag
  drift_failure_rate        — training runs with PSI > 0.20
  drift_events              — drift detections (model MTTR in mart layer)

  30-day baseline precision computed here for drift scoring.
*/

with ml as (
    select * from {{ ref('stg_ml_events') }}
),

daily_agg as (
    select
        event_day          as date_day,
        team,
        model_name,

        countif(is_training_run)                     as training_run_count,

        avg(case when is_training_run then precision_at_10 end)   as avg_precision_at_10,
        avg(case when is_training_run then psi_score end)         as avg_psi_score,

        countif(is_training_run and drift_triggered)              as drift_training_count,
        safe_divide(
            countif(is_training_run and drift_triggered),
            nullif(countif(is_training_run), 0)
        )                                            as drift_failure_rate,

        countif(is_deployment)                       as model_deployment_count,
        countif(is_drift_detected)                   as drift_detected_count,
        countif(is_retrain_triggered)                as retrain_triggered_count

    from ml
    group by 1, 2, 3
),

with_rolling as (
    select
        *,
        avg(avg_precision_at_10) over (
            partition by team, model_name
            order by date_day
            rows between 29 preceding and current row
        )                                            as precision_30d_baseline,

        avg(drift_failure_rate) over (
            partition by team, model_name
            order by date_day
            rows between 6 preceding and current row
        )                                            as drift_failure_rate_7d_avg,

        sum(training_run_count) over (
            partition by team, model_name
            order by date_day
            rows between 29 preceding and current row
        )                                            as training_runs_last_30d

    from daily_agg
)

select * from with_rolling
