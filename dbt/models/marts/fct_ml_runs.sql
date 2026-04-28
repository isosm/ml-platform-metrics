/*
  One row per model training run with time-to-deploy attached.

  Time to deploy = hours between training_run and the next model_deployed
  for the same team. Analogous to lead time but on the ML delivery side.
*/

with training as (
    select * from {{ ref('stg_ml_events') }}
    where is_training_run
),

deployments as (
    select
        team,
        model_name,
        event_timestamp  as deployed_at
    from {{ ref('stg_ml_events') }}
    where is_deployment
),

drift as (
    select
        team,
        model_name,
        event_day        as drift_day,
        event_timestamp  as drift_detected_at,
        psi_score        as drift_psi_score
    from {{ ref('stg_ml_events') }}
    where is_drift_detected
),

retrain as (
    select
        team,
        model_name,
        event_timestamp  as retrain_triggered_at
    from {{ ref('stg_ml_events') }}
    where is_retrain_triggered
),

-- For each training run, find next deployment
with_deploy as (
    select
        t.event_id         as run_id,
        t.team,
        t.model_name,
        t.event_day        as run_day,
        t.event_timestamp  as trained_at,
        t.precision_at_10,
        t.psi_score,
        t.psi_severity,
        t.drift_triggered,

        min(d.deployed_at)  as deployed_at

    from training t
    left join deployments d
        on t.team = d.team
        and t.model_name = d.model_name
        and d.deployed_at > t.event_timestamp
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

final as (
    select
        run_id,
        team,
        model_name,
        run_day,
        trained_at,
        deployed_at,
        precision_at_10,
        psi_score,
        psi_severity,
        drift_triggered,

        timestamp_diff(deployed_at, trained_at, hour)  as time_to_deploy_hours,
        deployed_at is not null                        as was_deployed
    from with_deploy
)

select * from final
