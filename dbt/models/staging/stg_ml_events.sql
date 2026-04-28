with source as (
    select * from {{ source('raw', 'ml_events') }}
),

renamed as (
    select
        event_id,
        team,
        model_name,
        event_type,

        date(event_date)   as event_day,
        event_date         as event_timestamp,

        psi_score,
        precision_at_10,

        coalesce(drift_triggered, psi_score > 0.20) as drift_triggered,

        -- PSI severity buckets per DORA-for-ML thresholds:
        -- stable < 0.10, warning 0.10–0.20, critical > 0.20
        case
            when psi_score < 0.10                then 'stable'
            when psi_score between 0.10 and 0.20 then 'warning'
            when psi_score > 0.20                then 'critical'
        end as psi_severity,

        event_type = 'training_run'       as is_training_run,
        event_type = 'model_deployed'     as is_deployment,
        event_type = 'drift_detected'     as is_drift_detected,
        event_type = 'retrain_triggered'  as is_retrain_triggered

    from source
)

select * from renamed
