with source as (
    select * from {{ source('raw', 'github_events') }}
),

renamed as (
    select
        event_id,
        team,
        event_type,

        date(event_date)     as event_day,
        event_date           as event_timestamp,

        lead_time_days,
        ci_passed,

        event_type = 'ci_run'           as is_ci_run,
        event_type = 'pr_merged'        as is_pr_merged,
        event_type = 'deployment'       as is_deployment,
        event_type = 'incident_opened'  as is_incident_opened,
        event_type = 'incident_closed'  as is_incident_closed,

        case
            when event_type = 'ci_run' and ci_passed = false then true
            else false
        end as is_ci_failure

    from source
)

select * from renamed
