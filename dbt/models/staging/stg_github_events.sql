with source as (
    select * from {{ source('raw', 'github_events') }}
),

renamed as (
    select
        event_id,
        team,
        event_type,

        -- Normalise to DATE for daily aggregations downstream
        date(event_date)     as event_day,
        event_date           as event_timestamp,

        -- DORA signal columns — null for non-applicable event types
        lead_time_days,
        ci_passed,

        -- Derived convenience flags
        event_type = 'ci_run'           as is_ci_run,
        event_type = 'pr_merged'        as is_pr_merged,
        event_type = 'deployment'       as is_deployment,
        event_type = 'incident_opened'  as is_incident_opened,
        event_type = 'incident_closed'  as is_incident_closed,

        -- ci_passed is only meaningful on ci_run rows
        case
            when event_type = 'ci_run' and ci_passed = false then true
            else false
        end as is_ci_failure

    from source
)

select * from renamed
