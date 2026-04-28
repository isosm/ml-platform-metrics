/*
  Static team dimension. In production this would be sourced from an internal
  service registry or HR system. Here it's built from the event grain.
*/

with team_models as (
    select distinct
        team,
        model_name
    from {{ ref('stg_ml_events') }}
),

enriched as (
    select
        team,
        model_name,

        -- Domain classification for dashboard grouping
        case team
            when 'team-reco'    then 'Personalisation'
            when 'team-pricing' then 'Commercial'
            when 'team-churn'   then 'CRM'
            when 'team-search'  then 'Discovery'
        end as domain,

        -- SLA tier — influences alerting thresholds
        case team
            when 'team-reco'    then 'tier-1'
            when 'team-pricing' then 'tier-1'
            when 'team-churn'   then 'tier-2'
            when 'team-search'  then 'tier-1'
        end as sla_tier

    from team_models
)

select * from enriched
