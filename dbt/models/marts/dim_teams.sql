/*
  Team dimension.

  In production this would be sourced from an internal service registry.
  Here it's derived from the event grain to keep things self-contained.
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

        case team
            when 'team-reco'    then 'Personalisation'
            when 'team-pricing' then 'Commercial'
            when 'team-churn'   then 'CRM'
            when 'team-search'  then 'Discovery'
        end as domain,

        case team
            when 'team-reco'    then 'tier-1'
            when 'team-pricing' then 'tier-1'
            when 'team-churn'   then 'tier-2'
            when 'team-search'  then 'tier-1'
        end as sla_tier

    from team_models
)

select * from enriched
