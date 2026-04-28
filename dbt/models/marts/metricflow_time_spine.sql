{{
    config(
        materialized = 'table',
        schema = 'marts',
    )
}}

-- MetricFlow kräver en time spine — en rad per dag.
-- Analogt med en dim_date i klassisk Kimball-modellering.
-- dbt_utils.date_spine genererar sekvensen utan att lagra data i källan.

with days as (
    {{
        dbt.date_spine(
            'day',
            "cast('2020-01-01' as date)",
            "cast('2030-01-01' as date)"
        )
    }}
)

select cast(date_day as date) as date_day
from days
