{{
    config(
        materialized = 'table',
        schema = 'marts',
    )
}}

-- Required by MetricFlow to fill in days with no events.
-- Equivalent to a dim_date in Kimball modeling.

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
