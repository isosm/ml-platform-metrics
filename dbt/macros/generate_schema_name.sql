{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {#-
            prod  → clean schema names: staging, intermediate, marts
            dev   → prefixed:           dbt_dev_staging, dbt_dev_intermediate, dbt_dev_marts
            ci    → prefixed:           dbt_ci_staging, dbt_ci_intermediate, dbt_ci_marts
        -#}
        {%- if target.name == 'prod' -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            {{ target.schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
