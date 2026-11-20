{# Naive UTC timestamp -> naive Europe/Helsinki wall-clock timestamp #}
{% macro to_helsinki(ts) -%}
    timezone('Europe/Helsinki', ({{ ts }}) at time zone 'UTC')
{%- endmacro %}

{% macro helsinki_date(ts) -%}
    cast({{ to_helsinki(ts) }} as date)
{%- endmacro %}

{% macro money(expr) -%}
    cast({{ expr }} as decimal(14, 2))
{%- endmacro %}

{# Write schemas as staging / marts instead of main_staging / main_marts #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
