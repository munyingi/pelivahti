{# Naive UTC timestamp -> naive wall-clock timestamp in the business time zone (var: business_timezone) #}
{% macro to_local(ts) -%}
    timezone('{{ var("business_timezone") }}', ({{ ts }}) at time zone 'UTC')
{%- endmacro %}

{% macro local_date(ts) -%}
    cast({{ to_local(ts) }} as date)
{%- endmacro %}

{% macro money(expr) -%}
    cast({{ expr }} as decimal(14, 2))
{%- endmacro %}

{# Write schemas as staging / marts instead of main_staging / main_marts #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
