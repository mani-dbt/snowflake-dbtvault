
{% set segments = dbt_utils.get_column_values(table=ref('dim_customers'),column='market_segment',filter_column='nation',filter_value='CHINA') %}

{% for segment in segments %}
    select customer_key from {{ ref('dim_customers') }} where market_segment = '{{segment}}'

{% if not loop.last %} UNION {%endif%}
{% endfor %}
