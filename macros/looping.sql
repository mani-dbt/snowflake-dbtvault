{% macro looping() %}
    
    {% set my_query %}
        select * from {{ ref('inserttime') }}
    {% endset %}

    {% set results = run_query(my_query) %}
    {% do results.print_table() %}

{% endmacro %}