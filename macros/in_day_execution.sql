{% macro in_day_execution(inputvalue) %}

    {%- set insert_stmt -%}
    begin;
        insert into ANALYTICS.DBT_MPACHINEELAM_VAULT.INSERTTIME values (DATEADD(Day ,-{{inputvalue}}, current_timestamp()));
    commit;
    {%- endset -%}

    {% set insert_query_results = run_query(insert_stmt) %}

    {% if execute %}
    {% do insert_query_results.print_table() %}
    {% endif %}

{% endmacro %}


