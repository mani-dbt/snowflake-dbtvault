{% macro increment_vault_date_id() %}

    {% set increment_table %} 
        {{ 'analytics.dbt_mpachineelam_vault.date_increment' }} 
    {% endset %}

    {% set create_query %}
        create table if not exists {{ increment_table }}
        (
            increment_id int
        );
    {% endset %}

    {% set results = run_query(create_query) %}

    {% set count_query %}
        select count(*) from {{ increment_table }}
    {% endset %}

    {% set results = run_query(count_query) %}

    {% if execute %}
    {# Return the first column #}
        {% set count_records = results.columns[0].values()[0] %}
    {% else %}
        {% set count_records = [] %}
    {% endif %}

    {% if count_records == 0 %}
        insert into {{ increment_table }} values (1)
    {% else %}
        update {{ increment_table }}
        set increment_id = (
                select increment_id+1 from 
                {{ increment_table }}
            );
    {% endif %}   

{% endmacro %} 