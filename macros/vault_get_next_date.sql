{% macro vault_get_next_date() %}

    {% set increment_table %} 
        {{ this.database + '.' + this.schema + '.increment_vault_date' }} 
    {% endset %}
    
    {% set date_query %}
        WITH raw_dates as (
        SELECT 
            DISTINCT O_ORDERDATE,
            ROW_NUMBER() OVER (ORDER BY O_ORDERDATE) as row_num
        FROM raw.TPCH_SF001.ORDERS
        GROUP BY O_ORDERDATE
        ),
        next_row_num as (
        select ID as row_num
        from {{ increment_table }}
        )

        select O_ORDERDATE as next_date
        from raw_dates
        inner join next_row_num on (next_row_num.row_num = raw_dates.row_num)
    {% endset %}

    {% set results = run_query(date_query) %}

    {% if execute %}
        {# Return the first column #}
        {% set next_date = results.columns[0].values()[0] %}
    {% else %}
        {% set next_date = [] %}
    {% endif %}
    
    {{ return(next_date) }}
{% endmacro %} 