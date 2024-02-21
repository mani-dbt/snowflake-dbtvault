{%- set wh_sizes = dbt_utils.get_column_values(
    table = ref('int_snowflake_prod_queries'),
    column = 'warehouse_size',
    order_by = 'min(warehouse_size_numeric)'
) -%}

with

production_queries as (

    select * from {{ ref('int_snowflake_prod_queries') }}
    where dbt_cloud_run_id is not null

),

aggregated as (

    select
        dbt_cloud_run_id,
        sum(compute_credits_used) as total_compute_credits_used,
        sum(est_credit_cost) as total_est_dollars_spent,
        count(*) as total_queries_executed,
        count(distinct dbt_model_name) as total_dbt_models_executed,

        {% for wh_size in wh_sizes %}
            {# Cleanup warehouse sizes to make friendly column names #}
            {%- set wh_name = dbt_utils.slugify(wh_size) -%}
            {%- if wh_name[0] == '_' -%}{%- set wh_name = wh_name[1:] -%}{%- endif -%}
            
            sum(
                case
                    when warehouse_size = '{{ wh_size }}'
                        then compute_credits_used
                    else 0
                end
            ) as total_compute_credits_used_on_{{ wh_name }}_wh,

            sum(
                case
                    when warehouse_size = '{{ wh_size }}'
                        then est_credit_cost
                    else 0
                end
            ) as total_est_dollars_spent_on_{{ wh_name }}_wh,


            sum(
                case
                    when warehouse_size = '{{ wh_size }}'
                        then 1
                    else 0
                end
            ) as total_queries_executed_on_{{ wh_name }}_wh {% if not loop.last %},{% endif %}
        {% endfor %}

    from production_queries
    group by 1

)

select * from aggregated