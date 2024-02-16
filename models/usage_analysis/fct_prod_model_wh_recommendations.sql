with

warehouse_recommendations as (

    select * from {{ ref('int_prod_models_wh_recommendations_stats') }}

),

cast_to_warehouse_numeric as (

    select
        model_config_id,
        dbt_model_name,
        dbt_materialization_type,
        is_incremental_full_refresh,
        warehouse_size_numeric,
        model_config_version,
        is_current_config,
        is_previous_config,
        current_object_size_in_rows,
        current_object_size_in_gigabytes,
        count_of_runs,
        average_execution_time_in_seconds,
        average_credits_used,
        average_rows_produced,
        average_gigabytes_written,
        average_gigabytes_spilled_to_local_storage,
        average_gigabytes_spilled_to_remote_storage,
        pct_of_runs_with_remote_spillage,
        latest_run_at,

        case
            when warehouse_size_numeric in ('1 - XS', '2 - S', '3 - M')
                then '4 - L'
            when warehouse_size_numeric = '4 - L'
                then '5 - XL'
            when warehouse_size_numeric = '5 - XL'
                then '6 - 2XL'
        end as next_warehouse_size,

        case
            when next_warehouse_size = '4 - L'
                then 'large'
            when next_warehouse_size = '5 - XL'
                then 'xlarge'
            when next_warehouse_size = '6 - 2XL'
                then '2xlarge'
        end as next_warehouse_size_arg,

        -- Translate to values for `snowflake_warehouse_sizes` var in dbt_project.yml
        case
            when recommended_warehouse_size in (1, 2, 3)
                then 'medium'
            when recommended_warehouse_size = 4
                then 'large'
            when recommended_warehouse_size = 5
                then 'xlarge'
            when recommended_warehouse_size = 6
                then '2xlarge'
        end as recommended_warehouse_size_arg,

        

        {%- set columns = [
            'warehouse_rec_by_execution_time', 'warehouse_rec_by_model_size_in_rows',
            'warehouse_rec_by_model_size_in_gigabytes', 'recommended_warehouse_size',
            'min_warehouse_size', 'median_warehouse_size', 'max_warehouse_size'
        ] -%}

        {% for column in columns %}
            case
                when {{ column }} = 1
                    then '1 - XS'
                when {{ column }} = 2
                    then '2 - S'
                when {{ column }} = 3
                    then '3 - M'
                when {{ column }} = 4
                    then '4 - L'
                when {{ column }} = 5
                    then '5 - XL'
                when {{ column }} = 6
                    then '6 - 2XL'
                when {{ column }} = 7
                    then '7 - 3XL'
                when {{ column }} = 8
                    then '8 - 4XL'
                when {{ column }} = 9
                    then '9 - 5XL'
                when {{ column }} = 10
                    then '10 - 6XL'
            end as {{ column }},

        {% endfor %}

        model_efficiency_ranking

    from warehouse_recommendations

),

verbose_recommendations as (

    select
        *,

        -- Recommendations
        case
            when model_efficiency_ranking in ('Poor', 'Very Poor')
                then 'Poor efficiency, increase warehouse size'
            when warehouse_size_numeric = warehouse_rec_by_execution_time
                then 'Keep as is'
            when warehouse_rec_by_execution_time in ('1 - XS', '2 - S', '3 - M')
                then 'Keep as is'
            when warehouse_size_numeric != recommended_warehouse_size
                then 'Potential opportunity for optimization'
            else 'Keep as is'
        end as recommendation,

        case
            when recommendation = 'Poor efficiency, increase warehouse size'
                and next_warehouse_size is null
                then 'Speak with Snowflake Admin'
            when recommended_warehouse_size_arg is null
                then 'Speak with Snowflake admin'
            when recommendation = 'Keep as is'
                then null
            when recommendation = 'Poor efficiency, increase warehouse size'
                then 'snowflake_warehouse = set_warehouse_config(\'' || next_warehouse_size_arg || '\')'
            else 'snowflake_warehouse = set_warehouse_config(\'' || recommended_warehouse_size_arg || '\')'
        end as pastable_warehouse_configuration,

        case
            when recommendation = 'Poor efficiency, increase warehouse size'
                then next_warehouse_size
            else recommended_warehouse_size
        end as recommended_warehouse_size_override

    from cast_to_warehouse_numeric

),

final as (

    select
        --identifiers
        model_config_id,

        -- dimensions
        dbt_materialization_type,
        dbt_model_name,
        warehouse_size_numeric,

        -- booleans
        is_incremental_full_refresh,
        is_current_config,
        is_previous_config,
        
        -- model run stats
        count_of_runs,
        average_execution_time_in_seconds,
        average_credits_used,
        average_rows_produced,
        average_gigabytes_written,
        average_gigabytes_spilled_to_local_storage,
        average_gigabytes_spilled_to_remote_storage,
        pct_of_runs_with_remote_spillage,
        
        -- model information
        current_object_size_in_rows,
        current_object_size_in_gigabytes,

        -- recommendations
        recommendation,
        pastable_warehouse_configuration,
        model_efficiency_ranking,
        warehouse_rec_by_execution_time,
        warehouse_rec_by_model_size_in_rows,
        warehouse_rec_by_model_size_in_gigabytes,
        recommended_warehouse_size_override as recommended_warehouse_size

    from verbose_recommendations
        
)

select * from final