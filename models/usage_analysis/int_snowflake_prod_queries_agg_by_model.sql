with

production_queries as (

    select * from {{ ref('int_snowflake_prod_queries') }}
    where execution_status = 'SUCCESS'  -- Only successful queries

),

snowflake_objects as (

    select * from {{ ref('stg_snowflake_prod_objects') }}

),

filtered_queries as (

        select
            production_queries.*,
            snowflake_objects.object_size_in_rows,
            snowflake_objects.object_size_in_gigabytes
        
        from production_queries
        inner join snowflake_objects
            on production_queries.dbt_model_name = snowflake_objects.snowflake_object_name

),

aggregated as (

    select
        model_config_id,
        dbt_model_name,
        is_incremental_full_refresh,
        max(start_time) as latest_run_at
    
    from filtered_queries
    group by all
    
),

current_configs as (

    select
        model_config_id,
        latest_run_at,
        
        row_number() over (
            partition by
                dbt_model_name,
                is_incremental_full_refresh
            order by latest_run_at
        ) as model_config_version,
        
        -- a current config is the latest configuration on a per model per refresh type basis
        case
            when row_number() over (
                partition by
                    dbt_model_name,
                    is_incremental_full_refresh
                order by latest_run_at desc
                ) = 1
                then true
            else false
        end as is_current_config,

        -- a previous config is the version immediately before the current
        -- this is largely used for change analysis
        case
            when row_number() over (
                partition by
                    dbt_model_name,
                    is_incremental_full_refresh
                order by latest_run_at desc
                ) = 2
                then true
            else false
        end as is_previous_config

    from aggregated

),

model_performance_by_run as (

    select
        filtered_queries.model_config_id,
        filtered_queries.dbt_cloud_run_id,
        filtered_queries.dbt_model_name,
        filtered_queries.dbt_materialization_type,
        filtered_queries.is_incremental_full_refresh,
        filtered_queries.warehouse_size_numeric,
        filtered_queries.object_size_in_rows,
        filtered_queries.object_size_in_gigabytes,
        
        current_configs.model_config_version,
        current_configs.is_current_config,
        current_configs.is_previous_config,

        current_configs.latest_run_at,
        count(distinct
            case
                when filtered_queries.gigabytes_spilled_to_remote_storage > 0
                    then dbt_cloud_run_id
            end
        ) as count_of_runs_with_remote_spillage,
        sum(filtered_queries.credits_used_cloud_services) as total_cloud_credits_used,
        sum(filtered_queries.total_elapsed_time_in_seconds) as total_execution_time_in_seconds,
        sum(filtered_queries.compute_credits_used) as total_compute_credits_used,
        sum(filtered_queries.rows_produced) as total_rows_produced,
        sum(filtered_queries.gigabytes_written) as total_gigabytes_written,
        sum(filtered_queries.gigabytes_spilled_to_local_storage) as total_gigabytes_spilled_to_local_storage,
        sum(filtered_queries.gigabytes_spilled_to_remote_storage) as total_gigabytes_spilled_to_remote_storage,

        min(start_time) as run_started_at

    from filtered_queries
    left join current_configs
        on filtered_queries.model_config_id = current_configs.model_config_id
    group by all

),

filtered as (
    -- the below CTE gets the most recent 100 runs per unique config
    select * from model_performance_by_run
    qualify row_number() over (
            partition by 
                dbt_model_name,
                dbt_materialization_type,
                is_incremental_full_refresh,
                warehouse_size_numeric
            order by
                run_started_at desc
        ) <= 100

),

average_model_performance as (

    select
        model_config_id,
        dbt_model_name,
        dbt_materialization_type,
        is_incremental_full_refresh,
        warehouse_size_numeric,
        model_config_version,
        is_current_config,
        is_previous_config,
        object_size_in_rows,
        object_size_in_gigabytes,
        latest_run_at,

        count(*) as count_of_runs,
        sum(count_of_runs_with_remote_spillage) as count_of_runs_with_remote_spillage,
        sum(count_of_runs_with_remote_spillage) / count_of_runs as pct_of_runs_with_remote_spillage,
        avg(total_cloud_credits_used) as average_cloud_credits_used,
        avg(total_execution_time_in_seconds) as average_execution_time_in_seconds,
        avg(total_compute_credits_used) as average_compute_credits_used,
        avg(total_rows_produced) as average_rows_produced,
        avg(total_gigabytes_written) as average_gigabytes_written,
        avg(total_gigabytes_spilled_to_local_storage) as average_gigabytes_spilled_to_local_storage,
        avg(total_gigabytes_spilled_to_remote_storage) as average_gigabytes_spilled_to_remote_storage

    from filtered
    group by all

)

select * from average_model_performance