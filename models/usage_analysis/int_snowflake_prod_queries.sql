with

query_history as (

    select * from {{ ref('stg_snowflake_query_history') }}
    where start_time::date > {{ dateadd('month', -6, 'current_date') }}
        and warehouse_size is not null  -- Only queries that require compute
        and user_name = 'DBT_CLOUD_USER'  -- Only queries related to production dbt runs

),

parse_query_tags as (

    select
        *,
        try_parse_json(query_tag):dbt_run_id::string as dbt_cloud_run_id,
        try_parse_json(query_tag):dbt_job_id::string as dbt_cloud_job_id,
        try_parse_json(query_tag):dbt_model_name::string as dbt_model_name,
        try_parse_json(query_tag):dbt_project_name::string as dbt_project_name,
        try_parse_json(query_tag):dbt_environment_name::string as dbt_cloud_environment_name,
        try_parse_json(query_tag):dbt_run_reason::string as dbt_run_reason,
        try_parse_json(query_tag):dbt_materialization_type::string as dbt_materialization_type,
        try_parse_json(query_tag):dbt_user_name::string as dbt_user_name,
        try_parse_json(query_tag):dbt_incremental_full_refresh::boolean as is_incremental_full_refresh

    from query_history

),

cleaned as (

    select
        query_id,
        session_id,
        database_id,
        schema_id,
        warehouse_id,
        
        dbt_cloud_run_id,
        dbt_cloud_job_id,

        dbt_project_name,
        dbt_model_name,
        dbt_cloud_environment_name,
        dbt_user_name,

        database_name,
        schema_name,
        user_name,
        role_name,
        
        warehouse_name,
        warehouse_size,
        -- Supports ranking by warehouse size downstream
        case
            when warehouse_size = 'X-Small'
                then '1 - XS'
            when warehouse_size = 'Small'
                then '2 - S'
            when warehouse_size = 'Medium'
                then '3 - M'
            when warehouse_size = 'Large'
                then '4 - L'
            when warehouse_size = 'X-Large'
                then '5 - XL'
            when warehouse_size = '2X-Large'
                then '6 - 2XL'
            when warehouse_size = '3X-Large'
                then '7 - 3XL'
            when warehouse_size = '4X-Large'
                then '8 - 4XL'
            when warehouse_size = '5X-Large'
                then '9 - 5XL'
            when warehouse_size = '6X-Large'
                then '10 - 6XL'
        end as warehouse_size_numeric,
        warehouse_type,
        execution_status,

        query_text,
        query_type,
        dbt_run_reason,
        dbt_materialization_type,

        rows_produced,
        compute_credits_used,
        compute_credits_used * 2.22 as est_credit_cost,
        credits_used_cloud_services,
        gigabytes_scanned,
        percentage_scanned_from_cache,
        gigabytes_written,
        gigabytes_spilled_to_local_storage,
        gigabytes_spilled_to_remote_storage,
        
        is_incremental_full_refresh,
        start_time,
        end_time,
        execution_time_in_seconds,
        total_elapsed_time_in_seconds
    
    from parse_query_tags

),

with_id as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'dbt_model_name',
            'dbt_materialization_type',
            'is_incremental_full_refresh',
            'warehouse_size_numeric'
        ]) }} as model_config_id,
        *
    
    from cleaned

)

select * from with_id