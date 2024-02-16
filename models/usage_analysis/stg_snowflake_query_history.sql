{{
    config(
        materialized = 'incremental',
        unique_key = 'query_id',
        full_refresh = false
    )
}}

with

source as (

    select * from {{ source('snowflake_account_usage', 'query_history') }}

    {% if is_incremental() %}
        where end_time > dateadd(hour, -1, (select max(end_time) from {{ this }}))
    {% endif %}

),

final as (

    select
        query_id,
        session_id,
        database_id,
        schema_id,
        warehouse_id,

        database_name,
        schema_name,
        user_name,
        role_name,
        
        warehouse_name,
        warehouse_size,
        warehouse_type,

        query_text,
        query_type,
        query_tag,
        execution_status,

        rows_produced,
        -- this is storage credits, not as interesting
        credits_used_cloud_services,
        bytes_scanned / 1000000000 as gigabytes_scanned,
        percentage_scanned_from_cache,
        bytes_written / 1000000000 as gigabytes_written,
        bytes_spilled_to_local_storage / 1000000000 as gigabytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage / 1000000000 as gigabytes_spilled_to_remote_storage,

        start_time,
        end_time,
        execution_time / 1000 as execution_time_in_seconds,
        total_elapsed_time / 1000 as total_elapsed_time_in_seconds,

        -- Directional, not necessarily what we are billed. Use warehouse metering for precision.
        total_elapsed_time_in_seconds * (
            case
                when warehouse_size = 'X-Small'
                    then 0.0003
                when warehouse_size = 'Small'
                    then 0.0006
                when warehouse_size = 'Medium'
                    then 0.0011
                when warehouse_size = 'Large'
                    then 0.0022
                when warehouse_size = 'X-Large'
                    then 0.0044
                when warehouse_size = '2X-Large'
                    then 0.0089
                when warehouse_size = '3X-Large'
                    then 0.0178
                when warehouse_size = '4X-Large'
                    then 0.0356
                when warehouse_size = '5X-Large'
                    then 0.0711
                when warehouse_size = '6X-Large'
                    then 0.1422
            end
        ) as compute_credits_used

    from source

)

select * from final