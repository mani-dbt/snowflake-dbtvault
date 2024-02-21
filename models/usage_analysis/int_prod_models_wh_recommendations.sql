with

model_performance as (

    select * from {{ ref('int_snowflake_prod_queries_agg_by_model') }}
    -- For now, we can't control test warehouse configurations
    where dbt_materialization_type != 'test'

),

warehouse_recommendations as (

    select
        model_config_id,
        dbt_model_name,
        dbt_materialization_type,
        is_incremental_full_refresh,
        warehouse_size_numeric,
        model_config_version,
        is_current_config,
        is_previous_config,

        object_size_in_rows as current_object_size_in_rows,
        object_size_in_gigabytes as current_object_size_in_gigabytes,

        count_of_runs,
        count_of_runs_with_remote_spillage,
        pct_of_runs_with_remote_spillage,
        average_execution_time_in_seconds,
        average_cloud_credits_used + average_compute_credits_used as average_credits_used,
        average_rows_produced,
        average_gigabytes_written,
        average_gigabytes_spilled_to_local_storage,
        average_gigabytes_spilled_to_remote_storage,

        -- https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#model-run-time
        case
            when average_execution_time_in_seconds between 0 and 60
                then 1
            when average_execution_time_in_seconds between 60 and 300
                then 2
            when average_execution_time_in_seconds between 300 and 900
                then 3
            when average_execution_time_in_seconds between 900 and 1800
                then 4
            when average_execution_time_in_seconds between 1800 and 3600
                then 5
            else 6
        end as warehouse_rec_by_execution_time,

        -- https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#model-size
        case
            when object_size_in_rows between 0 and 1000000
                then 1
            when object_size_in_rows between 1000000 and 10000000
                then 2
            when object_size_in_rows between 10000000 and 100000000
                then 3
            when object_size_in_rows between 100000000 and 1000000000
                then 4
            when object_size_in_rows >= 1000000000
                then 5
        end as warehouse_rec_by_model_size_in_rows,

        -- https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#model-size
        case
            when object_size_in_gigabytes between 0 and 1
                then 1
            when object_size_in_gigabytes between 1 and 10
                then 2
            when object_size_in_gigabytes between 10 and 100
                then 3
            when object_size_in_gigabytes between 100 and 1000
                then 4
            when object_size_in_gigabytes >= 1000
                then 5
        end as warehouse_rec_by_model_size_in_gigabytes,

        -- https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#model-efficiency
        case
            when average_gigabytes_spilled_to_remote_storage > 0
                and pct_of_runs_with_remote_spillage >= 0.25
                then 'Very Poor'
            when average_gigabytes_spilled_to_local_storage <= (5 * average_gigabytes_written)
                then 'Acceptable'
            when average_gigabytes_spilled_to_local_storage > (5 * average_gigabytes_written)
                then 'Poor'
        end as model_efficiency_ranking,

        latest_run_at

    from model_performance

)

select * from warehouse_recommendations