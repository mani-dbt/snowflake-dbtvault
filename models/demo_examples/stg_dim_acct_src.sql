{{
    config(
        materialized='table'
    )
}}

with 

generate_acct_period_id as (
select {{dbt_utils.generate_surrogate_key([
            'EBX_LOCAL_ID',
            'BUSINESS_REPORTING_PERIOD'
        ])}} as account_period_id,
        *
        from {{ source('tpch', 'dim_acct_src') }}
),

final as (

    select account_period_id, ebx_local_id, business_reporting_period, ebx_last_updated_at, glaccountcode, account_name from generate_acct_period_id

)

select * from final
