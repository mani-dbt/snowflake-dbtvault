{% snapshot dim_account_scd_snapshot %}
    {{
        config(
            strategy='timestamp',
            unique_key='account_period_id',
            target_schema='dbt_mpachineelam_vault',            
            updated_at='ebx_last_updated_at'
        )
    }}

    select * from {{ ref('stg_dim_acct_src') }}

 {% endsnapshot %}
