{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

select current_timestamp() as curr_time

