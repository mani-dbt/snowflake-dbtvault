{{
    config(
        materialized='table',
        post_hook='update abc set desc=asdf where id = 5'
    )
}}

with orders as (
    select *
    from   {{ ref('fct_orders') }}
)

select * from orders
