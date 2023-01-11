{{
    config(
        materialized='table'
    )
}}

with orders as (
    select *
    from   {{ ref('fct_orders') }}
)

select * from orders
