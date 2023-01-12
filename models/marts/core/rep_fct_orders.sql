{{
    config(
        materialized = 'table',
        tags=['finance']
    )
}}

with order_item as (
    
    select * from {{ ref('int_order_lineitem') }}

),
final as (

    select 

        order_item.order_pk, 
        order_item.orderdate,
        order_item.orderstatus,
        order_item.orderpriority,
        order_item.clerk_name,
        order_item.shippriority,
        order_item.discount,
        order_item.extendedprice,
        order_item.quantity,     
        1 as order_count
    from
        orders
)
select 
    *
from
    final

order by
    order_date