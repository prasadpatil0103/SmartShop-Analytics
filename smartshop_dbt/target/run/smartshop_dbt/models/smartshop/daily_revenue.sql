
  
  
  
  create or replace view `workspace`.`default`.`daily_revenue`
  
  as (
    with orders as (
    select * from workspace.default.olist_orders_dataset
),
payments as (
    select * from workspace.default.olist_order_payments_dataset
)

select
    date(o.order_purchase_timestamp) as order_date,
    count(distinct o.order_id)       as total_orders,
    round(sum(p.payment_value), 2)   as total_revenue,
    round(avg(p.payment_value), 2)   as avg_order_value
from orders o
join payments p on o.order_id = p.order_id
where o.order_status = 'delivered'
group by date(o.order_purchase_timestamp)
order by order_date
  )
