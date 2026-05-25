
  
  
  
  create or replace view `workspace`.`default`.`customer_analytics`
  
  as (
    with orders as (
    select * from workspace.default.olist_orders_dataset
),
payments as (
    select * from workspace.default.olist_order_payments_dataset
),
customers as (
    select * from workspace.default.olist_customers_dataset
)

select
    c.customer_state                        as state,
    count(distinct c.customer_id)           as total_customers,
    count(distinct o.order_id)              as total_orders,
    round(sum(p.payment_value), 2)          as total_revenue,
    round(avg(p.payment_value), 2)          as avg_order_value
from customers c
join orders o on c.customer_id = o.customer_id
join payments p on o.order_id = p.order_id
where o.order_status = 'delivered'
group by c.customer_state
order by total_revenue desc
  )
