
  
  
  
  create or replace view `workspace`.`default`.`order_status_summary`
  
  as (
    select
    order_status,
    count(*)                                    as total_orders,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage
from workspace.default.olist_orders_dataset
group by order_status
order by total_orders desc
  )
