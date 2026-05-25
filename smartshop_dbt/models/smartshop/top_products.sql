with items as (
    select * from workspace.default.olist_order_items_dataset
),
products as (
    select * from workspace.default.olist_products_dataset
),
orders as (
    select * from workspace.default.olist_orders_dataset
),
translations as (
    select * from workspace.default.product_category_name_translation
)

select
    coalesce(t.product_category_name_english,
             p.product_category_name) as category,
    count(distinct i.order_id)        as total_orders,
    round(sum(i.price), 2)            as total_revenue,
    round(avg(i.price), 2)            as avg_price
from items i
join products p on i.product_id = p.product_id
join orders o on i.order_id = o.order_id
left join translations t
       on p.product_category_name = t.product_category_name
where o.order_status = 'delivered'
group by coalesce(t.product_category_name_english,
                  p.product_category_name)
order by total_revenue desc
limit 20