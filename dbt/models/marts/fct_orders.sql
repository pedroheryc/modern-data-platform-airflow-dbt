select
  customer_id,
  count(*) as orders_count,
  sum(amount) as total_amount,
  min(order_date) as first_order_date,
  max(order_date) as last_order_date
from {{ ref('stg_orders') }}
group by 1
