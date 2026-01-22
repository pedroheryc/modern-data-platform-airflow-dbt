with src as (
  select
    order_id::int as order_id,
    customer_id::int as customer_id,
    order_date::date as order_date,
    amount::numeric(18,2) as amount
  from {{ ref('raw_orders') }}
)

select * from src
