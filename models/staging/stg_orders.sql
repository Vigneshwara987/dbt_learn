
{{
    config(
        materialized='table'
    )
}}

select {{ dbt_utils.generate_surrogate_key(['customers.customerid', 'products.productid','orders.orderid']) }} as sk_orders,
       customers.customerid, 
       customers.customername,
       customers.segment,
       customers.country,
       products.productid,
       products.category,       
       products.productname,
       products.subcategory,
       orders.orderid,
       orders.orderdate,
       orders.shipdate,
       orders.shipmode,
       (ordersellingprice - ordercostprice)  as profit ,
       delivery_teams.delivery_team
from 
{{ ref('raw_orders') }}  orders 
left outer join  {{ ref('raw_customers') }} customers 
on orders.customerid = customers.customerid
left outer join  {{ ref('raw_products') }} products 
on orders.productid = products.productid 
left outer join  {{ ref('delivery_teams') }} delivery_teams 
on orders.shipmode = delivery_teams.shipmode 
