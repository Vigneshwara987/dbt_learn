select productid, productname, category, subcategory, sum(profit) as profit
from {{ ref("stg_orders") }}
group by productid, productname, category, subcategory
