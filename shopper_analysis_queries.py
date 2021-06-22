cy_brand = """
select
*
from {segment_table}
"""
py_brand = """
select
*
from {segment_py_table}
"""

cy_cat = """
select
*
from {category_table}
"""
py_cat = """
select
*
from {category_py_table}
"""

cy_total_pop = """
select
count(distinct customer_id) as total
from {active_table}
"""

py_total_pop = """
select
count(distinct customer_id) as total
from {active_py_table}
"""

basket_data = """
select
f.customer_id,
f.receipt_id,
f.brand_id,
f.global_product_id,
f.receipt_item_id,
f.secondary_category_id,
case when ri.ext_price is not null and ri.quantity > 1 then ri.ext_price
when ri.ext_price is null then ri.price
else ri.ext_price
end as "price"
from vw_fact_customer_receipt_item_procuct_details f
inner join {segment_table} s on f.receipt_id = s.receipt_id
where
((ri.ext_price is not null and ri.quantity > 1 and ri.ext_price > 0 and ri.ext_price < 50) or (ri.ext_price is null and ri.price > 0 and ri.price < 50) or (ri.ext_price > 0 and ri.ext_price < 50))
and f.verified is null
"""

brand_names = """
select
distinct name as brand_name,
id as brand_id
from vw_brands
"""
product_names = """
select
distinct name as product_name,
id as global_product_id
from vw_global_products
"""

category_names = """
select
distinct id as secondary_category_id,
name as category_name
from vw_product_categories
"""

cat_brand_trip = """
with total as(
select
count(distinct receipt_id) as total
from {category_table})
select distinct b.name as brand_name,
count(distinct f.receipt_id)/t.total as 'Category Benchmark'
from vw_fact_customer_receipt_item_product_details f,total t
join vw_brands b on b.id = f.brand_id
where
f.receipt_id in (select distinct receipt_id
from {category_table})
and f.verified is null
group by 1
order by 2 desc
"""

cat_category_trip = """
with total as(
select
count(distinct receipt_id) as total
from {category_table})
select distinct b.name as category_name,
count(distinct f.receipt_id)/t.total as 'Category Benchmark'
from vw_fact_customer_receipt_item_product_details f, total t
join vw_product_categories b on b.id = f.secondary_category_id
where
f.receipt_id in (select distinct receipt_id
from {category_table})
and f.verified is null
group by 1
order by 2 desc
"""

cat_product_trip = """
with total as(
select
count(distinct receipt_id) as total
from {category_table})
select distinct b.name as product_name,
count(distinct f.receipt_id)/t.total as 'Category Benchmark'
from vw_fact_customer_receipt_item_product_details f,total t
join vw_global_products b on b.id = f.global_product_id
where
f.receipt_id in (select distinct receipt_id
from {category_table})
and f.verified is null
group by 1
order by 2 desc
"""
