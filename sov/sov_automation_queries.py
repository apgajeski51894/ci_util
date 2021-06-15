incrementality="""
with active as(
  SELECT
  customer_id,
  sum(price) as cy_spend
  from {segment_table}
  WHERE
  true
  and segment = 'brand'
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id not in(
	select
	customer_id
	from {segment_py_table})
  group by 1),
pre_volume as(
  SELECT
  g.customer_id,
  sum(c.price) as pre_cat_volume
  from {category_py_table} c
  inner join active g on c.customer_id = g.customer_id
  WHERE
  c.receipt_created_at >= date'{pre_date}'
  group by 1),
post_volume as(
  SELECT
  g.customer_id,
  sum(c.price) as post_cat_volume
  from {category_table} c
  inner join active g on c.customer_id = g.customer_id
  WHERE
  c.receipt_item_id not in(
    SELECT
    receipt_item_id
    from {segment_table})
  group by 1),
product as(
  SELECT
  customer_id,
  cy_spend as product_spend
  from active),
volume_change as(
  SELECT
  v.customer_id,
  coalesce(pr.pre_cat_volume,0) as pre_cat_volume,
  coalesce(po.post_cat_volume,0) as post_cat_volume,
  (coalesce(po.post_cat_volume,0) - coalesce(pr.pre_cat_volume,0)) as change_volume,
  v.product_spend
  from product v
  full join post_volume po on v.customer_id = po.customer_id
  full join pre_volume pr on v.customer_id = pr.customer_id),
sourcerer as(
  select
  customer_id,
  product_spend,
  (case when change_volume >= product_spend and pre_cat_volume > 0 then product_spend end) as incremental,
  (case when change_volume < product_spend and change_volume > 0 and pre_cat_volume > 0 then change_volume end) as incremental1,
  (case when change_volume < product_spend and change_volume > 0 and pre_cat_volume > 0 then (product_spend-change_volume) end) as shifting,
  (case when change_volume <= 0 and pre_cat_volume > 0 then product_spend end) as shifting1,
  (case when (pre_cat_volume = 0 or pre_cat_volume is null) then product_spend end) as new_category
  from volume_change),
incremental as(
  select
  'incremental' as source,
  (sum(incremental)+sum(incremental1)) as dollars,
  ((sum(incremental)+sum(incremental1)) / sum(product_spend)) as volume_share
  from sourcerer
  group by 1),
shifting as(
  select
  'shifting' as source,
  (sum(shifting)+sum(shifting1)) as dollars,
  ((sum(shifting)+sum(shifting1)) / sum(product_spend)) as volume_share
  from sourcerer
  group by 1),
new_cat as(
  select
  'new_category' as source,
  sum(new_category) as dollars,
  (sum(new_category) / sum(product_spend)) as volume_share
  from sourcerer
  group by 1)
select
source,
dollars,
volume_share
from incremental
union
select
source,
dollars,
volume_share
from shifting
union
select
source,
dollars,
volume_share
from new_cat
"""

brand_cannibalization = """
with active as(
  SELECT
  customer_id,
  sum(price) as product_spend
  from {segment_table}
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id not in(
    SELECT
    customer_id
    from {segment_py_table}
    WHERE
    true
    and receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    WHERE
    receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    where
    {brand_filter}
    and receipt_created_at >= date'{pre_date}')
  group by 1),
pre_volume as(
  SELECT
  a.customer_id,
  sum(c.price) as pre_cat_volume
  from {category_py_table} c
  inner join active a on c.customer_id = a.customer_id
  WHERE
  c.{brand_filter}
  and c.receipt_created_at >= date'{pre_date}'
  group by 1),
post_volume as(
  SELECT
  a.customer_id,
  sum(c.price) as post_cat_volume
  from {category_table} c
  inner join active a on c.customer_id = a.customer_id
  WHERE
  c.receipt_item_id not in(
    SELECT
    receipt_item_id
    from {segment_table})
  and c.{brand_filter}
  group by 1),
volume_change as(
  SELECT
  a.customer_id,
  coalesce(pr.pre_cat_volume,0) as pre_cat_volume,
  coalesce(po.post_cat_volume,0) as post_cat_volume,
  (coalesce(po.post_cat_volume,0) - coalesce(pr.pre_cat_volume,0)) as change_volume,
  a.product_spend
  from active a
  full join post_volume po on a.customer_id = po.customer_id
  full join pre_volume pr on a.customer_id = pr.customer_id),
sourcerer as(
  select
  customer_id,
  product_spend,
  (case when change_volume >= product_spend then product_spend end) as incremental,
  (case when change_volume < product_spend and change_volume > 0 then change_volume end) as incremental1,
  (case when change_volume < product_spend and change_volume > 0 then (product_spend-change_volume) end) as cannibalizing,
  (case when change_volume <= 0 then product_spend end) as cannibalizing1
  from volume_change
  group by 1,2,3,4,5,6),
incremental as(
  select
  'incremental' as source,
  (sum(incremental)+sum(incremental1)) as dollars,
  ((sum(incremental)+sum(incremental1)) / sum(product_spend)) as volume_share
  from sourcerer
  group by 1),
cannibalizing as(
  select
  'cannibalizing' as source,
  (sum(cannibalizing)+sum(cannibalizing1)) as dollars,
  ((sum(cannibalizing)+sum(cannibalizing1)) / sum(product_spend)) as volume_share
  from sourcerer
  group by 1)
select
source,
dollars,
volume_share
from incremental
union
select
source,
dollars,
volume_share
from cannibalizing
"""

sourcing ="""
with active as(
  SELECT
  customer_id,
  sum(price) as "product_spend"
  from {segment_table}
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  group by 1),
groupings as(
  SELECT
  case WHEN customer_id in (select customer_id from {segment_py_table} where receipt_created_at >= date'{pre_date}') then 'Previous Product Buyers'
  WHEN customer_id not in (select customer_id from {category_py_table} where receipt_created_at >= date'{pre_date}') then 'New Category Buyers'
  WHEN (customer_id in (select customer_id from {category_py_table} where receipt_created_at >= date'{pre_date}')
    and customer_id not in(select customer_id from {category_py_table} where {brand_filter} and receipt_created_at >= date'{pre_date}'))then 'New Brand Buyers'
  WHEN (customer_id in(select customer_id from {category_py_table} where {brand_filter} and receipt_created_at >= date'{pre_date}')) then 'Previous Brand New Product Buyers'
    end as "segment",
  cast(count(distinct customer_id) as double) as "segment_count",
  sum(product_spend) as "product_spend"
  from active
  group by 1),
total_buyers as(
  SELECT
  cast(count(distinct customer_id) as double) as total
  from active),
total_spend as(
  select
  sum(product_spend) as "volume"
  from active)
SELECT
s.segment,
s.segment_count as "Customer Count",
(s.segment_count / t.total) as "Share of Purchasers",
s.product_spend as "Product Volume",
(s.product_spend / ts.volume) as "Share of Volume"
from groupings s, total_buyers t, total_spend ts
"""

product_sourcing="""
with active as(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id not in(
    SELECT
    customer_id
    from {segment_py_table}
    WHERE
    True
    and receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    WHERE
    receipt_created_at >= date'{pre_date}')),
pre_volume as(
  SELECT
  C.global_product_name,
  cast(sum(c.price) as double) as pre_cat_volume
  from {category_py_table} c
  inner join active a on c.customer_id = a.customer_id
  where
  c.receipt_created_at >= date'{pre_date}'
  and c.{brand_filter}
  group by 1),
post_volume as(
  SELECT
  c.global_product_name,
  cast(sum(c.price) as double) as post_cat_volume
  from {category_table} c
  inner join active a on c.customer_id = a.customer_id
  where c.receipt_item_id not in(
    SELECT
    receipt_item_id
    from {segment_table})
  and c.{brand_filter}
  group by 1),
total_spend as(
  select
  cast(sum(post.post_cat_volume) as double) as post_total,
  cast(sum(pre.pre_cat_volume) as double) as pre_total
  from post_volume post, pre_volume pre),
share as(
  SELECT
  a.global_product_name,
  coalesce(pr.pre_cat_volume,0) as pre_cat_volume,
  (coalesce(pr.pre_cat_volume,0) / t.pre_total) as pre_share,
  coalesce(po.post_cat_volume,0) as post_cat_volume,
  (coalesce(po.post_cat_volume,0) / t.post_total) as post_share,
  ((coalesce(po.post_cat_volume,0) / t.post_total)-(coalesce(pr.pre_cat_volume,0) / t.pre_total)) as share_change
  from total_spend t,(select global_product_name from post_volume union select global_product_name from pre_volume) a
  full join post_volume po on a.global_product_name = po.global_product_name
  full join pre_volume pr on a.global_product_name = pr.global_product_name),
total_lost as(
  SELECT
  sum(share_change) as change
  from share
  where
  share_change < 0),
source as(
  SELECT
  s.global_product_name,
  s.pre_cat_volume,
  s.post_cat_volume,
  s.pre_share,
  s.post_share,
  s.share_change,
  (s.share_change / t.change) as change
  from share s, total_lost t)
SELECT
global_product_name as "Product",
pre_cat_volume as "Pre Brand Volume",
post_cat_volume as "Post Brand Volume",
pre_share as "Pre Brand Share",
post_share as "Post Brand Share",
share_change as "Change in Share",
case when change < 0 then 0 else change end as "Stolen Volume"
from source
"""

total_brand_source = """
with active as(
  SELECT
  customer_id,
  'Prodct of Interest' as brand_name,
  sum(price) as post_cat_volume
  from {segment_table}
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id in(
    select customer_id
    from {category_py_table}
    where
    receipt_created_at >= date'{pre_date}')
  and customer_id not in(
    SELECT
    customer_id
    from {segment_py_table}
    WHERE
    true
    and receipt_created_at >= date'{pre_date}')
  group by 1,2),
pre_volume as(
  SELECT
  C.brand_name,
  cast(sum(c.price) as double) as pre_cat_volume
  from {category_py_table} c
  inner join active a on c.customer_id = a.customer_id
  WHERE
  c.receipt_created_at >= date'{pre_date}'
  group by 1),
post as(
  SELECT
  c.brand_name,
  cast(sum(c.price) as double) as post_cat_volume
  from {category_table} c
  inner join active a on c.customer_id = a.customer_id
  where c.receipt_item_id not in(
    SELECT
    receipt_item_id
    from {segment_table})
  group by 1),
post_volume as(
  SELECT
  brand_name,
  post_cat_volume
  from (select brand_name,post_cat_volume from post union select brand_name,sum(post_cat_volume) as post_cat_volume from active group by 1)),
total_spend as(
  select
  cast(sum(post.post_cat_volume) as double) as post_total,
  cast(sum(pre.pre_cat_volume) as double) as pre_total
  from post_volume post, pre_volume pre),
share as(
  SELECT
  a.brand_name,
  coalesce(pr.pre_cat_volume,0) as pre_cat_volume,
  (coalesce(pr.pre_cat_volume,0) / t.pre_total) as pre_share,
  coalesce(po.post_cat_volume,0) as post_cat_volume,
  (coalesce(po.post_cat_volume,0) / t.post_total) as post_share,
  ((coalesce(po.post_cat_volume,0) / t.post_total)-(coalesce(pr.pre_cat_volume,0) / t.pre_total)) as share_change
  from total_spend t,(select brand_name from post_volume union select brand_name from pre_volume) a
  full join post_volume po on a.brand_name = po.brand_name
  full join pre_volume pr on a.brand_name = pr.brand_name),
total_lost as(
  SELECT
  sum(share_change) as change
  from share
  where
  share_change < 0),
source as(
  SELECT
  s.brand_name,
  s.pre_cat_volume,
  s.post_cat_volume,
  s.pre_share,
  s.post_share,
  s.share_change,
  (s.share_change / t.change) as change
  from share s, total_lost t)
SELECT
brand_name as "Brand",
pre_cat_volume as "Pre Brand Volume",
post_cat_volume as "Post Brand Volume",
pre_share as "Pre Brand Share",
post_share as "Post Brand Share",
share_change as "Change in Share",
case when change < 0 then 0 else change end as "Stolen Volume"
from source
"""

new_brand_incrementality = """
with active as(
  SELECT
  customer_id,
  sum(price) as product_spend
  from {segment_table}
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id not in(
    SELECT
    customer_id
    from {segment_py_table}
    WHERE
    true
    and receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    where
    receipt_created_at >= date'{pre_date}')
  and customer_id not in(
    select customer_id
    from {category_py_table}
    where
    {brand_filter}
    and receipt_created_at >= date'{pre_date}')
  group by 1),
pre_volume as(
  SELECT
  a.customer_id,
  sum(c.price) as pre_cat_volume
  from {category_py_table} c
  inner join active a on c.customer_id = a.customer_id
  WHERE
  c.receipt_created_at >= date'{pre_date}'
  group by 1),
post_volume as(
  SELECT
  a.customer_id,
  sum(c.price) as post_cat_volume
  from {category_table} c
  inner join active a on c.customer_id = a.customer_id
  group by 1),
volume_change as(
  SELECT
  a.customer_id,
  pr.pre_cat_volume,
  po.post_cat_volume,
  (po.post_cat_volume - pr.pre_cat_volume) as change_volume,
  a.product_spend
  from active a
  full join post_volume po on a.customer_id = po.customer_id
  full join pre_volume pr on a.customer_id = pr.customer_id),
sourcerer as(
  select
  customer_id,
  product_spend,
  (case when change_volume >= product_spend then product_spend end) as incremental,
  (case when change_volume < product_spend and change_volume > 0 then change_volume end) as incremental1,
  (case when change_volume < product_spend and change_volume > 0 then (product_spend-change_volume) end) as shifting,
  (case when change_volume <= 0 then product_spend end) as shifting1
  from volume_change
  group by 1,2,3,4,5,6),
incremental as(
  select
  'incremental' as source,
  (sum(incremental)+sum(incremental1)) as dollars,
  ((sum(incremental)+sum(incremental1)) / sum(product_spend)) as volume_share
  from sourcerer
  group by 1),
shifting as(
  select
  'shifting' as source,
  (sum(shifting)+sum(shifting1)) as dollars,
  ((sum(shifting)+sum(shifting1)) / sum(product_spend)) as volume_share
  from sourcerer
  group by 1)
select
source,
dollars,
volume_share
from incremental
union
select
source,
dollars,
volume_share
from shifting
"""

new_brand_source = """
with active as(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id not in(
    SELECT
    customer_id
    from {segment_py_table}
    WHERE
    true
    and receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    where
    receipt_created_at >= date'{pre_date}')
  and customer_id not in(
    select customer_id
    from {category_py_table}
    where
    {brand_filter}
    and receipt_created_at >= date'{pre_date}')),
pre_volume as(
  SELECT
  C.brand_name,
  cast(sum(c.price) as double) as pre_cat_volume
  from {category_py_table} c
  inner join active a on c.customer_id = a.customer_id
  WHERE
  c.receipt_created_at >= date'{pre_date}'
  group by 1),
post_volume as(
  SELECT
  c.brand_name,
  cast(sum(c.price) as double) as post_cat_volume
  from {category_table} c
  inner join active a on c.customer_id = a.customer_id
  group by 1),
total_spend as(
  select
  cast(sum(post.post_cat_volume) as double) as post_total,
  cast(sum(pre.pre_cat_volume) as double) as pre_total
  from post_volume post, pre_volume pre),
share as(
  SELECT
  a.brand_name,
  coalesce(pr.pre_cat_volume,0) as pre_cat_volume,
  (coalesce(pr.pre_cat_volume,0) / t.pre_total) as pre_share,
  coalesce(po.post_cat_volume,0) as post_cat_volume,
  (coalesce(po.post_cat_volume,0) / t.post_total) as post_share,
  ((coalesce(po.post_cat_volume,0) / t.post_total)-(pr.pre_cat_volume / t.pre_total)) as share_change
  from total_spend t,(select brand_name from post_volume union select brand_name from pre_volume) a
  full join post_volume po on a.brand_name = po.brand_name
  full join pre_volume pr on a.brand_name = pr.brand_name),
total_lost as(
  SELECT
  sum(share_change) as change
  from share
  where
  share_change < 0
)
SELECT
s.brand_name as "Brand",
s.pre_cat_volume as "Pre Brand Volume",
s.post_cat_volume as "Post Brand Volume",
s.pre_share as "Pre Brand Share",
s.post_share as "Post Brand Share",
s.share_change as "Change in Share",
(s.share_change / t.change) as "Stolen Volume"
from share s, total_lost t
WHERE
share_change < 0
order by 7 desc
"""

previous_brand_incrementality = """
with active as(
  SELECT
  customer_id,
  sum(price) as product_spend
  from {segment_table}
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id not in(
    SELECT
    customer_id
    from {segment_py_table}
    WHERE
    true
    and receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    where
    receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    where
    {brand_filter}
    and receipt_created_at >= date'{pre_date}')
  group by 1),
pre_volume as(
  SELECT
  a.customer_id,
  sum(c.price) as pre_cat_volume
  from {category_py_table} c
  inner join active a on c.customer_id = a.customer_id
  WHERE
  c.receipt_created_at >= date'{pre_date}'
  group by 1),
post_volume as(
  SELECT
  a.customer_id,
  sum(c.price) as post_cat_volume
  from {category_table} c
  inner join active a on c.customer_id = a.customer_id
  group by 1),
volume_change as(
  SELECT
  a.customer_id,
  pr.pre_cat_volume,
  po.post_cat_volume,
  (po.post_cat_volume - pr.pre_cat_volume) as change_volume,
  a.product_spend
  from active a
  full join post_volume po on a.customer_id = po.customer_id
  full join pre_volume pr on a.customer_id = pr.customer_id),
sourcerer as(
  select
  customer_id,
  product_spend,
  (case when change_volume >= product_spend then product_spend end) as incremental,
  (case when change_volume < product_spend and change_volume > 0 then change_volume end) as incremental1,
  (case when change_volume < product_spend and change_volume > 0 then (product_spend-change_volume) end) as shifting,
  (case when change_volume <= 0 then product_spend end) as shifting1
  from volume_change
  group by 1,2,3,4,5,6),
incremental as(
  select
  'incremental' as source,
  (sum(incremental)+sum(incremental1)) as dollars,
  ((sum(incremental)+sum(incremental1)) / sum(product_spend)) as volume_share
  from sourcerer
  group by 1),
shifting as(
  select
  'shifting' as source,
  (sum(shifting)+sum(shifting1)) as dollars,
  ((sum(shifting)+sum(shifting1)) / sum(product_spend)) as volume_share
  from sourcerer
  group by 1)
select
source,
dollars,
volume_share
from incremental
union
select
source,
dollars,
volume_share
from shifting
"""

previous_brand_sourcing = """
with active as(
  SELECT
  customer_id,
  'Prodct of Interest' as brand_name,
  sum(price) as post_cat_volume
  from {segment_table}
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id not in(
    SELECT
    customer_id
    from {segment_py_table}
    WHERE
    true
    and receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    where
    receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    where
    {brand_filter}
    and receipt_created_at >= date'{pre_date}')
  group by 1,2),
pre_volume as(
  SELECT
  C.brand_name,
  cast(sum(c.price) as double) as pre_cat_volume
  from {category_py_table} c
  inner join active a on c.customer_id = a.customer_id
  WHERE
  c.receipt_created_at >= date'{pre_date}'
  group by 1),
post as(
  SELECT
  c.brand_name,
  cast(sum(c.price) as double) as post_cat_volume
  from {category_table} c
  inner join active a on c.customer_id = a.customer_id
  where c.receipt_item_id not in(
    SELECT
    receipt_item_id
    from {segment_table})
  group by 1),
post_volume as(
  SELECT
  brand_name,
  post_cat_volume
  from (select brand_name,post_cat_volume from post union select brand_name,sum(post_cat_volume) as post_cat_volume from active group by 1)),
total_spend as(
  select
  cast(sum(post.post_cat_volume) as double) as post_total,
  cast(sum(pre.pre_cat_volume) as double) as pre_total
  from post_volume post, pre_volume pre),
share as(
  SELECT
  a.brand_name,
  coalesce(pr.pre_cat_volume,0) as pre_cat_volume,
  (coalesce(pr.pre_cat_volume,0) / t.pre_total) as pre_share,
  coalesce(po.post_cat_volume,0) as post_cat_volume,
  (coalesce(po.post_cat_volume,0) / t.post_total) as post_share,
  ((coalesce(po.post_cat_volume,0) / t.post_total)-(coalesce(pr.pre_cat_volume,0) / t.pre_total)) as share_change
  from total_spend t,(select brand_name from post_volume union select brand_name from pre_volume) a
  full join post_volume po on a.brand_name = po.brand_name
  full join pre_volume pr on a.brand_name = pr.brand_name),
total_lost as(
  SELECT
  sum(share_change) as change
  from share
  where
  share_change < 0),
source as(
  SELECT
  s.brand_name,
  s.pre_cat_volume,
  s.post_cat_volume,
  s.pre_share,
  s.post_share,
  s.share_change,
  (s.share_change / t.change) as change
  from share s, total_lost t)
SELECT
brand_name as "Brand",
pre_cat_volume as "Pre Brand Volume",
post_cat_volume as "Post Brand Volume",
pre_share as "Pre Brand Share",
post_share as "Post Brand Share",
share_change as "Change in Share",
case when change < 0 then 0 else change end as "Stolen Volume"
from source
"""

previous_brand_product_cannibalization="""
with active as(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id not in(
    SELECT
    customer_id
    from {segment_py_table}
    WHERE
    true
    and receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    WHERE
    receipt_created_at >= date'{pre_date}')
  and customer_id in(
    select customer_id
    from {category_py_table}
    where
    {brand_filter}
    and receipt_created_at >= date'{pre_date}')),
pre_volume as(
  SELECT
  C.global_product_name,
  cast(sum(c.price) as double) as pre_cat_volume
  from {category_py_table} c
  inner join active a on c.customer_id = a.customer_id
  where
  c.receipt_created_at >= date'{pre_date}'
  and c.{brand_filter}
  group by 1),
post_volume as(
  SELECT
  c.global_product_name,
  cast(sum(c.price) as double) as post_cat_volume
  from {category_table} c
  inner join active a on c.customer_id = a.customer_id
  where c.receipt_item_id not in(
    SELECT
    receipt_item_id
    from {segment_table})
  and c.{brand_filter}
  group by 1),
total_spend as(
  select
  cast(sum(post.post_cat_volume) as double) as post_total,
  cast(sum(pre.pre_cat_volume) as double) as pre_total
  from post_volume post, pre_volume pre),
share as(
  SELECT
  a.global_product_name,
  coalesce(pr.pre_cat_volume,0) as pre_cat_volume,
  (coalesce(pr.pre_cat_volume,0) / t.pre_total) as pre_share,
  coalesce(po.post_cat_volume,0) as post_cat_volume,
  (coalesce(po.post_cat_volume,0) / t.post_total) as post_share,
  ((coalesce(po.post_cat_volume,0) / t.post_total)-(coalesce(pr.pre_cat_volume,0) / t.pre_total)) as share_change
  from total_spend t,(select global_product_name from post_volume union select global_product_name from pre_volume) a
  full join post_volume po on a.global_product_name = po.global_product_name
  full join pre_volume pr on a.global_product_name = pr.global_product_name),
total_lost as(
  SELECT
  sum(share_change) as change
  from share
  where
  share_change < 0),
source as(
  SELECT
  s.global_product_name,
  s.pre_cat_volume,
  s.post_cat_volume,
  s.pre_share,
  s.post_share,
  s.share_change,
  (s.share_change / t.change) as change
  from share s, total_lost t)
SELECT
global_product_name as "Product",
pre_cat_volume as "Pre Brand Volume",
post_cat_volume as "Post Brand Volume",
pre_share as "Pre Brand Share",
post_share as "Post Brand Share",
share_change as "Change in Share",
case when change < 0 then 0 else change end as "Stolen Volume"
from source
"""
