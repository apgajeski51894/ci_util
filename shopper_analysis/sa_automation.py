import os
import ba_tools
import numpy as np
import pandas as pd
from ds_util.qubole import run_hive, run_presto
from ba_tools.utils import dump_to_excel
from datetime import datetime, timedelta
from functools import lru_cache
import sa_automation_queries as sql
from ba_tools.utils import id_input_validation
import re
import ssl
from itertools import compress


@lru_cache(maxsize=35)
def exec_presto(query, return_data=True, cluster='presto-consumer-insights-dev', **kwargs):
    return run_presto(query=query, return_data=return_data, na_values='\\N')

def category_purchases(**params):

    cat_category = exec_presto(sql.cat_category.format(**params))
    seg1_category = exec_presto(sql.seg1_category.format(**params))
    seg2_category = exec_presto(sql.seg2_category.format(**params))
    seg3_category = exec_presto(sql.seg3_category.format(**params))
    seg4_category = exec_presto(sql.seg4_category.format(**params))
    seg5_category = exec_presto(sql.seg5_category.format(**params))
    seg6_category = exec_presto(sql.seg6_category.format(**params))

    category_purchasing = cat_category.merge(seg1_category,on='category',how='outer').merge(seg2_category,on='category',how='outer').merge(seg3_category,on='category',how='outer').merge(seg4_category,on='category',how='outer').merge(seg5_category,on='category',how='outer').merge(seg6_category,on='category',how='outer')
    category_purchasing['segment1_index'] = (category_purchasing['segment1'] / category_purchasing['total_category'])
    category_purchasing['segment2_index'] = (category_purchasing['segment2'] / category_purchasing['total_category'])
    category_purchasing['segment3_index'] = (category_purchasing['segment3'] / category_purchasing['total_category'])
    category_purchasing['segment4_index'] = (category_purchasing['segment4'] / category_purchasing['total_category'])
    category_purchasing['segment5_index'] = (category_purchasing['segment5'] / category_purchasing['total_category'])
    category_purchasing['segment6_index'] = (category_purchasing['segment6'] / category_purchasing['total_category'])

    return category_purchasing

def category_loyalty(**params):
    segment_buy_rate_cy = exec_presto(sql.segment_buy_rate_cy_query.format(**params))
    segment_buy_rate_py = exec_presto(sql.segment_buy_rate_py_query.format(**params))
    segment_buy_rate = segment_buy_rate_py.merge(segment_buy_rate_cy,on='segment',how='inner')
    segment_buy_rate['change_in_avg_seg_buy_rate'] = ((segment_buy_rate['segment_average_buy_rate_cy'] - segment_buy_rate['segment_average_buy_rate_py']) / segment_buy_rate['segment_average_buy_rate_py'])
    segment_buy_rate['change_in_median_seg_buy_rate'] = ((segment_buy_rate['segment_median_buy_rate_cy'] - segment_buy_rate['segment_median_buy_rate_py']) / segment_buy_rate['segment_median_buy_rate_py'])

    segment_units_cy = exec_presto(sql.segment_units_cy_query.format(**params))
    segment_units_py = exec_presto(sql.segment_units_py_query.format(**params))
    segment_units = segment_units_py.merge(segment_units_cy,on='segment',how='inner')
    segment_units['change_in_avg_seg_units'] = ((segment_units['segment_average_units_cy'] - segment_units['segment_average_units_py']) / segment_units['segment_average_units_py'])
    segment_units['change_in_median_seg_units'] = ((segment_units['segment_median_units_cy'] - segment_units['segment_median_units_py']) / segment_units['segment_median_units_py'])

    category_buy_rate_cy = exec_presto(sql.category_buy_rate_cy_query.format(**params))
    category_buy_rate_py = exec_presto(sql.category_buy_rate_py_query.format(**params))
    category_buy_rate = category_buy_rate_py.merge(category_buy_rate_cy,on='segment',how='inner')
    category_buy_rate['change_in_avg_category_buy_rate'] = ((category_buy_rate['category_average_buy_rate_cy'] - category_buy_rate['category_average_buy_rate_py']) / category_buy_rate['category_average_buy_rate_py'])
    category_buy_rate['change_in_median_category_buy_rate'] = ((category_buy_rate['category_median_buy_rate_cy'] - category_buy_rate['category_median_buy_rate_py']) / category_buy_rate['category_median_buy_rate_py'])
    category_units_cy = exec_presto(sql.category_units_cy_query.format(**params))
    category_units_py = exec_presto(sql.category_units_py_query.format(**params))
    category_units = category_units_py.merge(category_units_cy,on='segment',how='inner')
    category_units['change_in_avg_category_units'] = ((category_units['category_average_units_cy'] - category_units['category_average_units_py']) / category_units['category_average_units_py'])
    category_units['change_in_median_category_units'] = ((category_units['category_median_units_cy'] - category_units['category_median_units_py']) / category_units['category_median_units_py'])

    category_buy_rate = segment_buy_rate.merge(category_buy_rate,on='segment',how='inner')
    category_buy_rate['category_average_share_of_wallet_cy'] = (category_buy_rate['segment_average_buy_rate_cy'] / category_buy_rate['category_average_buy_rate_cy'])
    category_buy_rate['category_average_share_of_wallet_py'] = (category_buy_rate['segment_average_buy_rate_py'] / category_buy_rate['category_average_buy_rate_py'])
    category_buy_rate['category_median_share_of_wallet_cy'] = (category_buy_rate['segment_median_buy_rate_cy'] / category_buy_rate['category_median_buy_rate_cy'])
    category_buy_rate['category_median_share_of_wallet_py'] = (category_buy_rate['segment_median_buy_rate_py'] / category_buy_rate['category_median_buy_rate_py'])
    category_buy_rate['change_in_category_average_share_of_wallet'] = ((category_buy_rate['category_average_share_of_wallet_cy'] - category_buy_rate['category_average_share_of_wallet_py']) / category_buy_rate['category_average_share_of_wallet_py'])
    category_buy_rate['change_in_category_median_share_of_wallet'] = ((category_buy_rate['category_median_share_of_wallet_cy'] - category_buy_rate['category_median_share_of_wallet_py']) / category_buy_rate['category_median_share_of_wallet_py'])

    category_units = segment_units.merge(category_units,on='segment',how='inner')
    category_units['category_average_share_of_requirements_cy'] = (category_units['segment_average_units_cy'] / category_units['category_average_units_cy'])
    category_units['category_average_share_of_requirements_py'] = (category_units['segment_average_units_py'] / category_units['category_average_units_py'])
    category_units['category_median_share_of_requirements_cy'] = (category_units['segment_median_units_cy'] / category_units['category_median_units_cy'])
    category_units['category_median_share_of_requirements_py'] = (category_units['segment_median_units_py'] / category_units['category_median_units_py'])
    category_units['change_in_average_category_share_of_requirements'] = ((category_units['category_average_share_of_requirements_cy'] - category_units['category_average_share_of_requirements_py']) / category_units['category_average_share_of_requirements_py'])
    category_units['change_in_median_category_share_of_requirements'] = ((category_units['category_median_share_of_requirements_cy'] - category_units['category_median_share_of_requirements_py']) / category_units['category_median_share_of_requirements_py'])

    category_brand_loyalty = category_buy_rate.merge(category_units,on='segment',how='inner')

    return category_brand_loyalty

def sub_category_loyalty(**params):
    segment_buy_rate_cy = exec_presto(sql.segment_buy_rate_cy_query.format(**params))
    segment_buy_rate_py = exec_presto(sql.segment_buy_rate_py_query.format(**params))
    segment_buy_rate = segment_buy_rate_py.merge(segment_buy_rate_cy,on='segment',how='inner')
    segment_buy_rate['change_in_avg_seg_buy_rate'] = ((segment_buy_rate['segment_average_buy_rate_cy'] - segment_buy_rate['segment_average_buy_rate_py']) / segment_buy_rate['segment_average_buy_rate_py'])
    segment_buy_rate['change_in_median_seg_buy_rate'] = ((segment_buy_rate['segment_median_buy_rate_cy'] - segment_buy_rate['segment_median_buy_rate_py']) / segment_buy_rate['segment_median_buy_rate_py'])

    segment_units_cy = exec_presto(sql.segment_units_cy_query.format(**params))
    segment_units_py = exec_presto(sql.segment_units_py_query.format(**params))
    segment_units = segment_units_py.merge(segment_units_cy,on='segment',how='inner')
    segment_units['change_in_avg_seg_units'] = ((segment_units['segment_average_units_cy'] - segment_units['segment_average_units_py']) / segment_units['segment_average_units_py'])
    segment_units['change_in_median_seg_units'] = ((segment_units['segment_median_units_cy'] - segment_units['segment_median_units_py']) / segment_units['segment_median_units_py'])

    sub_category_buy_rate_cy = exec_presto(sql.sub_category_buy_rate_cy_query.format(**params))
    sub_category_buy_rate_py = exec_presto(sql.sub_category_buy_rate_py_query.format(**params))
    sub_category_buy_rate = sub_category_buy_rate_py.merge(sub_category_buy_rate_cy,on='segment',how='inner')
    sub_category_buy_rate['change_in_avg_sub_category_buy_rate'] = ((sub_category_buy_rate['sub_category_average_buy_rate_cy'] - sub_category_buy_rate['sub_category_average_buy_rate_py']) / sub_category_buy_rate['sub_category_average_buy_rate_py'])
    sub_category_buy_rate['change_in_median_sub_category_buy_rate'] = ((sub_category_buy_rate['sub_category_median_buy_rate_cy'] - sub_category_buy_rate['sub_category_median_buy_rate_py']) / sub_category_buy_rate['sub_category_median_buy_rate_py'])
    sub_category_units_cy = exec_presto(sql.sub_category_units_cy_query.format(**params))
    sub_category_units_py = exec_presto(sql.sub_category_units_py_query.format(**params))
    sub_category_units = sub_category_units_py.merge(sub_category_units_cy,on='segment',how='inner')
    sub_category_units['change_in_avg_sub_category_units'] = ((sub_category_units['sub_category_average_units_cy'] - sub_category_units['sub_category_average_units_py']) / sub_category_units['sub_category_average_units_py'])
    sub_category_units['change_in_median_sub_category_units'] = ((sub_category_units['sub_category_median_units_cy'] - sub_category_units['sub_category_median_units_py']) / sub_category_units['sub_category_median_units_py'])

    sub_category_buy_rate = segment_buy_rate.merge(sub_category_buy_rate,on='segment',how='inner')
    sub_category_buy_rate['sub_category_average_share_of_wallet_cy'] = (sub_category_buy_rate['segment_average_buy_rate_cy'] / sub_category_buy_rate['sub_category_average_buy_rate_cy'])
    sub_category_buy_rate['sub_category_average_share_of_wallet_py'] = (sub_category_buy_rate['segment_average_buy_rate_py'] / sub_category_buy_rate['sub_category_average_buy_rate_py'])
    sub_category_buy_rate['sub_category_median_share_of_wallet_cy'] = (sub_category_buy_rate['segment_median_buy_rate_cy'] / sub_category_buy_rate['sub_category_median_buy_rate_cy'])
    sub_category_buy_rate['sub_category_median_share_of_wallet_py'] = (sub_category_buy_rate['segment_median_buy_rate_py'] / sub_category_buy_rate['sub_category_median_buy_rate_py'])
    sub_category_buy_rate['change_in_sub_category_average_share_of_wallet'] = ((sub_category_buy_rate['sub_category_average_share_of_wallet_cy'] - sub_category_buy_rate['sub_category_average_share_of_wallet_py']) / sub_category_buy_rate['sub_category_average_share_of_wallet_py'])
    sub_category_buy_rate['change_in_sub_category_edian_share_of_wallet'] = ((sub_category_buy_rate['sub_category_median_share_of_wallet_cy'] - sub_category_buy_rate['sub_category_median_share_of_wallet_py']) / sub_category_buy_rate['sub_category_median_share_of_wallet_py'])

    sub_category_units = segment_units.merge(sub_category_units,on='segment',how='inner')
    sub_category_units['sub_category_average_share_of_requirements_cy'] = (sub_category_units['segment_average_units_cy'] / sub_category_units['sub_category_average_units_cy'])
    sub_category_units['sub_category_average_share_of_requirements_py'] = (sub_category_units['segment_average_units_py'] / sub_category_units['sub_category_average_units_py'])
    sub_category_units['sub_category_median_share_of_requirements_cy'] = (sub_category_units['segment_median_units_cy'] / sub_category_units['sub_category_median_units_cy'])
    sub_category_units['sub_category_median_share_of_requirements_py'] = (sub_category_units['segment_median_units_py'] / sub_category_units['sub_category_median_units_py'])
    sub_category_units['change_in_average_sub_category_share_of_requirements'] = ((sub_category_units['sub_category_average_share_of_requirements_cy'] - sub_category_units['sub_category_average_share_of_requirements_py']) / sub_category_units['sub_category_average_share_of_requirements_py'])
    sub_category_units['change_in_median_sub_category_share_of_requirements'] = ((sub_category_units['sub_category_median_share_of_requirements_cy'] - sub_category_units['sub_category_median_share_of_requirements_py']) / sub_category_units['sub_category_median_share_of_requirements_py'])

    sub_category_brand_loyalty = sub_category_buy_rate.merge(sub_category_units,on='segment',how='inner')

    return sub_category_brand_loyalty

def trial_repeat(**params):

    category = exec_presto(sql.category_trial_repeat_query.format(**params))
    category['purchase_time_filled'] = pd.to_datetime(category.receipt_created_at)
    category['delta'] = category.groupby('customer_id').purchase_time_filled.transform(pd.Series.diff)
    category_repurchases = category.groupby('customer_id').receipt_id.count().reset_index().groupby('receipt_id').customer_id.count().reset_index()
    category_repurchases['pct_total_category'] = category_repurchases.customer_id / category_repurchases.customer_id.sum()
    category_repurchases['sub_pct_category'] = category_repurchases['customer_id'] / category_repurchases[category_repurchases.receipt_id >= 2].pct_total_category.sum()

    category_scalars = {
        "pct_repurchases_category": category_repurchases[category_repurchases.receipt_id >= 2].pct_total_category.sum(),
        "mean_days_between_purchasse_category": category.delta[category['delta']>'1 days 00:00:00'].mean().days,
        "median_days_between_purchases_category": category.delta[category['delta']>'1 days 00:00:00'].median().days
    }

    segment1 = exec_presto(sql.segment1_trial_repeat_query.format(**params))
    segment1['purchase_time_filled'] = pd.to_datetime(segment1.receipt_created_at)
    segment1['delta'] = segment1.groupby('customer_id').purchase_time_filled.transform(pd.Series.diff)
    segment1_repurchases = segment1.groupby('customer_id').receipt_id.count().reset_index().groupby('receipt_id').customer_id.count().reset_index()
    segment1_repurchases['pct_total_segment1'] = segment1_repurchases.customer_id / segment1_repurchases.customer_id.sum()
    segment1_repurchases['sub_pct_segment1'] = segment1_repurchases['customer_id'] / segment1_repurchases[segment1_repurchases.receipt_id >= 2].pct_total_segment1.sum()

    segment1_scalars = {
        "pct_repurchases_segment1": segment1_repurchases[segment1_repurchases.receipt_id >= 2].pct_total_segment1.sum(),
        "mean_days_between_purchasse_segment1": segment1.delta[segment1['delta']>'1 days 00:00:00'].mean().days,
        "median_days_between_purchases_segment1": segment1.delta[segment1['delta']>'1 days 00:00:00'].median().days
    }

    segment2 = exec_presto(sql.segment2_trial_repeat_query.format(**params))
    segment2['purchase_time_filled'] = pd.to_datetime(segment2.receipt_created_at)
    segment2['delta'] = segment2.groupby('customer_id').purchase_time_filled.transform(pd.Series.diff)
    segment2_repurchases = segment2.groupby('customer_id').receipt_id.count().reset_index().groupby('receipt_id').customer_id.count().reset_index()
    segment2_repurchases['pct_total_segment2'] = segment2_repurchases.customer_id / segment2_repurchases.customer_id.sum()
    segment2_repurchases['sub_pct_segment2'] = segment2_repurchases['customer_id'] / segment2_repurchases[segment2_repurchases.receipt_id >= 2].pct_total_segment2.sum()

    segment2_scalars = {
        "pct_repurchases_segment2": segment2_repurchases[segment2_repurchases.receipt_id >= 2].pct_total_segment2.sum(),
        "mean_days_between_purchasse_segment2": segment2.delta[segment2['delta']>'1 days 00:00:00'].mean().days,
        "median_days_between_purchases_segment2": segment2.delta[segment2['delta']>'1 days 00:00:00'].median().days
    }

    segment3 = exec_presto(sql.segment3_trial_repeat_query.format(**params))
    segment3['purchase_time_filled'] = pd.to_datetime(segment3.receipt_created_at)
    segment3['delta'] = segment3.groupby('customer_id').purchase_time_filled.transform(pd.Series.diff)
    segment3_repurchases = segment3.groupby('customer_id').receipt_id.count().reset_index().groupby('receipt_id').customer_id.count().reset_index()
    segment3_repurchases['pct_total_segment3'] = segment3_repurchases.customer_id / segment3_repurchases.customer_id.sum()
    segment3_repurchases['sub_pct_segment3'] = segment3_repurchases['customer_id'] / segment3_repurchases[segment3_repurchases.receipt_id >= 2].pct_total_segment3.sum()

    segment3_scalars = {
        "pct_repurchases_segment3": segment3_repurchases[segment3_repurchases.receipt_id >= 2].pct_total_segment3.sum(),
        "mean_days_between_purchasse_segment3": segment3.delta[segment3['delta']>'1 days 00:00:00'].mean().days,
        "median_days_between_purchases_segment3": segment3.delta[segment3['delta']>'1 days 00:00:00'].median().days
    }

    segment4 = exec_presto(sql.segment4_trial_repeat_query.format(**params))
    segment4['purchase_time_filled'] = pd.to_datetime(segment4.receipt_created_at)
    segment4['delta'] = segment4.groupby('customer_id').purchase_time_filled.transform(pd.Series.diff)
    segment4_repurchases = segment4.groupby('customer_id').receipt_id.count().reset_index().groupby('receipt_id').customer_id.count().reset_index()
    segment4_repurchases['pct_total_segment4'] = segment4_repurchases.customer_id / segment4_repurchases.customer_id.sum()
    segment4_repurchases['sub_pct_segment4'] = segment4_repurchases['customer_id'] / segment4_repurchases[segment4_repurchases.receipt_id >= 2].pct_total_segment4.sum()

    segment4_scalars = {
        "pct_repurchases_segment4": segment4_repurchases[segment4_repurchases.receipt_id >= 2].pct_total_segment4.sum(),
        "mean_days_between_purchasse_segment4": segment4.delta[segment4['delta']>'1 days 00:00:00'].mean().days,
        "median_days_between_purchases_segment4": segment4.delta[segment4['delta']>'1 days 00:00:00'].median().days
    }

    segment5 = exec_presto(sql.segment5_trial_repeat_query.format(**params))
    segment5['purchase_time_filled'] = pd.to_datetime(segment5.receipt_created_at)
    segment5['delta'] = segment5.groupby('customer_id').purchase_time_filled.transform(pd.Series.diff)
    segment5_repurchases = segment5.groupby('customer_id').receipt_id.count().reset_index().groupby('receipt_id').customer_id.count().reset_index()
    segment5_repurchases['pct_total_segment5'] = segment5_repurchases.customer_id / segment5_repurchases.customer_id.sum()
    segment5_repurchases['sub_pct_segment5'] = segment5_repurchases['customer_id'] / segment5_repurchases[segment5_repurchases.receipt_id >= 2].pct_total_segment5.sum()

    segment5_scalars = {
        "pct_repurchases_segment5": segment5_repurchases[segment5_repurchases.receipt_id >= 2].pct_total_segment5.sum(),
        "mean_days_between_purchasse_segment5": segment5.delta[segment5['delta']>'1 days 00:00:00'].mean().days,
        "median_days_between_purchases_segment5": segment5.delta[segment5['delta']>'1 days 00:00:00'].median().days
    }

    segment6 = exec_presto(sql.segment6_trial_repeat_query.format(**params))
    segment6['purchase_time_filled'] = pd.to_datetime(segment6.receipt_created_at)
    segment6['delta'] = segment6.groupby('customer_id').purchase_time_filled.transform(pd.Series.diff)
    segment6_repurchases = segment6.groupby('customer_id').receipt_id.count().reset_index().groupby('receipt_id').customer_id.count().reset_index()
    segment6_repurchases['pct_total_segment6'] = segment6_repurchases.customer_id / segment6_repurchases.customer_id.sum()
    segment6_repurchases['sub_pct_segment6'] = segment6_repurchases['customer_id'] / segment6_repurchases[segment6_repurchases.receipt_id >= 2].pct_total_segment6.sum()

    segment6_scalars = {
        "pct_repurchases_segment6": segment6_repurchases[segment6_repurchases.receipt_id >= 2].pct_total_segment6.sum(),
        "mean_days_between_purchasse_segment6": segment6.delta[segment6['delta']>'1 days 00:00:00'].mean().days,
        "median_days_between_purchases_segment6": segment6.delta[segment6['delta']>'1 days 00:00:00'].median().days
    }

    return (category_repurchases,category_scalars,segment1_repurchases,segment1_scalars,segment2_repurchases,segment2_scalars,segment3_repurchases,segment3_scalars,segment4_repurchases,segment4_scalars,segment5_repurchases,segment5_scalars,segment6_repurchases,segment6_scalars)

def category_market_share(**params):
    market_share_category_items_cy = exec_presto(sql.market_share_category_units_cy_query.format(**params))
    market_share_category_items_py = exec_presto(sql.market_share_category_units_py_query.format(**params))
    market_share_category_items = market_share_category_items_py.merge(market_share_category_items_cy, on='segment',how='inner')
    market_share_category_items['change_in_category_unit_market_share'] = ((market_share_category_items['market_share_category_units_cy'] - market_share_category_items['market_share_category_units_py']) / market_share_category_items['market_share_category_units_py'])

    market_share_category_volume_cy = exec_presto(sql.market_share_category_dollars_cy_query.format(**params))
    market_share_category_volume_py = exec_presto(sql.market_share_category_dollars_py_query.format(**params))
    market_share_category_volume = market_share_category_volume_py.merge(market_share_category_volume_cy, on='segment',how='inner')
    market_share_category_volume['change_in_dollar_market_share_category'] = ((market_share_category_volume['market_share_category_dollars_cy'] - market_share_category_volume['market_share_category_dollars_py']) / market_share_category_volume['market_share_category_dollars_py'])

    brand_category_market_share = market_share_category_volume.merge(market_share_category_items,on='segment',how='inner')

    return brand_category_market_share

def sub_category_market_share(**params):
    market_share_sub_category_items_cy = exec_presto(sql.market_share_sub_category_units_cy_query.format(**params))
    market_share_sub_category_items_py = exec_presto(sql.market_share_sub_category_units_py_query.format(**params))
    market_share_sub_category_items = market_share_sub_category_items_py.merge(market_share_sub_category_items_cy, on='segment',how='inner')
    market_share_sub_category_items['change_in_sub_category_unit_market_share'] = ((market_share_sub_category_items['market_share_sub_category_units_cy'] - market_share_sub_category_items['market_share_sub_category_units_py']) / market_share_sub_category_items['market_share_sub_category_units_py'])

    market_share_sub_category_volume_cy = exec_presto(sql.market_share_sub_category_dollars_cy_query.format(**params))
    market_share_sub_category_volume_py = exec_presto(sql.market_share_sub_category_dollars_py_query.format(**params))
    market_share_sub_category_volume = market_share_sub_category_volume_py.merge(market_share_sub_category_volume_cy, on='segment',how='inner')
    market_share_sub_category_volume['change_in_dollar_market_share_sub_category'] = ((market_share_sub_category_volume['market_share_sub_category_dollars_cy'] - market_share_sub_category_volume['market_share_sub_category_dollars_py']) / market_share_sub_category_volume['market_share_sub_category_dollars_py'])

    brand_sub_category_market_share = market_share_sub_category_volume.merge(market_share_sub_category_items,on='segment',how='inner')

    return brand_sub_category_market_share

def brand_overall_purchase_history_category(**params):

    category_brands = exec_presto(sql.category_top_brands_query.format(**params))
    category_total_buyers = exec_presto(sql.category_total_query.format(**params))
    category_brands['%_of_category_shoppers'] = category_brands['category_purchasers'] / np.sum(category_total_buyers['category_total'])
    category_brand_purchase_history = category_brands[['brand_name','%_of_category_shoppers']]

    segment1_brands = exec_presto(sql.segment1_top_brands_query.format(**params))
    segment1_total_buyers = exec_presto(sql.segment1_total_query.format(**params))
    segment1_brands['%_of_segment1_shoppers'] = segment1_brands['segment1_purchasers'] / np.sum(segment1_total_buyers['segment1_total'])
    segment1_brand_purchase_history = segment1_brands[['brand_name','%_of_segment1_shoppers']]

    segment2_brands = exec_presto(sql.segment2_top_brands_query.format(**params))
    segment2_total_buyers = exec_presto(sql.segment2_total_query.format(**params))
    segment2_brands['%_of_segment2_shoppers'] = segment2_brands['segment2_purchasers'] / np.sum(segment2_total_buyers['segment2_total'])
    segment2_brand_purchase_history = segment2_brands[['brand_name','%_of_segment2_shoppers']]

    segment3_brands = exec_presto(sql.segment3_top_brands_query.format(**params))
    segment3_total_buyers = exec_presto(sql.segment3_total_query.format(**params))
    segment3_brands['%_of_segment3_shoppers'] = segment3_brands['segment3_purchasers'] / np.sum(segment3_total_buyers['segment3_total'])
    segment3_brand_purchase_history = segment3_brands[['brand_name','%_of_segment3_shoppers']]

    segment4_brands = exec_presto(sql.segment4_top_brands_query.format(**params))
    segment4_total_buyers = exec_presto(sql.segment4_total_query.format(**params))
    segment4_brands['%_of_segment4_shoppers'] = segment4_brands['segment4_purchasers'] / np.sum(segment4_total_buyers['segment4_total'])
    segment4_brand_purchase_history = segment4_brands[['brand_name','%_of_segment4_shoppers']]

    segment5_brands = exec_presto(sql.segment5_top_brands_query.format(**params))
    segment5_total_buyers = exec_presto(sql.segment5_total_query.format(**params))
    segment5_brands['%_of_segment5_shoppers'] = segment5_brands['segment5_purchasers'] / np.sum(segment5_total_buyers['segment5_total'])
    segment5_brand_purchase_history = segment5_brands[['brand_name','%_of_segment5_shoppers']]

    brand_purchase_history_category = category_brand_purchase_history.merge(segment1_brand_purchase_history,on='brand_name',how='outer').merge(segment2_brand_purchase_history,on='brand_name',how='outer').merge(segment3_brand_purchase_history,on='brand_name',how='outer').merge(segment4_brand_purchase_history,on='brand_name',how='outer').merge(segment5_brand_purchase_history,on='brand_name',how='outer')

    brand_purchase_history_category['segment1_index_category'] = ((brand_purchase_history_category['%_of_segment1_shoppers'] / brand_purchase_history_category['%_of_category_shoppers'])*100)
    brand_purchase_history_category['segment2_index_category'] = ((brand_purchase_history_category['%_of_segment2_shoppers'] / brand_purchase_history_category['%_of_category_shoppers'])*100)
    brand_purchase_history_category['segment3_index_category'] = ((brand_purchase_history_category['%_of_segment3_shoppers'] / brand_purchase_history_category['%_of_category_shoppers'])*100)
    brand_purchase_history_category['segment4_index_category'] = ((brand_purchase_history_category['%_of_segment4_shoppers'] / brand_purchase_history_category['%_of_category_shoppers'])*100)
    brand_purchase_history_category['segment5_index_category'] = ((brand_purchase_history_category['%_of_segment5_shoppers'] / brand_purchase_history_category['%_of_category_shoppers'])*100)

    return brand_purchase_history_category

def brand_overall_purchase_history_sub_category(**params):

    sub_category_brands = exec_presto(sql.sub_category_top_brands_query.format(**params))
    sub_category_total_buyers = exec_presto(sql.sub_category_total_query.format(**params))
    sub_category_brands['%_of_sub_category_shoppers'] = sub_category_brands['sub_category_purchasers'] / np.sum(sub_category_total_buyers['sub_category_total'])
    sub_category_brand_purchase_history = sub_category_brands[['brand_name','%_of_sub_category_shoppers']]

    segment1_brands = exec_presto(sql.segment1_top_brands_query.format(**params))
    segment1_total_buyers = exec_presto(sql.segment1_total_query.format(**params))
    segment1_brands['%_of_segment1_shoppers'] = segment1_brands['segment1_purchasers'] / np.sum(segment1_total_buyers['segment1_total'])
    segment1_brand_purchase_history = segment1_brands[['brand_name','%_of_segment1_shoppers']]

    segment2_brands = exec_presto(sql.segment2_top_brands_query.format(**params))
    segment2_total_buyers = exec_presto(sql.segment2_total_query.format(**params))
    segment2_brands['%_of_segment2_shoppers'] = segment2_brands['segment2_purchasers'] / np.sum(segment2_total_buyers['segment2_total'])
    segment2_brand_purchase_history = segment2_brands[['brand_name','%_of_segment2_shoppers']]

    segment3_brands = exec_presto(sql.segment3_top_brands_query.format(**params))
    segment3_total_buyers = exec_presto(sql.segment3_total_query.format(**params))
    segment3_brands['%_of_segment3_shoppers'] = segment3_brands['segment3_purchasers'] / np.sum(segment3_total_buyers['segment3_total'])
    segment3_brand_purchase_history = segment3_brands[['brand_name','%_of_segment3_shoppers']]

    segment4_brands = exec_presto(sql.segment4_top_brands_query.format(**params))
    segment4_total_buyers = exec_presto(sql.segment4_total_query.format(**params))
    segment4_brands['%_of_segment4_shoppers'] = segment4_brands['segment4_purchasers'] / np.sum(segment4_total_buyers['segment4_total'])
    segment4_brand_purchase_history = segment4_brands[['brand_name','%_of_segment4_shoppers']]

    segment5_brands = exec_presto(sql.segment5_top_brands_query.format(**params))
    segment5_total_buyers = exec_presto(sql.segment5_total_query.format(**params))
    segment5_brands['%_of_segment5_shoppers'] = segment5_brands['segment5_purchasers'] / np.sum(segment5_total_buyers['segment5_total'])
    segment5_brand_purchase_history = segment5_brands[['brand_name','%_of_segment5_shoppers']]

    brand_purchase_history_sub_category = sub_category_brand_purchase_history.merge(segment1_brand_purchase_history,on='brand_name',how='outer').merge(segment2_brand_purchase_history,on='brand_name',how='outer').merge(segment3_brand_purchase_history,on='brand_name',how='outer').merge(segment4_brand_purchase_history,on='brand_name',how='outer').merge(segment5_brand_purchase_history,on='brand_name',how='outer')

    brand_purchase_history_sub_category['segment1_index_sub_category'] = ((brand_purchase_history_sub_category['%_of_segment1_shoppers'] / brand_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)
    brand_purchase_history_sub_category['segment2_index_sub_category'] = ((brand_purchase_history_sub_category['%_of_segment2_shoppers'] / brand_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)
    brand_purchase_history_sub_category['segment3_index_sub_category'] = ((brand_purchase_history_sub_category['%_of_segment3_shoppers'] / brand_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)
    brand_purchase_history_sub_category['segment4_index_sub_category'] = ((brand_purchase_history_sub_category['%_of_segment4_shoppers'] / brand_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)
    brand_purchase_history_sub_category['segment5_index_sub_category'] = ((brand_purchase_history_sub_category['%_of_segment5_shoppers'] / brand_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)

    return brand_purchase_history_sub_category

def category_overall_purchase_history_category(**params):

    category_categories = exec_presto(sql.category_top_categories_query.format(**params))
    category_total_buyers = exec_presto(sql.category_total_query.format(**params))
    category_categories['%_of_category_shoppers'] = category_categories['category_purchasers'] / np.sum(category_total_buyers['category_total'])
    category_category_purchase_history = category_categories[['category_name','%_of_category_shoppers']]

    segment1_categories = exec_presto(sql.segment1_top_categories_query.format(**params))
    segment1_total_buyers = exec_presto(sql.segment1_total_query.format(**params))
    segment1_categories['%_of_segment1_shoppers'] = segment1_categories['segment1_purchasers'] / np.sum(segment1_total_buyers['segment1_total'])
    segment1_category_purchase_history = segment1_categories[['category_name','%_of_segment1_shoppers']]

    segment2_categories = exec_presto(sql.segment2_top_categories_query.format(**params))
    segment2_total_buyers = exec_presto(sql.segment2_total_query.format(**params))
    segment2_categories['%_of_segment2_shoppers'] = segment2_categories['segment2_purchasers'] / np.sum(segment2_total_buyers['segment2_total'])
    segment2_category_purchase_history = segment2_categories[['category_name','%_of_segment2_shoppers']]

    segment3_categories = exec_presto(sql.segment3_top_categories_query.format(**params))
    segment3_total_buyers = exec_presto(sql.segment3_total_query.format(**params))
    segment3_categories['%_of_segment3_shoppers'] = segment3_categories['segment3_purchasers'] / np.sum(segment3_total_buyers['segment3_total'])
    segment3_category_purchase_history = segment3_categories[['category_name','%_of_segment3_shoppers']]

    segment4_categories = exec_presto(sql.segment4_top_categories_query.format(**params))
    segment4_total_buyers = exec_presto(sql.segment4_total_query.format(**params))
    segment4_categories['%_of_segment4_shoppers'] = segment4_categories['segment4_purchasers'] / np.sum(segment4_total_buyers['segment4_total'])
    segment4_category_purchase_history = segment4_categories[['category_name','%_of_segment4_shoppers']]

    segment5_categories = exec_presto(sql.segment5_top_categories_query.format(**params))
    segment5_total_buyers = exec_presto(sql.segment5_total_query.format(**params))
    segment5_categories['%_of_segment5_shoppers'] = segment5_categories['segment5_purchasers'] / np.sum(segment5_total_buyers['segment5_total'])
    segment5_category_purchase_history = segment5_categories[['category_name','%_of_segment5_shoppers']]

    category_purchase_history_category = category_category_purchase_history.merge(segment1_category_purchase_history,on='category_name',how='outer').merge(segment2_category_purchase_history,on='category_name',how='outer').merge(segment3_category_purchase_history,on='category_name',how='outer').merge(segment4_category_purchase_history,on='category_name',how='outer').merge(segment5_category_purchase_history,on='category_name',how='outer')

    category_purchase_history_category['segment1_index_category'] = ((category_purchase_history_category['%_of_segment1_shoppers'] / category_purchase_history_category['%_of_category_shoppers'])*100)
    category_purchase_history_category['segment2_index_category'] = ((category_purchase_history_category['%_of_segment2_shoppers'] / category_purchase_history_category['%_of_category_shoppers'])*100)
    category_purchase_history_category['segment3_index_category'] = ((category_purchase_history_category['%_of_segment3_shoppers'] / category_purchase_history_category['%_of_category_shoppers'])*100)
    category_purchase_history_category['segment4_index_category'] = ((category_purchase_history_category['%_of_segment4_shoppers'] / category_purchase_history_category['%_of_category_shoppers'])*100)
    category_purchase_history_category['segment5_index_category'] = ((category_purchase_history_category['%_of_segment5_shoppers'] / category_purchase_history_category['%_of_category_shoppers'])*100)


    return category_purchase_history_category

def category_overall_purchase_history_sub_category(**params):

    sub_category_categories = exec_presto(sql.sub_category_top_categories_query.format(**params))
    sub_category_total_buyers = exec_presto(sql.sub_category_total_query.format(**params))
    sub_category_categories['%_of_sub_category_shoppers'] = sub_category_categories['sub_category_purchasers'] / np.sum(sub_category_total_buyers['sub_category_total'])
    sub_category_sub_category_purchase_history = sub_category_categories[['category_name','%_of_sub_category_shoppers']]

    segment1_categories = exec_presto(sql.segment1_top_categories_query.format(**params))
    segment1_total_buyers = exec_presto(sql.segment1_total_query.format(**params))
    segment1_categories['%_of_segment1_shoppers'] = segment1_categories['segment1_purchasers'] / np.sum(segment1_total_buyers['segment1_total'])
    segment1_sub_category_purchase_history = segment1_categories[['category_name','%_of_segment1_shoppers']]

    segment2_categories = exec_presto(sql.segment2_top_categories_query.format(**params))
    segment2_total_buyers = exec_presto(sql.segment2_total_query.format(**params))
    segment2_categories['%_of_segment2_shoppers'] = segment2_categories['segment2_purchasers'] / np.sum(segment2_total_buyers['segment2_total'])
    segment2_sub_category_purchase_history = segment2_categories[['category_name','%_of_segment2_shoppers']]

    segment3_categories = exec_presto(sql.segment3_top_categories_query.format(**params))
    segment3_total_buyers = exec_presto(sql.segment3_total_query.format(**params))
    segment3_categories['%_of_segment3_shoppers'] = segment3_categories['segment3_purchasers'] / np.sum(segment3_total_buyers['segment3_total'])
    segment3_sub_category_purchase_history = segment3_categories[['category_name','%_of_segment3_shoppers']]

    segment4_categories = exec_presto(sql.segment4_top_categories_query.format(**params))
    segment4_total_buyers = exec_presto(sql.segment4_total_query.format(**params))
    segment4_categories['%_of_segment4_shoppers'] = segment4_categories['segment4_purchasers'] / np.sum(segment4_total_buyers['segment4_total'])
    segment4_sub_category_purchase_history = segment4_categories[['category_name','%_of_segment4_shoppers']]

    segment5_categories = exec_presto(sql.segment5_top_categories_query.format(**params))
    segment5_total_buyers = exec_presto(sql.segment5_total_query.format(**params))
    segment5_categories['%_of_segment5_shoppers'] = segment5_categories['segment5_purchasers'] / np.sum(segment5_total_buyers['segment5_total'])
    segment5_sub_category_purchase_history = segment5_categories[['category_name','%_of_segment5_shoppers']]

    category_purchase_history_sub_category = sub_category_sub_category_purchase_history.merge(segment1_sub_category_purchase_history,on='category_name',how='outer').merge(segment2_sub_category_purchase_history,on='category_name',how='outer').merge(segment3_sub_category_purchase_history,on='category_name',how='outer').merge(segment4_sub_category_purchase_history,on='category_name',how='outer').merge(segment5_sub_category_purchase_history,on='category_name',how='outer')

    category_purchase_history_sub_category['segment1_index_sub_category'] = ((category_purchase_history_sub_category['%_of_segment1_shoppers'] / category_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)
    category_purchase_history_sub_category['segment2_index_sub_category'] = ((category_purchase_history_sub_category['%_of_segment2_shoppers'] / category_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)
    category_purchase_history_sub_category['segment3_index_sub_category'] = ((category_purchase_history_sub_category['%_of_segment3_shoppers'] / category_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)
    category_purchase_history_sub_category['segment4_index_sub_category'] = ((category_purchase_history_sub_category['%_of_segment4_shoppers'] / category_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)
    category_purchase_history_sub_category['segment5_index_sub_category'] = ((category_purchase_history_sub_category['%_of_segment5_shoppers'] / category_purchase_history_sub_category['%_of_sub_category_shoppers'])*100)


    return category_purchase_history_sub_category

def category_brands_purchase_history(**params):

    category_category_brands = exec_presto(sql.category_top_competitive_brands_query.format(**params))
    category_total_buyers = exec_presto(sql.category_total_query.format(**params))
    category_category_brands['%_of_category_shoppers'] = category_category_brands['category_purchasers'] / np.sum(category_total_buyers['category_total'])
    category_category_brand_purchase_history = category_category_brands[['brand_name','%_of_category_shoppers']]

    sub_category_category_brands = exec_presto(sql.sub_category_top_competitive_brands_query.format(**params))
    sub_category_total_buyers = exec_presto(sql.sub_category_total_query.format(**params))
    sub_category_category_brands['%_of_sub_category_shoppers'] = sub_category_category_brands['sub_category_purchasers'] / np.sum(sub_category_total_buyers['sub_category_total'])
    sub_category_category_brand_purchase_history = sub_category_category_brands[['brand_name','%_of_sub_category_shoppers']]

    segment1_category_brands = exec_presto(sql.segment1_top_category_brands_query.format(**params))
    segment1_total_buyers = exec_presto(sql.segment1_total_query.format(**params))
    segment1_category_brands['%_of_segment1_shoppers'] = segment1_category_brands['segment1_purchasers'] / np.sum(segment1_total_buyers['segment1_total'])
    segment1_category_brand_purchase_history = segment1_category_brands[['brand_name','%_of_segment1_shoppers']]

    segment2_category_brands = exec_presto(sql.segment2_top_category_brands_query.format(**params))
    segment2_total_buyers = exec_presto(sql.segment2_total_query.format(**params))
    segment2_category_brands['%_of_segment2_shoppers'] = segment2_category_brands['segment2_purchasers'] / np.sum(segment2_total_buyers['segment2_total'])
    segment2_category_brand_purchase_history = segment2_category_brands[['brand_name','%_of_segment2_shoppers']]

    segment3_category_brands = exec_presto(sql.segment3_top_category_brands_query.format(**params))
    segment3_total_buyers = exec_presto(sql.segment3_total_query.format(**params))
    segment3_category_brands['%_of_segment3_shoppers'] = segment3_category_brands['segment3_purchasers'] / np.sum(segment3_total_buyers['segment3_total'])
    segment3_category_brand_purchase_history = segment3_category_brands[['brand_name','%_of_segment3_shoppers']]

    segment4_category_brands = exec_presto(sql.segment4_top_category_brands_query.format(**params))
    segment4_total_buyers = exec_presto(sql.segment4_total_query.format(**params))
    segment4_category_brands['%_of_segment4_shoppers'] = segment4_category_brands['segment4_purchasers'] / np.sum(segment4_total_buyers['segment4_total'])
    segment4_category_brand_purchase_history = segment4_category_brands[['brand_name','%_of_segment4_shoppers']]

    segment5_category_brands = exec_presto(sql.segment5_top_category_brands_query.format(**params))
    segment5_total_buyers = exec_presto(sql.segment5_total_query.format(**params))
    segment5_category_brands['%_of_segment5_shoppers'] = segment5_category_brands['segment5_purchasers'] / np.sum(segment5_total_buyers['segment5_total'])
    segment5_category_brand_purchase_history = segment5_category_brands[['brand_name','%_of_segment5_shoppers']]

    category_brand_purchase_history = category_category_brand_purchase_history.merge(sub_category_category_brand_purchase_history,on='brand_name',how='outer').merge(segment1_category_brand_purchase_history,on='brand_name',how='outer').merge(segment2_category_brand_purchase_history,on='brand_name',how='outer').merge(segment3_category_brand_purchase_history,on='brand_name',how='outer').merge(segment4_category_brand_purchase_history,on='brand_name',how='outer').merge(segment5_category_brand_purchase_history,on='brand_name',how='outer')

    category_brand_purchase_history['segment1_index_category'] = ((category_brand_purchase_history['%_of_segment1_shoppers'] / category_brand_purchase_history['%_of_category_shoppers'])*100)
    category_brand_purchase_history['segment2_index_category'] = ((category_brand_purchase_history['%_of_segment2_shoppers'] / category_brand_purchase_history['%_of_category_shoppers'])*100)
    category_brand_purchase_history['segment3_index_category'] = ((category_brand_purchase_history['%_of_segment3_shoppers'] / category_brand_purchase_history['%_of_category_shoppers'])*100)
    category_brand_purchase_history['segment4_index_category'] = ((category_brand_purchase_history['%_of_segment4_shoppers'] / category_brand_purchase_history['%_of_category_shoppers'])*100)
    category_brand_purchase_history['segment5_index_category'] = ((category_brand_purchase_history['%_of_segment5_shoppers'] / category_brand_purchase_history['%_of_category_shoppers'])*100)
    category_brand_purchase_history['segment1_index_sub_category'] = ((category_brand_purchase_history['%_of_segment1_shoppers'] / category_brand_purchase_history['%_of_sub_category_shoppers'])*100)
    category_brand_purchase_history['segment2_index_sub_category'] = ((category_brand_purchase_history['%_of_segment2_shoppers'] / category_brand_purchase_history['%_of_sub_category_shoppers'])*100)
    category_brand_purchase_history['segment3_index_sub_category'] = ((category_brand_purchase_history['%_of_segment3_shoppers'] / category_brand_purchase_history['%_of_sub_category_shoppers'])*100)
    category_brand_purchase_history['segment4_index_sub_category'] = ((category_brand_purchase_history['%_of_segment4_shoppers'] / category_brand_purchase_history['%_of_sub_category_shoppers'])*100)
    category_brand_purchase_history['segment5_index_sub_category'] = ((category_brand_purchase_history['%_of_segment5_shoppers'] / category_brand_purchase_history['%_of_sub_category_shoppers'])*100)

    return category_brand_purchase_history

def sub_category_brands_purchase_history(**params):

    sub_category_sub_category_brands = exec_presto(sql.sub_category_top_sub_category_brands_query.format(**params))
    sub_category_total_buyers = exec_presto(sql.sub_category_total_query.format(**params))
    sub_category_sub_category_brands['%_of_sub_category_shoppers'] = sub_category_sub_category_brands['sub_category_purchasers'] / np.sum(sub_category_total_buyers['sub_category_total'])
    sub_category_sub_category_brand_purchase_history = sub_category_sub_category_brands[['brand_name','%_of_sub_category_shoppers']]

    segment1_sub_category_brands = exec_presto(sql.segment1_top_sub_category_brands_query.format(**params))
    segment1_total_buyers = exec_presto(sql.segment1_total_query.format(**params))
    segment1_sub_category_brands['%_of_segment1_shoppers'] = segment1_sub_category_brands['segment1_purchasers'] / np.sum(segment1_total_buyers['segment1_total'])
    segment1_sub_category_brand_purchase_history = segment1_sub_category_brands[['brand_name','%_of_segment1_shoppers']]

    segment2_sub_category_brands = exec_presto(sql.segment2_top_sub_category_brands_query.format(**params))
    segment2_total_buyers = exec_presto(sql.segment2_total_query.format(**params))
    segment2_sub_category_brands['%_of_segment2_shoppers'] = segment2_sub_category_brands['segment2_purchasers'] / np.sum(segment2_total_buyers['segment2_total'])
    segment2_sub_category_brand_purchase_history = segment2_sub_category_brands[['brand_name','%_of_segment2_shoppers']]

    segment3_sub_category_brands = exec_presto(sql.segment3_top_sub_category_brands_query.format(**params))
    segment3_total_buyers = exec_presto(sql.segment3_total_query.format(**params))
    segment3_sub_category_brands['%_of_segment3_shoppers'] = segment3_sub_category_brands['segment3_purchasers'] / np.sum(segment3_total_buyers['segment3_total'])
    segment3_sub_category_brand_purchase_history = segment3_sub_category_brands[['brand_name','%_of_segment3_shoppers']]

    segment4_sub_category_brands = exec_presto(sql.segment4_top_sub_category_brands_query.format(**params))
    segment4_total_buyers = exec_presto(sql.segment4_total_query.format(**params))
    segment4_sub_category_brands['%_of_segment4_shoppers'] = segment4_sub_category_brands['segment4_purchasers'] / np.sum(segment4_total_buyers['segment4_total'])
    segment4_sub_category_brand_purchase_history = segment4_sub_category_brands[['brand_name','%_of_segment4_shoppers']]

    segment5_sub_category_brands = exec_presto(sql.segment5_top_sub_category_brands_query.format(**params))
    segment5_total_buyers = exec_presto(sql.segment5_total_query.format(**params))
    segment5_sub_category_brands['%_of_segment5_shoppers'] = segment5_sub_category_brands['segment5_purchasers'] / np.sum(segment5_total_buyers['segment5_total'])
    segment5_sub_category_brand_purchase_history = segment5_sub_category_brands[['brand_name','%_of_segment5_shoppers']]

    sub_category_brand_purchase_history = sub_category_sub_category_brand_purchase_history.merge(segment1_sub_category_brand_purchase_history,on='brand_name',how='outer').merge(segment2_sub_category_brand_purchase_history,on='brand_name',how='outer').merge(segment3_sub_category_brand_purchase_history,on='brand_name',how='outer').merge(segment4_sub_category_brand_purchase_history,on='brand_name',how='outer').merge(segment5_sub_category_brand_purchase_history,on='brand_name',how='outer')

    sub_category_brand_purchase_history['segment1_index_sub_category'] = ((sub_category_brand_purchase_history['%_of_segment1_shoppers'] / sub_category_brand_purchase_history['%_of_sub_category_shoppers'])*100)
    sub_category_brand_purchase_history['segment2_index_sub_category'] = ((sub_category_brand_purchase_history['%_of_segment2_shoppers'] / sub_category_brand_purchase_history['%_of_sub_category_shoppers'])*100)
    sub_category_brand_purchase_history['segment3_index_sub_category'] = ((sub_category_brand_purchase_history['%_of_segment3_shoppers'] / sub_category_brand_purchase_history['%_of_sub_category_shoppers'])*100)
    sub_category_brand_purchase_history['segment4_index_sub_category'] = ((sub_category_brand_purchase_history['%_of_segment4_shoppers'] / sub_category_brand_purchase_history['%_of_sub_category_shoppers'])*100)
    sub_category_brand_purchase_history['segment5_index_sub_category'] = ((sub_category_brand_purchase_history['%_of_segment5_shoppers'] / sub_category_brand_purchase_history['%_of_sub_category_shoppers'])*100)


    return sub_category_brand_purchase_history

def basket_overview(**params):
    total_basket = exec_presto(sql.total_trip_basket_metrics.format(**params))
    category_basket = exec_presto(sql.category_basket_metrics.format(**params))
    sub_category_basket = exec_presto(sql.sub_category_basket_metrics.format(**params))

    basket_comparison = total_basket.merge(category_basket,on='segment',how='inner').merge(sub_category_basket,on='segment',how='inner')

    return basket_comparison

def demographics_comparison(**params):

    category_age_tier = exec_presto(sql.category_age_tier_query.format(**params))
    category_gender = exec_presto(sql.category_gender_query.format(**params))
    category_hh_income = exec_presto(sql.category_hh_income_query.format(**params))
    category_education = exec_presto(sql.category_education_query.format(**params))
    category_ethnicity = exec_presto(sql.category_ethnicity_query.format(**params))
    category_kids_in_hh = exec_presto(sql.category_kids_in_hh_query.format(**params))
    category_region = exec_presto(sql.category_region_query.format(**params))


    sub_category_age_tier = exec_presto(sql.sub_category_age_tier_query.format(**params))
    sub_category_gender = exec_presto(sql.sub_category_gender_query.format(**params))
    sub_category_hh_income = exec_presto(sql.sub_category_hh_income_query.format(**params))
    sub_category_education = exec_presto(sql.sub_category_education_query.format(**params))
    sub_category_ethnicity = exec_presto(sql.sub_category_ethnicity_query.format(**params))
    sub_category_kids_in_hh = exec_presto(sql.sub_category_kids_in_hh_query.format(**params))
    sub_category_region = exec_presto(sql.sub_category_region_query.format(**params))


    segment1_age_tier = exec_presto(sql.segment1_age_tier_query.format(**params))
    segment1_gender = exec_presto(sql.segment1_gender_query.format(**params))
    segment1_hh_income = exec_presto(sql.segment1_hh_income_query.format(**params))
    segment1_education = exec_presto(sql.segment1_education_query.format(**params))
    segment1_ethnicity = exec_presto(sql.segment1_ethnicity_query.format(**params))
    segment1_kids_in_hh = exec_presto(sql.segment1_kids_in_hh_query.format(**params))
    segment1_region = exec_presto(sql.segment1_region_query.format(**params))


    segment2_age_tier = exec_presto(sql.segment2_age_tier_query.format(**params))
    segment2_gender = exec_presto(sql.segment2_gender_query.format(**params))
    segment2_hh_income = exec_presto(sql.segment2_hh_income_query.format(**params))
    segment2_education = exec_presto(sql.segment2_education_query.format(**params))
    segment2_ethnicity = exec_presto(sql.segment2_ethnicity_query.format(**params))
    segment2_kids_in_hh = exec_presto(sql.segment2_kids_in_hh_query.format(**params))
    segment2_region = exec_presto(sql.segment2_region_query.format(**params))


    segment3_age_tier = exec_presto(sql.segment3_age_tier_query.format(**params))
    segment3_gender = exec_presto(sql.segment3_gender_query.format(**params))
    segment3_hh_income = exec_presto(sql.segment3_hh_income_query.format(**params))
    segment3_education = exec_presto(sql.segment3_education_query.format(**params))
    segment3_ethnicity = exec_presto(sql.segment3_ethnicity_query.format(**params))
    segment3_kids_in_hh = exec_presto(sql.segment3_kids_in_hh_query.format(**params))
    segment3_region = exec_presto(sql.segment3_region_query.format(**params))

    segment4_age_tier = exec_presto(sql.segment4_age_tier_query.format(**params))
    segment4_gender = exec_presto(sql.segment4_gender_query.format(**params))
    segment4_hh_income = exec_presto(sql.segment4_hh_income_query.format(**params))
    segment4_education = exec_presto(sql.segment4_education_query.format(**params))
    segment4_ethnicity = exec_presto(sql.segment4_ethnicity_query.format(**params))
    segment4_kids_in_hh = exec_presto(sql.segment4_kids_in_hh_query.format(**params))
    segment4_region = exec_presto(sql.segment4_region_query.format(**params))

    segment5_age_tier = exec_presto(sql.segment5_age_tier_query.format(**params))
    segment5_gender = exec_presto(sql.segment5_gender_query.format(**params))
    segment5_hh_income = exec_presto(sql.segment5_hh_income_query.format(**params))
    segment5_education = exec_presto(sql.segment5_education_query.format(**params))
    segment5_ethnicity = exec_presto(sql.segment5_ethnicity_query.format(**params))
    segment5_kids_in_hh = exec_presto(sql.segment5_kids_in_hh_query.format(**params))
    segment5_region = exec_presto(sql.segment5_region_query.format(**params))

    category_age_tier['category_age_tier_%'] = category_age_tier['category'] / np.sum(category_age_tier['category'])
    category_gender['category_gender_%'] = category_gender['category'] / np.sum(category_gender['category'])
    category_hh_income['category_hh_income_%'] = category_hh_income['category'] / np.sum(category_hh_income['category'])
    category_education['category_education_%'] = category_education['category'] / np.sum(category_education['category'])
    category_ethnicity['category_ethnicity_%'] = category_ethnicity['category'] / np.sum(category_ethnicity['category'])
    category_kids_in_hh['category_kids_in_hh_%'] = category_kids_in_hh['category'] / np.sum(category_kids_in_hh['category'])
    category_region['category_region_%'] = category_region['category'] / np.sum(category_region['category'])
    sub_category_age_tier['sub_category_age_tier_%'] = sub_category_age_tier['sub_category'] / np.sum(sub_category_age_tier['sub_category'])
    sub_category_gender['sub_category_gender_%'] = sub_category_gender['sub_category'] / np.sum(sub_category_gender['sub_category'])
    sub_category_hh_income['sub_category_hh_income_%'] = sub_category_hh_income['sub_category'] / np.sum(sub_category_hh_income['sub_category'])
    sub_category_education['sub_category_education_%'] = sub_category_education['sub_category'] / np.sum(sub_category_education['sub_category'])
    sub_category_ethnicity['sub_category_ethnicity_%'] = sub_category_ethnicity['sub_category'] / np.sum(sub_category_ethnicity['sub_category'])
    sub_category_kids_in_hh['sub_category_kids_in_hh_%'] = sub_category_kids_in_hh['sub_category'] / np.sum(sub_category_kids_in_hh['sub_category'])
    sub_category_region['sub_category_region_%'] = sub_category_region['sub_category'] / np.sum(sub_category_region['sub_category'])
    segment1_age_tier['segment1_age_tier_%'] = segment1_age_tier['segment1'] / np.sum(segment1_age_tier['segment1'])
    segment1_gender['segment1_gender_%'] = segment1_gender['segment1'] / np.sum(segment1_gender['segment1'])
    segment1_hh_income['segment1_hh_income_%'] = segment1_hh_income['segment1'] / np.sum(segment1_hh_income['segment1'])
    segment1_education['segment1_education_%'] = segment1_education['segment1'] / np.sum(segment1_education['segment1'])
    segment1_ethnicity['segment1_ethnicity_%'] = segment1_ethnicity['segment1'] / np.sum(segment1_ethnicity['segment1'])
    segment1_kids_in_hh['segment1_kids_in_hh_%'] = segment1_kids_in_hh['segment1'] / np.sum(segment1_kids_in_hh['segment1'])
    segment1_region['segment1_region_%'] = segment1_region['segment1'] / np.sum(segment1_region['segment1'])
    segment2_age_tier['segment2_age_tier_%'] = segment2_age_tier['segment2'] / np.sum(segment2_age_tier['segment2'])
    segment2_gender['segment2_gender_%'] = segment2_gender['segment2'] / np.sum(segment2_gender['segment2'])
    segment2_hh_income['segment2_hh_income_%'] = segment2_hh_income['segment2'] / np.sum(segment2_hh_income['segment2'])
    segment2_education['segment2_education_%'] = segment2_education['segment2'] / np.sum(segment2_education['segment2'])
    segment2_ethnicity['segment2_ethnicity_%'] = segment2_ethnicity['segment2'] / np.sum(segment2_ethnicity['segment2'])
    segment2_kids_in_hh['segment2_kids_in_hh_%'] = segment2_kids_in_hh['segment2'] / np.sum(segment2_kids_in_hh['segment2'])
    segment2_region['segment2_region_%'] = segment2_region['segment2'] / np.sum(segment2_region['segment2'])
    segment3_age_tier['segment3_age_tier_%'] = segment3_age_tier['segment3'] / np.sum(segment3_age_tier['segment3'])
    segment3_gender['segment3_gender_%'] = segment3_gender['segment3'] / np.sum(segment3_gender['segment3'])
    segment3_hh_income['segment3_hh_income_%'] = segment3_hh_income['segment3'] / np.sum(segment3_hh_income['segment3'])
    segment3_education['segment3_education_%'] = segment3_education['segment3'] / np.sum(segment3_education['segment3'])
    segment3_ethnicity['segment3_ethnicity_%'] = segment3_ethnicity['segment3'] / np.sum(segment3_ethnicity['segment3'])
    segment3_kids_in_hh['segment3_kids_in_hh_%'] = segment3_kids_in_hh['segment3'] / np.sum(segment3_kids_in_hh['segment3'])
    segment3_region['segment3_region_%'] = segment3_region['segment3'] / np.sum(segment3_region['segment3'])
    segment4_age_tier['segment4_age_tier_%'] = segment4_age_tier['segment4'] / np.sum(segment4_age_tier['segment4'])
    segment4_gender['segment4_gender_%'] = segment4_gender['segment4'] / np.sum(segment4_gender['segment4'])
    segment4_hh_income['segment4_hh_income_%'] = segment4_hh_income['segment4'] / np.sum(segment4_hh_income['segment4'])
    segment4_education['segment4_education_%'] = segment4_education['segment4'] / np.sum(segment4_education['segment4'])
    segment4_ethnicity['segment4_ethnicity_%'] = segment4_ethnicity['segment4'] / np.sum(segment4_ethnicity['segment4'])
    segment4_kids_in_hh['segment4_kids_in_hh_%'] = segment4_kids_in_hh['segment4'] / np.sum(segment4_kids_in_hh['segment4'])
    segment4_region['segment4_region_%'] = segment4_region['segment4'] / np.sum(segment4_region['segment4'])
    segment5_age_tier['segment5_age_tier_%'] = segment5_age_tier['segment5'] / np.sum(segment5_age_tier['segment5'])
    segment5_gender['segment5_gender_%'] = segment5_gender['segment5'] / np.sum(segment5_gender['segment5'])
    segment5_hh_income['segment5_hh_income_%'] = segment5_hh_income['segment5'] / np.sum(segment5_hh_income['segment5'])
    segment5_education['segment5_education_%'] = segment5_education['segment5'] / np.sum(segment5_education['segment5'])
    segment5_ethnicity['segment5_ethnicity_%'] = segment5_ethnicity['segment5'] / np.sum(segment5_ethnicity['segment5'])
    segment5_kids_in_hh['segment5_kids_in_hh_%'] = segment5_kids_in_hh['segment5'] / np.sum(segment5_kids_in_hh['segment5'])
    segment5_region['segment5_region_%'] = segment5_region['segment5'] / np.sum(segment5_region['segment5'])

    age_tier = category_age_tier.merge(sub_category_age_tier,on='age_tier',how='inner').merge(segment1_age_tier,on='age_tier',how='inner').merge(segment2_age_tier,on='age_tier',how='inner').merge(segment3_age_tier,on='age_tier',how='inner').merge(segment4_age_tier,on='age_tier',how='inner').merge(segment5_age_tier,on='age_tier',how='inner')
    gender = category_gender.merge(sub_category_gender,on='gender',how='inner').merge(segment1_gender,on='gender',how='inner').merge(segment2_gender,on='gender',how='inner').merge(segment3_gender,on='gender',how='inner').merge(segment4_gender,on='gender',how='inner').merge(segment5_gender,on='gender',how='inner')
    hh_income = category_hh_income.merge(sub_category_hh_income,on='hh_income',how='inner').merge(segment1_hh_income,on='hh_income',how='inner').merge(segment2_hh_income,on='hh_income',how='inner').merge(segment3_hh_income,on='hh_income',how='inner').merge(segment4_hh_income,on='hh_income',how='inner').merge(segment5_hh_income,on='hh_income',how='inner')
    education = category_education.merge(sub_category_education,on='education',how='inner').merge(segment1_education,on='education',how='inner').merge(segment2_education,on='education',how='inner').merge(segment3_education,on='education',how='inner').merge(segment4_education,on='education',how='inner').merge(segment5_education,on='education',how='inner')
    ethnicity = category_ethnicity.merge(sub_category_ethnicity,on='ethnicity',how='inner').merge(segment1_ethnicity,on='ethnicity',how='inner').merge(segment2_ethnicity,on='ethnicity',how='inner').merge(segment3_ethnicity,on='ethnicity',how='inner').merge(segment4_ethnicity,on='ethnicity',how='inner').merge(segment5_ethnicity,on='ethnicity',how='inner')
    kids_in_hh = category_kids_in_hh.merge(sub_category_kids_in_hh,on='kids_in_hh',how='inner').merge(segment1_kids_in_hh,on='kids_in_hh',how='inner').merge(segment2_kids_in_hh,on='kids_in_hh',how='inner').merge(segment3_kids_in_hh,on='kids_in_hh',how='inner').merge(segment4_kids_in_hh,on='kids_in_hh',how='inner').merge(segment5_kids_in_hh,on='kids_in_hh',how='inner')
    region = category_region.merge(sub_category_region,on='region',how='inner').merge(segment1_region,on='region',how='inner').merge(segment2_region,on='region',how='inner').merge(segment3_region,on='region',how='inner').merge(segment4_region,on='region',how='inner').merge(segment5_region,on='region',how='inner')

    category_avg_age = exec_presto(sql.category_avg_age_query.format(**params))
    category_avg_age.columns = ['category']
    category_avg_age['index']=category_avg_age.reset_index().index
    sub_category_avg_age = exec_presto(sql.sub_category_avg_age_query.format(**params))
    sub_category_avg_age.columns = ['sub_category']
    sub_category_avg_age['index']=sub_category_avg_age.reset_index().index
    segment1_avg_age = exec_presto(sql.segment1_avg_age_query.format(**params))
    segment1_avg_age.columns = ['segment1']
    segment1_avg_age['index']=segment1_avg_age.reset_index().index
    segment2_avg_age = exec_presto(sql.segment2_avg_age_query.format(**params))
    segment2_avg_age.columns = ['segment2']
    segment2_avg_age['index']=segment2_avg_age.reset_index().index
    segment3_avg_age = exec_presto(sql.segment3_avg_age_query.format(**params))
    segment3_avg_age.columns = ['segment3']
    segment3_avg_age['index']=segment3_avg_age.reset_index().index
    segment4_avg_age = exec_presto(sql.segment4_avg_age_query.format(**params))
    segment4_avg_age.columns = ['segment4']
    segment4_avg_age['index']=segment4_avg_age.reset_index().index
    segment5_avg_age = exec_presto(sql.segment5_avg_age_query.format(**params))
    segment5_avg_age.columns = ['segment5']
    segment5_avg_age['index']=segment5_avg_age.reset_index().index

    avg_age_combined = category_avg_age.merge(sub_category_avg_age,on='index',how='inner').merge(segment1_avg_age,on='index',how='inner').merge(segment2_avg_age,on='index',how='inner').merge(segment3_avg_age,on='index',how='inner').merge(segment4_avg_age,on='index',how='inner').merge(segment5_avg_age,on='index',how='inner')
    avg_age = avg_age_combined[['category','sub_category','segment1','segment2','segment3','segment4','segment5']]

    return age_tier, avg_age, gender, hh_income, education, ethnicity, kids_in_hh, region



def sa(segment_table: str = 'agajeski.table1',
       segment_py_table: str = 'agajeski.table1_py',
       category_table: str = 'agajeski.cat_table1',
       category_py_table: str = 'agajeski.cat_table1_py',
       active_table: str = 'agajeski.active',
       active_py_table: str = 'agajeski.active_py',
       segment1: str = 'Wholesome',
       segment2: str = 'Whole Earth',
       segment3: str = 'Equal',
       segment4: str = 'Swerve',
       segment5: str = 'PureVia',
       start_date: str = '2020-02-01',
       end_date: str = '2021-02-01'):

    params = {}
    params['segment_table'] = segment_table
    params['segment_py_table'] = segment_py_table
    params['category_table'] = category_table
    params['category_py_table'] = category_py_table
    params['active_table'] = active_table
    params['active_py_table'] = active_py_table
    params['segment1'] = segment1
    params['segment2'] = segment2
    params['segment3'] = segment3
    params['segment4'] = segment4
    params['segment5'] = segment5
    params['start_date'] = start_date
    params['end_date'] = end_date


    tabs_data: list = []

    print('Slide 1 volumetrics...')
    # Slide 1 - Volumetrics Comparison
    volume = volumetrics(**params)
    tabs_data.append([volume])

    print('Slide 2 category loyalty...')
    # Slide 2 - Catregory Brand Loyalty Comparison
    category_loyalty_comparison = category_loyalty(**params)
    tabs_data.append([category_loyalty_comparison])

    print('Slide 3 sub category loyalty...')
    # Slide 3 - Sub-Category Brand Loyalty Comparison
    sub_category_loyalty_comparison = sub_category_loyalty(**params)
    tabs_data.append([sub_category_loyalty_comparison])

    print('Slide 4 trial & repeat...')
    # Slide 4 - Trial & Repeat Comparison
    segment1_repurchases,segment1_scalars,segment2_repurchases,segment2_scalars,segment3_repurchases,segment3_scalars,segment4_repurchases,segment4_scalars,segment5_repurchases,segment5_scalars  = trial_repeat(**params)
    scalars_segment1 = pd.DataFrame(segment1_scalars, index=[0])
    scalars_segment2 = pd.DataFrame(segment2_scalars, index=[0])
    scalars_segment3 = pd.DataFrame(segment3_scalars, index=[0])
    scalars_segment4 = pd.DataFrame(segment4_scalars, index=[0])
    scalars_segment5 = pd.DataFrame(segment5_scalars, index=[0])

    tabs_data.append([segment1_repurchases,scalars_segment1,segment2_repurchases,scalars_segment2,segment3_repurchases,scalars_segment3,segment4_repurchases,scalars_segment4,segment5_repurchases,scalars_segment5])

    print('Slide 5 category market share...')
    # Slide 5 - Category Market Share Comparison
    category_market_share_comparison = category_market_share(**params)
    tabs_data.append([category_market_share_comparison])

    print('Slide 6 sub-category market share...')
    # Slide 5 - Sub-Category Market Share Comparison
    sub_category_market_share_comparison = sub_category_market_share(**params)
    tabs_data.append([sub_category_market_share_comparison])

    print('Slide 7 brand purchase history category...')
    # Slide 7 - Brand Purchase History Comparison Indexed to Total Category
    brand_purchase_history_comparison_category = brand_overall_purchase_history_category(**params)
    tabs_data.append([brand_purchase_history_comparison_category])

    print('Slide 8 brand purchase history sub-category...')
    # Slide 8 - Brand Purchase History Comparison Indexed to Total Sub-Category
    brand_purchase_history_comparison_sub_category = brand_overall_purchase_history_sub_category(**params)
    tabs_data.append([brand_purchase_history_comparison_sub_category])

    print('Slide 9 category purchase history category...')
    # Slide 9 - Category Purchase History Comparison Indexed to Category
    category_purchase_history_comparison_category = category_overall_purchase_history_category(**params)
    tabs_data.append([category_purchase_history_comparison_category])

    print('Slide 10 category purchase history sub-category...')
    # Slide 10 - Category Purchase History Comparison Indexed to Sub-Category
    category_purchase_history_comparison_sub_category = category_overall_purchase_history_sub_category(**params)
    tabs_data.append([category_purchase_history_comparison_sub_category])

    print('Slide 11 category brand purchase history...')
    # Slide 11 - Category Brand Purchase History Comparison
    category_competitve_brand_purchase_history = category_brands_purchase_history(**params)
    tabs_data.append([category_competitve_brand_purchase_history])

    print('Slide 12 sub-category brand purchase history...')
    # Slide 12 - Sub-Category Brand Purchase History Comparison
    sub_category_competitve_brand_purchase_history = sub_category_brands_purchase_history(**params)
    tabs_data.append([sub_category_competitve_brand_purchase_history])

    print('Slide 13 basket comparison...')
    # Slide 13 - Basket Comparison (Category, Sub-Category, Total Trip)
    basket_data = basket_overview(**params)
    tabs_data.append([basket_data])

    print('Slide 14 demographics comparison...')
    # Slide 14 - Demographic Comparison
    demos_comparison = list(demographics_comparison(**params))
    tabs_data.append(demos_comparison)

    xlname = 'Aperol_Shopper_Analysis.xlsx'.format(**params)

    sheet_names = ["Volumentrics",
                   "Category Loyalty",
                   "Sub Category Loyalty",
                   "Trial & Repeat",
                   "Category Market Share",
                   "Sub Cat Market Share",
                   "Brand Purchase Cat",
                   "Brand Purchase Sub Cat",
                   "Cat Purchase Cat",
                   "Cat Purchase Sub Cat",
                   "Comp Brand Purchase Cat",
                   "Comp Brand Purchase SubCat",
                   "Basket Comparison",
                   "Demographics"]

    dump_to_excel(xlname, tabs_data, sheet_names=sheet_names)
