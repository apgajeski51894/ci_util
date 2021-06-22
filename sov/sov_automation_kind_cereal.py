import os
import ba_tools
import numpy as np
import pandas as pd
from ds_util.qubole import run_hive, run_presto
from ba_tools.utils import dump_to_excel
from datetime import datetime, timedelta
from functools import lru_cache
import sov_automation_queries_kind_cereal as sql
from ba_tools.utils import id_input_validation
import re
import ssl
from itertools import compress


@lru_cache(maxsize=35)
def exec_presto(query, return_data=True, cluster='presto-consumer-insights-dev', **kwargs):
    return run_presto(query=query, return_data=return_data, na_values='\\N')

def summary(**params):

    incrementality = exec_presto(sql.incrementality.format(**params))
    cannibalization = exec_presto(sql.brand_cannibalization.format(**params))
    brand_source = new_source = exec_presto(sql.total_brand_source.format(**params))
    product_source = new_source = exec_presto(sql.product_sourcing.format(**params))

    return incrementality, cannibalization, brand_source, product_source

def sa(parent_brand_ids,
       product_name: str = 'new cereal',
       parent_brand_id_type: str = 'global_product_id',
       segment_table: str = 'agajeski.table1',
       segment_py_table: str = 'agajeski.table1_py',
       category_table: str = 'agajeski.cat_table1',
       category_py_table: str = 'agajeski.cat_table1_py',
       active_table: str = 'agajeski.active',
       pre_date: str = '2019-02-01',
       start_date: str = '2020-02-01',
       end_date: str = '2021-02-01'):

    params = {}
    params['parent_brand_ids'] = parent_brand_ids
    params['product_name'] = product_name
    params['parent_brand_id_type'] = parent_brand_id_type
    params['segment_table'] = segment_table
    params['segment_py_table'] = segment_py_table
    params['category_table'] = category_table
    params['category_py_table'] = category_py_table
    params['active_table'] = active_table
    params['pre_date'] = pre_date
    params['start_date'] = start_date
    params['end_date'] = end_date

    if parent_brand_id_type == 'brand_name':
        params['brand_filter'] = 'brand_name = {parent_brand_ids}'.format(**params)
    elif parent_brand_id_type == 'brand_id':
        params['brand_filter'] = 'brand_id in {parent_brand_ids}'.format(**params)
    elif parent_brand_id_type == 'global_product_id':
        params['brand_filter'] = 'global_product_id in ({parent_brand_ids})'.format(**params)
    else:
        raise ValueError('Expecing parent_brand_id_type to be one of [brand_id,brand_name,global_product_id]')


    tabs_data: list = []

    print('Slide 1 overview...')
    # Slide 1 - Source of Volume Summary
    source_summary = list(summary(**params))
    tabs_data.append(source_summary)

    xlname = '{product_name}_sov.xlsx'.format(**params)

    sheet_names = ["Source of Volume Summary"]

    dump_to_excel(xlname, tabs_data, sheet_names=sheet_names)
