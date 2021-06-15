import os
import ba_tools
import numpy as np
import pandas as pd
from ds_util.qubole import run_hive, run_presto
from ba_tools.utils import dump_to_excel
from datetime import datetime, timedelta
from functools import lru_cache
from ba_tools.utils import id_input_validation
import re
from ds_util.qubole import run_presto
import io
from decipher.beacon import api

#Import and format screener data for sampling
def get_screener_data(survey):

    #Creates dataframe with all survey data
    df = pd.read_csv('{survey}.csv'.format(survey=survey))

    #Pulls out, renames, and combines key columns for balancing
    email = df.loc[:, df.columns.str.contains('email')]
    email.columns = ['email']
    customer_id = df.loc[:, df.columns.str.contains('customer_id')]
    customer_id.columns = ['customer_id']
    first_name = df.loc[:, df.columns.str.contains('first_name')]
    first_name.columns = ['first_name']
    age_tier = df.loc[:, df.columns.str.contains('age_tier')]
    age_tier.columns = ['age_tier']
    gender = df.loc[:, df.columns.str.contains('gender')]
    gender.columns = ['gender']
    region = df.loc[:, df.columns.str.contains('region')]
    region.columns = ['region']
    ethnicity = df.loc[:, df.columns.str.contains('ethnicity')]
    ethnicity.columns = ['ethnicity']
    kids = df.loc[:, df.columns.str.contains('kids_in_hh')]
    kids.columns = ['kids']
    income = df.loc[:, df.columns.str.contains('income')]
    income.columns = ['income']
    frames = [email,customer_id,first_name,age_tier,gender,region,ethnicity,kids,income]
    new_data = pd.concat(frames,axis=1)
    new_data=new_data.dropna(axis=0)

    #filter out respondents who selected prefer not to answer for income or ethnicity
    new_data = new_data[new_data['income']!=9]
    new_data = new_data[new_data['ethnicity']!=8]

    new_data['ethnicity'].replace({6: "Caucasian", 1: "Asian",2: "African American",3: "Hispanic",4: "Native American",5: "Native Hawiaan", 7: "Other"},inplace=True)
    new_data['ethnicity_group'] = new_data['ethnicity']
    new_data['ethnicity_group'].replace({'Caucasian': "Caucasian",'Asian': "Non-Caucasian",'African American':"Non-Caucasian",'Hispanic':"Non-Caucasian",'Native American': "Non-Caucasian",'Native Hawiaan':"Non-Caucasian",'Other': "Non-Caucasian"},inplace=True)
    new_data['kids'].replace({1: "Kids",2:"No_kids"}, inplace=True)
    new_data['income'].replace({1: "<$50K",2: "<$50K",3: "<$50K",4: "<$50K",5: "$50K-$100K",6: "$50K-$100K",7: "$100K+",8: "$100K+"}, inplace=True)
    new_data['age_tier'].replace({"18-24": "18-34","25-34": "18-34","35-44": "35-54","45-54": "35-54","55-64": "55+","65+": "55+"}, inplace=True)

    return new_data

def balancing_criteria(data,n_size,lim):
    dd=data.groupby(['age_tier','region','kids','income','gender','ethnicity_group'],axis=0,as_index=True,dropna=True).count().reset_index()
    dd['total']=pd.to_numeric(dd['email'],downcast='float')
    dd['weight']=dd['total']/dd.total.sum()
    weight = dd[['age_tier','region','kids','income','gender','ethnicity_group','total','weight']]
    weight['sample_size'] = round(weight['weight']*n_size,0)
    weight = weight[(weight['sample_size']*lim <= weight['total'])]
    weight = weight[(weight['sample_size'] >= lim/3)].reset_index()
    weight['key'] = weight['age_tier']+weight['region']+weight['kids']+weight['income']+weight['gender']+weight['ethnicity_group']

    return weight

def filtered_data_frames(weight,data):
    df_dict={}
    for index, row in weight.iterrows():
        filtered = filtered_tables(df = data,
                                   age_tier = row['age_tier'],
                                   region = row['region'],
                                   kids = row['kids'],
                                   income = row['income'],
                                   gender = row['gender'],
                                   ethnicity_group = row['ethnicity_group'])
        df_dict[row['key']]=filtered

    return df_dict

def filtered_tables(df,age_tier,region,kids,income,gender,ethnicity_group):

    filtered = df[(df['age_tier'] == '{age_tier}'.format(age_tier=age_tier)) & (df['region'] == '{region}'.format(region=region)) & (df['kids']=='{kids}'.format(kids=kids)) & (df['income'] == '{income}'.format(income=income)) & (df['gender'] == '{gender}'.format(gender=gender))& (df['ethnicity_group'] == '{ethnicity_group}'.format(ethnicity_group=ethnicity_group))]

    return filtered

def sample_file(df_dict,weight,lim,n_size,data):
    dict_keys=weight['key']
    keys=dict_keys.to_frame(name='keys')
    columns = ['email','first_name','customer_id','age_tier','gender','region','ethnicity','kids','income','ethnicity_group','group']
    sample_list = pd.DataFrame(columns=columns)

    for index, row in keys.iterrows():
        respondents = participants(keys=row['keys'],
                          df_dict=df_dict,
                          weight=weight,
                          lim=lim)
        sample_list=sample_list.append(respondents)

    duplicates = pd.merge(data, sample_list, how='inner',left_on=['email'], right_on=['email'],left_index=True)
    available_sample = data.drop(duplicates.index)

    segments = sample_list.group.unique()
    segments = pd.DataFrame(segments,columns=['segment'])

    sample_file = sample_list

    for index, row in segments.iterrows():
        if (n_size - sum(sample_file.group == row['segment'])) > 0:
            n = (n_size - sum(sample_file.group == row['segment']))
            ns = available_sample.sample(n)
            ns['group'] = row['segment']
            available_sample = available_sample.drop(ns.index)
            sample_file=sample_file.append(ns)

    return sample_file

def participants(weight,df_dict,lim,keys):
    df = df_dict['{keys}'.format(keys=keys)]
    n_pre = weight[(weight['key']=='{keys}'.format(keys=keys))]
    n = n_pre.sample_size.sum()
    n = n.astype(np.int64)

    columns = ['email','customer_id','first_name','age_tier','gender','region','ethnicity','kids','income','ethnicity_group','group']
    sample_group = pd.DataFrame(columns=columns)
    count=0
    for i in range(lim):
        participants = df.sample(n)
        count += 1
        participants['group'] = count
        sample_group=sample_group.append(participants)
        df=df.drop(participants.index)


    return sample_group
def overview(data,sample):
    demo_dict={}
    segments = sample.group.unique()
    segments = pd.DataFrame(segments,columns=['segment'])
    for index, row in segments.iterrows():
        seg_data = sample[(sample['group']==row['segment'])]
        demo_dict[row['segment']]=seg_data

    demo_columns = ['age_tier','region','kids','gender','income','ethnicity']
    demo_profiles=pd.DataFrame(demo_columns,columns=['demo_metric'])

    combine = pd.Series()
    for index, row in demo_profiles.iterrows():
        cnt = (sample[row['demo_metric']].value_counts() / len(sample[row['demo_metric']]))
        combine = combine.append(cnt)

    tot_col = ['Total']
    profile = pd.DataFrame(combine,columns=tot_col)

    for k in demo_dict:
        datas = demo_dict[k]
        combine_data = pd.Series()
        for index, row in demo_profiles.iterrows():
            freq = (datas[row['demo_metric']].value_counts() / len(datas[row['demo_metric']]))
            combine_data = combine_data.append(freq)
        combine_data = pd.DataFrame(combine_data,columns=[k])
        profile = profile.join(combine_data,how='inner')

    return profile

def export(sample_overview,sample):

    tabs_data: list = []

    tabs_data.append(sample)
    tabs_data.append(sample_overview)


    xlname = 'IHUT_Sample_File.xlsx'

    sheet_names = ["Sample Assignment",
                   "Balancing Overview"]

    dump_to_excel(xlname, tabs_data, sheet_names=sheet_names)

def main():
    survey = str(input("What is the name of the csv file with the screener data? (this must be saved in your home directory; do not include .csv in the name)"))
    lim = int(input("How many sample groups are there? "))
    n_size = int(input("How participants would you like per group? (recommend adding 2-3 more than needed) "))
    data = pd.DataFrame(get_screener_data(survey))
    weight = pd.DataFrame(balancing_criteria(data,n_size,lim))
    df_dict = filtered_data_frames(weight,data)
    sample = sample_file(df_dict,weight,lim,n_size,data)
    sample_overview = overview(data,sample)
    exporting = export(sample_overview,sample)
    print("DONE!!!")


if __name__ == "__main__":
    main()
