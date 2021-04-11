import logging
from datetime import date

import pandas as pd
import numpy as np 
import functools

def read_CriticalLoc(loc_type):
    if loc_type == "district":
        df = pd.read_csv('/usr/data/citystats.csv')
    elif loc_type == "ward":
        df = pd.read_csv()
    else:
        raise ValueError
    df = df[['date', loc_type,'total.confirmed', 'total.deceased', 'total.recovered']].dropna()
    df['delta_case'] = df['total.confirmed'] - df['total.deceased'] - df['total.recovered']
    df = df[['date', loc_type, 'delta_case']]
    return df

def compute_DT(df, time_gap):
    series_cum_cases = df['delta_case'].cumsum()
    r = np.log(series_cum_cases/series_cum_cases.shift(time_gap))
    series_cases_increase_days = (time_gap*np.log(2))/r
    return series_cases_increase_days

def apply_DT(df):
    dt_out = []
    for loc in df['district'].drop_duplicates():
        temp_df = df[df['district']==loc].reset_index(drop=True)
        temp_df['dt'] = compute_DT(df=temp_df, time_gap=7)
        temp_df = temp_df[['date','district','dt']]
        dt_out.append(temp_df)
    dt_out_df = pd.concat(dt_out).reset_index(drop=True)
    return dt_out_df

def save_DT(df):
    df.to_csv("/usr/data/doubling_time_districts.csv")
    return None

def run_DT(loc_type):
    df =read_CriticalLoc(loc_type=loc_type)
    df_dt = apply_DT(df=df)
    save_DT(df=df_dt)
    return None


