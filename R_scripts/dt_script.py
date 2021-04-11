import logging
from datetime import date

import pandas as pd
import numpy as np 
import functools

def read_CriticalLoc(loc_type):
    if loc_type == "district":
        df = pd.read_csv()
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

