import logging
from datetime import date

import pandas as pd
import numpy as np 
import functools


def read_CriticalLoc(loc_type):
    if loc_type == "district":
        df = pd.read_csv()
        df = df[['date', 'district','total.confirmed', 'total.deceased', 'total.recovered']].dropna()
        df['delta_case'] = df['total.confirmed'] - df['total.deceased'] - df['total.recovered']
        df = df[['date', 'district', 'delta_case']]
    elif loc_type == "ward":
        df = pd.read_csv()
        df = df[['date', 'district']]
    else:
        raise ValueError

    return df 



def compute_DT(df):



    return 

