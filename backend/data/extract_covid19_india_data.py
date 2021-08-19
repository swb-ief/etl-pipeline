import pandas as pd
import json
from typing import List


class ExtractCovid19IndiaData:
    """ Extract data from api.covid19india.org """
    metric_columns = ['confirmed', 'deceased', 'recovered', 'tested', 'other']
    meta_columns = ['population', 'tested']  # 'tested': { 'last_updated': "2020-09-19", 'source': '_url_'}

    @staticmethod
    def _add_metrics(current: dict, metrics: dict, columns: List[str], prefix: str) -> None:
        for column in columns:
            current[f'{prefix}{column}'] = metrics.get(column, 0)

    def process(self, states_raw: pd.DataFrame, districts_raw: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
        
        df_state = states_raw.copy()
        df_state = df_state.loc[df_state['state']!='India']
        df_state = df_state.sort_values(by=["state", "date"]).reset_index(drop=True)
        
        df_district = districts_raw.copy()
        df_district = df_district.sort_values(by=["state", "district", "date"]).reset_index(drop=True)

        collist = ['confirmed', 'recovered', 'deceased', 'other', 'tested']

        for item in collist:
            df_state['delta.'+item] = df_state['total.'+item] - df_state.groupby(['state'])['total.'+item].shift(1)
            df_district['delta.'+item] = df_district['total.'+item] - df_district.groupby(['state', 'district'])['total.'+item].shift(1)      
        
        return df_state, df_district
