import pandas as pd
import json
from typing import List


class ExtractCovid19IndiaData:
    metric_columns = ['confirmed', 'deceased', 'recovered', 'tested', 'other']
    meta_columns = ['population', 'tested']  # 'tested': { 'last_updated': "2020-09-19", 'source': '_url_'}

    @staticmethod
    def _add_metrics(current: dict, metrics: dict, columns: List[str], prefix: str) -> None:
        for column in columns:
            current[f'{prefix}{column}'] = metrics.get(column, 0)

    def process(self, json_dict: dict) -> (pd.DataFrame, pd.DataFrame):
        state_list = []
        district_list = []

        for date, states in json_dict.items():
            measurement_date = pd.to_datetime(date)
            for state, state_data in states.items():
                state_dict = dict()
                state_dict['date'] = measurement_date
                state_dict['state'] = state

                if 'delta' in state_data:
                    self._add_metrics(state_dict, state_data['delta'], self.metric_columns, 'delta.')

                if 'total' in state_data:
                    self._add_metrics(state_dict, state_data['total'], self.metric_columns, 'total.')

                if 'meta' in state_data:
                    self._add_metrics(state_dict, state_data['meta'], self.meta_columns, '')

                state_list.append(state_dict)
                if 'districts' in state_data:
                    for district, district_data in state_data['districts'].items():
                        district_dict = dict()
                        district_dict['date'] = measurement_date
                        district_dict['state'] = state
                        district_dict['district'] = district
                        if 'delta' in district_data:
                            self._add_metrics(district_dict, district_data['delta'], self.metric_columns, 'delta.')

                        if 'total' in district_data:
                            self._add_metrics(district_dict, district_data['total'], self.metric_columns, 'total.')
                        district_list.append(district_dict)

        df_state = pd.DataFrame(state_list)
        df_district = pd.DataFrame(district_list)
        return df_state, df_district
