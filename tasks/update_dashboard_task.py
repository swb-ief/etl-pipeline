import json
from typing import List

import luigi as luigi
from datetime import datetime
import pandas as pd
import logging

from backend.data.utility import create_delta_cols
from backend.config import get_config
from backend.data import ExtractCovid19IndiaData
from backend.data.utility import last_values_by_date
from backend.repository import GSheetRepository, Repository, AWSFileRepository
from backend.metrics.calculations import impute_hospitalization_percentages, extend_and_impute_metrics
from tasks.fetch_covid19_india_data_task import FetchCovid19IndiaDataTask
from tasks.fetch_ward_data import FetchWardDataTask

log = logging.getLogger(__name__)


class UpdateDashboardTask(luigi.Task):
    storage_hospitalizations = 'Phase 2 - Hospitalization'
    storage_districts = 'Phase 2 - Districts'
    storage_states = 'Phase 2 - States'
    storage_wards = 'Phase 2 - Wards'

    # data for which we do not track a history
    storage_states_static = 'Phase 2 - States - Static data'
    storage_districts_static = 'Phase 2 - Districts - Static data'
    storage_wards_static = 'Phase 2 - Wards - Static data'  # Not yet implemented

    states_is_valid = False
    districts_is_valid = False
    wards_is_valid = False

    metrics_needed_by_dashboard = [
        # updated 2020-12-28 Phase 1
        # more can be found here https://github.com/swb-ief/etl-pipeline/blob/data_pipeline_readme/explainers/WorkflowDescription.md
        # note it is still on a branch. soon replace /data_pipeline_readme/ for /master/

        'delta.confirmed',
        'delta.deceased',
        'delta.tested',
        'delta.recovered',
        'delta.hospitalized',
        'delta.percent.case.growth',
        'delta.positivity',

        'total.confirmed',
        'total.deceased',
        'total.recovered',
        'total.vaccinated',

        'total.confirmed.14_day_ratio',

        'delta.confirmed.ratio_per_million',
        'delta.deceased.ratio_per_million',

        'total.confirmed.ratio_per_million',
        'total.deceased.ratio_per_million',

        'MA.21.delta.active',
        'MA.21.delta.deceased',
        'MA.21.delta.hospitalized',
        'MA.21.delta.recovered',
        'MA.21.delta.positivity',
        'MA.21.delta.tested',
        'MA.21.delta.confirmed',

        'MA.21.delta.confirmed.ratio_per_million',
        'MA.21.delta.deceased.ratio_per_million',

        'MA.21.total.confirmed.ratio_per_million',
        'MA.21.total.deceased.ratio_per_million',

        'delta.confirmed',
        'delta.recovered',
        'delta.active',
        'delta.deceased'
        # phase 1 name MA.21.daily.tests
    ]
    # Not yet added R generated metrics mean.mean, CI_lower.mean, CI_upper.mean, doubling.time

    state_keys = ['date', 'state']
    district_keys = state_keys + ['district']
    ward_keys = district_keys + ['ward']

    state_columns_needed_by_dashboard = state_keys + metrics_needed_by_dashboard
    district_columns_needed_by_dashboard = district_keys + metrics_needed_by_dashboard
    ward_columns_needed_by_dashboard = ward_keys + metrics_needed_by_dashboard

    def requires(self):
        return {
            'ward_data': FetchWardDataTask() #,'state_district_data': FetchCovid19IndiaDataTask()
        }

    def run(self):
        config = get_config()

        # we are skipping older data since we only have low case numbers there.
        start_date = datetime.strptime(config['dashboard']['start date'], '%Y-%m-%d')
        # repository = GSheetRepository(config['google sheets']['url production'])
        repository = AWSFileRepository(config['aws']['bucket production'])

        #fetch_covid19_india_task = self.input()['state_district_data']
        fetch_wards_task = self.input()['ward_data']

        #with fetch_covid19_india_task.open('r') as json_file:
        #    all_covid19india_data = json.load(json_file)
        states_covid19india_data = pd.read_csv("https://api.covid19india.org/csv/latest/states.csv", parse_dates=["Date"])
        districts_covid19india_data = pd.read_csv("https://api.covid19india.org/csv/latest/districts.csv", parse_dates=["Date"])
        
        states_covid19india_data.columns = ['date', 'state', 'total.confirmed', 'total.recovered', 'total.deceased', 'total.other', 'total.tested']
        states_covid19india_data['tested'] = states_covid19india_data['total.tested']
        districts_covid19india_data.columns = ['date', 'state', 'district', 'total.confirmed', 'total.recovered', 'total.deceased', 'total.other', 'total.tested']
        districts_covid19india_data['tested'] = districts_covid19india_data['total.tested']

        all_ward_data = pd.read_csv(fetch_wards_task.path, parse_dates=['date'])

        # cleanup
        #fetch_covid19_india_task.remove()
        fetch_wards_task.remove()

        state_data, district_data = ExtractCovid19IndiaData().process(states_covid19india_data, districts_covid19india_data)

        # not the best location to create this, but it's ok for now
        if not repository.exists(self.storage_hospitalizations):
            df = pd.DataFrame({'date': [], 'percentages': []})
            repository.store_dataframe(df, self.storage_hospitalizations, allow_create=True)

        hospitalization_df = repository.get_dataframe(self.storage_hospitalizations)
        hospitalizations_updated = impute_hospitalization_percentages(hospitalization_df, state_data['date'])

        state_data = state_data[state_data['date'] >= start_date]
        state_data = extend_and_impute_metrics(
            raw_metrics=state_data,
            hospitalizations=hospitalizations_updated,
            grouping_columns=['state']
        )

        district_data = district_data[district_data['date'] >= start_date]
        district_data = extend_and_impute_metrics(
            raw_metrics=district_data,
            hospitalizations=hospitalizations_updated,
            grouping_columns=['state', 'district']
        )

        ward_data = all_ward_data[all_ward_data['date'] >= start_date]

        ward_data = extend_and_impute_metrics(
            raw_metrics=ward_data,
            hospitalizations=hospitalizations_updated,
            grouping_columns=['state', 'district', 'ward']
        )

        self.update_population_sheets(state_data, district_data, repository)

        # Idea placeholder
        # Calculate today's top 20ish cities and add that top 20 as a tab in the google sheet so the dashboard can
        # get access to it.

        # validate and filter
        self.states_is_valid = self._has_all_columns(state_data, self.state_columns_needed_by_dashboard)
        self.districts_is_valid = self._has_all_columns(district_data, self.district_columns_needed_by_dashboard)
        self.wards_is_valid = self._has_all_columns(ward_data, self.ward_columns_needed_by_dashboard)

        states_filtered = state_data[self.state_columns_needed_by_dashboard]
        districts_filtered = district_data[self.district_columns_needed_by_dashboard]
        wards_filtered = ward_data[self.ward_columns_needed_by_dashboard]

        repository.store_dataframe(hospitalizations_updated, self.storage_hospitalizations, allow_create=True)
        repository.store_dataframe(states_filtered, self.storage_states, allow_create=True)
        repository.store_dataframe(districts_filtered, self.storage_districts, allow_create=True)
        repository.store_dataframe(wards_filtered, self.storage_wards, allow_create=True)

    @staticmethod
    def _has_all_columns(df: pd.DataFrame, columns: List[str]) -> bool:
        for column in columns:
            if column not in df.columns:
                log.error(f'Missing column: {column}')
                return False
        return True

    def complete(self):
        return self.states_is_valid and self.districts_is_valid and self.wards_is_valid

    def update_population_sheets(self, state: pd.DataFrame, district: pd.DataFrame, repository: Repository):
        basic_column_filter = ['date', 'population']
        state = state[['state'] + basic_column_filter]
        district = district[['state', 'district'] + basic_column_filter]

        state = last_values_by_date(state)
        district = last_values_by_date(district)

        repository.store_dataframe(state, self.storage_states_static, allow_create=True)
        repository.store_dataframe(district, self.storage_districts_static, allow_create=True)
