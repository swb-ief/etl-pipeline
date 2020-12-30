import json
from typing import List

import luigi as luigi
from datetime import datetime
import pandas as pd
import logging

from backend.data import ExtractCovid19IndiaData
from backend.gsheet_repository import GSheetRepository
from backend.metrics.calculations import impute_hospitalization_percentages, extend_and_impute_metrics
from tasks.fetch_covid19_india_data_task import FetchCovid19IndiaDataTask

log = logging.getLogger(__name__)


class UpdateGSheetTask(luigi.ExternalTask):
    worksheet_hospitalizations = 'Phase 2 - Hospitalization'
    worksheet_districts = 'Phase 2 - Districts'
    worksheet_states = 'Phase 2 - States'

    states_is_valid = False
    districts_is_valid = False

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

        'spline.active',
        'spline.deceased',
        'spline.hospitalized',
        'spline.recovered',

        'total.deceased.levitt',  # this needs a better name

        'MA.21.daily.tests',
        'MA.21.delta.positivity',
    ]
    # Not yet added R generated metrics mean.mean, CI_lower.mean, CI_upper.mean, doubling.time

    state_keys = ['date', 'state']
    district_keys = ['date', 'state', 'district']

    state_columns_needed_by_dashboard = state_keys + metrics_needed_by_dashboard
    district_columns_needed_by_dashboard = district_keys + metrics_needed_by_dashboard

    def run(self):
        # we are skipping older data since we only have low case numbers there.
        start_date = datetime(2020, 4, 1)

        repository = GSheetRepository(GSheetRepository.get_worksheet_url_from_env())

        fetch_covid19_india_task = yield FetchCovid19IndiaDataTask()

        # Kick off Ward data collection trough tasks
        # See if luigi can parallelize these 'download' steps

        with fetch_covid19_india_task.open('r') as json_file:
            all_covid19india_data = json.load(json_file)

        fetch_covid19_india_task.remove()

        # we have access to the state metrics as well but not needed yet in the dashboard
        state_data, district_data = ExtractCovid19IndiaData().process(all_covid19india_data)

        hospitalization_df = repository.get_dataframe(self.worksheet_hospitalizations)
        hospitalizations_updated = impute_hospitalization_percentages(hospitalization_df, state_data['date'])

        state_data = state_data[state_data['date'] >= start_date]
        state_data = extend_and_impute_metrics(
            raw_metrics=state_data,
            hospitalizations=hospitalizations_updated
        )

        district_data = district_data[district_data['date'] >= start_date]
        district_data = extend_and_impute_metrics(
            raw_metrics=district_data,
            hospitalizations=hospitalizations_updated,
        )

        # INSERT WARD PROCESSING HERE
        # INSERT R0, Rt calculations here (might also be part of the extend_and_impute part)

        # validate and filter
        self.states_is_valid = self._has_all_columns(state_data, self.state_columns_needed_by_dashboard)
        self.districts_is_valid = self._has_all_columns(district_data, self.district_columns_needed_by_dashboard)

        states_filtered = state_data[self.state_columns_needed_by_dashboard]
        districts_filtered = district_data[self.district_columns_needed_by_dashboard]

        repository.store_dataframe(hospitalizations_updated, self.worksheet_hospitalizations)
        repository.store_dataframe(states_filtered, self.worksheet_states)
        repository.store_dataframe(districts_filtered, self.worksheet_districts)

    @staticmethod
    def _has_all_columns(df: pd.DataFrame, columns: List[str]) -> bool:
        for column in columns:
            if column not in df.columns:
                log.error(f'Missing column: {column}')
                return False
        return True

    def complete(self):
        return self.states_is_valid and self.districts_is_valid
