import json

import luigi as luigi
from datetime import date, datetime
import pandas as pd

from backend.data import ExtractCovid19IndiaData
from backend.gsheet_repository import GSheetRepository
from backend.metrics.calculations import impute_hospitalization_percentages, impute_metrics
from tasks.fetch_covid19_india_data_task import FetchCovid19IndiaDataTask


class UpdateGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())

    worksheet_hospitalizations = 'Phase 2 - Hospitalization'
    worksheet_districts = 'Phase 2 - Districts'
    worksheet_states = 'Phase 2 - States'

    metrics_sheet_columns_needed_by_dashboard = [
        # updated 2020-12-28 Phase 1
        # more can be found here https://github.com/swb-ief/etl-pipeline/blob/data_pipeline_readme/explainers/WorkflowDescription.md
        # note it is still on a branch. soon replace /data_pipeline_readme/ for /master/
        'district',
        'date',

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

        'levitt.Metric',  # this needs a better name

        'MA.21.daily.tests',
        'MA.21.delta.positivity',
    ]

    def run(self):
        # we are skipping older data since we only have low case numbers there.
        start_date = datetime(2020, 4, 1)

        repository = GSheetRepository(GSheetRepository.get_worksheet_url_from_env())

        covid19_api_json_output = yield FetchCovid19IndiaDataTask()
        with covid19_api_json_output.open('r') as json_file:
            all_covid19india_data = json.load(json_file)

        # we have access to the state metrics as well but not needed yet in the dashboard
        state_data, district_data = ExtractCovid19IndiaData().process(all_covid19india_data)

        hospitalization_df = repository.get_dataframe(self.worksheet_hospitalizations)
        hospitalizations_updated = impute_hospitalization_percentages(hospitalization_df, state_data['date'])

        state_data = state_data[state_data['date'] >= start_date]
        state_data = impute_metrics(
            raw_metrics=state_data,
            hospitalizations=hospitalizations_updated
        )

        district_data = district_data[district_data['date'] >= start_date]
        district_data = impute_metrics(
            raw_metrics=district_data,
            hospitalizations=hospitalizations_updated,
        )

        repository.store_dataframe(hospitalizations_updated, self.worksheet_hospitalizations)
        repository.store_dataframe(state_data, self.worksheet_states)
        repository.store_dataframe(district_data, self.worksheet_districts)
