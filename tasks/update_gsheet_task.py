import luigi as luigi
from datetime import date, datetime
import pandas as pd
import numpy as np

from backend.gsheet_repository import GSheetRepository
from backend.metrics.calculations import impute_hospitalization_percentages
from tasks.fetch_covid19_india_data_task import FetchCovid19IndiaDataTask
from backend.orgestration import update_data


class UpdateGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())

    worksheet_hospitalizations = 'phase 2 - hospitalization'
    worksheet_metrics = 'phase 2 - metrics'
    worksheet_city_stats = 'phase 2 - city stats'

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
        repository = GSheetRepository(GSheetRepository.get_worksheet_url_from_env())

        covid19_api_json_output = yield FetchCovid19IndiaDataTask()
        with covid19_api_json_output.open('r') as json_file:
            all_covid19india_data = pd.read_json(json_file).T

        hospitalization_df = repository.get_dataframe(self.worksheet_hospitalizations)

        # city_stats_worksheet = get_worksheet(WORKSHEET_URL, "city_stats")
        # city_stats_df = get_dataframe(city_stats_worksheet)

        # metrics_worksheet = get_worksheet(WORKSHEET_URL, 'metrics')
        # metrics_df = get_dataframe(metrics_worksheet)

        hospitalizations_updated = impute_hospitalization_percentages(hospitalization_df, all_covid19india_data.index)

        city_stats_updated, metrics_updated = update_data(
            all_covid19india_data,
            hospitalizations_updated,
            start_date=datetime(2020, 4, 1)
        )

        repository.store_dataframe(hospitalizations_updated, self.worksheet_hospitalizations)
        repository.store_dataframe(metrics_updated, self.worksheet_metrics)

        # do we need this?
        repository.store_dataframe(city_stats_updated, self.worksheet_city_stats)

        # self.response_hospitalization = hospitalization_worksheet.update(
        #     [hospitalizations_updated.columns.values.tolist()]
        #     + hospitalizations_updated.replace("", np.nan).values.tolist()
        # )
        #
        # self.response_city_stats = city_stats_worksheet.update(
        #     [city_stats_updated.columns.values.tolist()]
        #     + city_stats_updated.replace("", np.nan).fillna("").values.tolist()
        # )
        #
        # self.response_metrics = metrics_worksheet.update(
        #     [metrics_updated.columns.values.tolist()]
        #     + [
        #         [str(vv).strip() if pd.notnull(vv) else "" for vv in ll]
        #         for ll in metrics_updated.values.tolist()
        #     ]
        # )

    def complete(self):
        return self.response_metrics is not None
