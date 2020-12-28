import luigi as luigi
from datetime import date, datetime
import pandas as pd
import numpy as np

from backend.metrics.calculations import impute_hospitalization_percentages
from tasks.fetch_covid19_india_data_task import FetchCovid19IndiaDataTask
from backend.gsheet import get_worksheet, get_dataframe, WORKSHEET_URL
from backend.orgestration import update_data


class UpdateGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())
    response_metrics = None
    response_hospitalization = None
    response_city_stats = None

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
        covid19_api_json_output = yield FetchCovid19IndiaDataTask()
        with covid19_api_json_output.open('r') as json_file:
            all_covid19india_data = pd.read_json(json_file).T

        hospitalization_worksheet = get_worksheet(WORKSHEET_URL, "hospitalization")
        hospitalization_df = get_dataframe(hospitalization_worksheet)

        city_stats_worksheet = get_worksheet(WORKSHEET_URL, "city_stats")
        city_stats_df = get_dataframe(city_stats_worksheet)

        metrics_worksheet = get_worksheet(WORKSHEET_URL, 'metrics')
        metrics_df = get_dataframe(metrics_worksheet)

        hospitalizations_updated = impute_hospitalization_percentages(hospitalization_df, all_covid19india_data.index)

        city_stats_updated, metrics_updated = update_data(
            all_covid19india_data,
            self.states_and_districts,
            hospitalizations_updated,
            datetime(2020, 4, 20)  # TODO see below... how do we want to handle this.
        )

        # TODO this will overwrite old stuff
        # need to append old and new dataframes
        # however with the hard coded date it is alwasy everything
        # so this works for now...
        # Recomendation: make the expectation that it will return the entire dataset updated.
        # We want to keep the tasks themselves as light as possible

        # TODO: Might need to fix ordering of columns...

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
