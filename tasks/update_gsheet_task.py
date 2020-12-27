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
    # states_and_districts = luigi.DictParameter()
    response_metrics = None
    response_hospitalization = None
    response_city_stats = None

    def run(self):
        covid19_api_json_output = yield FetchCovid19IndiaDataTask(date=self.date)
        hospitalization_worksheet = get_worksheet(WORKSHEET_URL, "hospitalization")
        hospitalization_df = get_dataframe(hospitalization_worksheet)
        city_stats_worksheet = get_worksheet(WORKSHEET_URL, "city_stats")
        city_stats_df = get_dataframe(city_stats_worksheet)

        metrics_worksheet = get_worksheet(WORKSHEET_URL, 'metrics')
        metrics_df = get_dataframe(metrics_worksheet)

        covid_19_df = pd.read_json(covid19_api_json_output).T

        hospitalizations_updated = impute_hospitalization_percentages(hospitalization_df, covid_19_df.index)

        city_stats_updated, metrics_updated = update_data(
            covid_19_df,
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
