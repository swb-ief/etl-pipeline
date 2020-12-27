import luigi as luigi
from datetime import date, datetime
import tempfile
import pandas as pd
import numpy as np

from tasks.fetch_covid19_india_data_task import FetchCovid19IndiaDataTask
from backend.gsheet import get_worksheet, get_dataframe, WORKSHEET_URL
from backend.orgestration import update_data


class HospitalizationGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())
    states_and_districts = luigi.DictParameter()
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

        # hospitalization_df we have not input for this yet... so call the proper func to generate for missing days
        # and update its GSheet
        # currently this is entagled with the metric calculations... sepperate this properly
        # TODO

        covid_19_df = pd.read_json(covid19_api_json_output).T

        city_stats_updated, metrics_updated, hospitalizations_updated = update_data(
            covid_19_df,
            self.states_and_districts,
            hospitalization_df,
            datetime(2020, 4, 20)
        )

        self.response_hospitalization = hospitalization_worksheet.update(
            [hospitalizations_updated.columns.values.tolist()]
            + hospitalizations_updated.replace("", np.nan).values.tolist()
        )
        
        self.response_city_stats = city_stats_worksheet.update(
            [city_stats_updated.columns.values.tolist()]
            + city_stats_updated.replace("", np.nan).fillna("").values.tolist()
        )

        self.response_metrics = metrics_worksheet.update(
            [metrics_updated.columns.values.tolist()]
            + [
                [str(vv).strip() if pd.notnull(vv) else "" for vv in ll]
                for ll in metrics_updated.values.tolist()
            ]
        )

    def complete(self):
        return self.response_metrics is not None
