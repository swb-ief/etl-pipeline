"""Contains the code written for the data_pipeline as Luigi tasks"""

import datetime
import json
import hashlib
import tempfile
from datetime import date, timedelta, datetime

import luigi

import requests
import pandas as pd

from pipeline.calculate_metrics import calculate_metrics_input
from pipeline.extract_history import extract_history_command
from .gcloud import gcs_target, textio2stringio
from .cities_metrics_v1 import FetchCovid19IndiaDataTask


def state_districts_hash(states_and_districts):
    return hashlib.md5(str(states_and_districts).encode("utf-8")).hexdigest()


class ExtractMetricsHistoryTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    states_and_districts = luigi.DictParameter()

    def requires(self):
        return FetchCovid19IndiaDataTask(date=self.date)

    def output(self):
        return gcs_target(
            f"/data/history-metrics/{state_districts_hash(self.states_and_districts)}-{self.date}.csv"
        )

    def run(self):
        with self.input().open("r") as input_file, self.output().open(
            "w"
        ) as output_file:
            extract_history_command(
                textio2stringio(input_file), self.states_and_districts, output_file
            )


class CalculateCityMetricsTask(luigi.Task):
    date = luigi.DateParameter()
    start_date = luigi.DateParameter()
    states_and_districts = luigi.DictParameter()

    def requires(self):
        return ExtractMetricsHistoryTask(
            date=self.date, states_and_districts=self.states_and_districts
        )

    def output(self):
        return {
            "city_metrics": gcs_target(
                f"/data/city-metrics/{state_districts_hash(self.states_and_districts)}-{self.date}-city-metrics.csv"
            ),
            "hospitalizations": gcs_target(
                f"/data/hospitalizations/hospitalizations-city-metrics.csv"
            ),
        }

    def run(self):
        hospitalizations = self.output()["hospitalizations"]
        city_metrics = self.output()["city_metrics"]
        history = self.input()
        start_datetime = datetime(
            self.start_date.year, self.start_date.month, self.start_date.day, 0, 0, 0
        )

        tmp_hospitalization = tempfile.NamedTemporaryFile("ab+")
        with hospitalizations.open("r") as hosptialization_input:
            if hospitalizations.exists():
                tmp_hospitalization.write(hosptialization_input.read().encode("utf-8"))
        tmp_hospitalization.seek(0)
        with (city_metrics.open("w")) as city_metrics_file, (
            history.open("r")
        ) as history_input_csv:
            calculate_metrics_input(
                textio2stringio(history_input_csv),
                start_datetime,
                tmp_hospitalization.name,
                city_metrics_file,
            )
        # assuming you want utf-8
        with (hospitalizations.open("w")) as hosptialization_output:
            hosptialization_output.write((tmp_hospitalization.read().decode("utf-8")))
        tmp_hospitalization.close()


if __name__ == "__main__":
    luigi.run()
