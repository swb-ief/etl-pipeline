"""Contains the code written for the data_pipeline as Luigi tasks"""

import datetime
import json
from datetime import date, timedelta

import luigi

import requests
import pandas as pd

from pipeline.extract_history import extract_history_command
from pipeline.calculate_metrics import (
    calculate_city_states_without_hospitalizations,
    calculate_city_states_hospitalizations,
    reset_hospitalization_percentages,
    calculate_city_stats_with_hospitalizations
)
from .dropbox import dropbox_target, textio2stringio

def hospitalization_csv_path(city_name, date):
    return f"/data/metrics/{city_name}/{date}-hospitalization.csv"

class FetchCovid19IndiaDataTask(luigi.Task):
    url = luigi.Parameter(default="https://raw.githubusercontent.com/covid19india/api/gh-pages/v4/data-all.json")
    date = luigi.DateParameter(default=date.today())
    
    def requires(self):
        return None 

    def run(self):
        response = requests.get(self.url)
        with self.output().open('w') as output:
            output.write(response.text)

    def output(self):
        return dropbox_target(f"/data/covid19api/{self.date}.json")

class ExtractHistoryTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    states_and_districts = luigi.DictParameter()

    def requires(self):
        return FetchCovid19IndiaDataTask(date=self.date)

    def output(self):
        return dropbox_target(f"/data/history{self.date}.csv")

    def run(self):
        with self.input().open('r') as input_file, self.output().open('w') as output_file:
            extract_history_command(textio2stringio(input_file), self.states_and_districts, output_file)

class CalculateMetricsWithoutHospitalizationTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    states_and_districts = luigi.DictParameter()
    city_name = luigi.Parameter()

    def requires(self):
        return ExtractHistoryTask(date=self.date, states_and_districts=self.states_and_districts)

    def output(self):
        return dropbox_target(f"/data/metrics/{self.city_name}/{self.date}-metrics-without-hospitalization.csv")

    def run(self):
        with self.input().open('r') as input_file, self.output().open('w') as output_file:
            calculate_city_states_without_hospitalizations(textio2stringio(input_file), output_file, self.city_name)


class CreateDefaultHosptializationTask(luigi.Task):
    date = luigi.DateParameter()
    metrics_date = luigi.DateParameter()
    states_and_districts = luigi.DictParameter()
    city_name = luigi.Parameter()
    default_hospitalization_path = luigi.Parameter(default="/data/metrics/hospitalization.csv")

    def requires(self):
        return CalculateMetricsWithoutHospitalizationTask(date=self.metrics_date, states_and_districts=self.states_and_districts, city_name=self.city_name)


    def output(self):
        return dropbox_target(self.default_hospitalization_path)

    def run(self):
        with self.output().open('w') as output_file, self.input().open('r') as metrics_input_file:
            reset_hospitalization_percentages(textio2stringio(metrics_input_file), output_file, self.city_name)

class FetchHospitalizationTask(luigi.Task):
    date = luigi.DateParameter()
    metrics_date = luigi.DateParameter()
    city_name = luigi.Parameter()
    states_and_districts = luigi.DictParameter()

    def requires(self):
        return CreateDefaultHosptializationTask(date=self.date, metrics_date=self.metrics_date, states_and_districts=self.states_and_districts, city_name=self.city_name)

    def output(self):
        return dropbox_target(hospitalization_csv_path(self.city_name, self.date))

    def run(self):
        with (
            self.input().open('r')
        ) as previous_hospitalization_file, (
            self.output().open('w')
        ) as output_file:
            output_file.write(previous_hospitalization_file.read()) 


class CreateHospitalizationTask(luigi.Task):
    date = luigi.DateParameter()
    metrics_date = luigi.DateParameter()
    states_and_districts = luigi.DictParameter()
    city_name = luigi.Parameter()

    def requires(self):
        return CalculateMetricsWithoutHospitalizationTask(date=self.metrics_date, states_and_districts=self.states_and_districts, city_name=self.city_name)

    def output(self):
        return dropbox_target(hospitalization_csv_path(self.city_name, self.date))

    def run(self):
        yesterday_hospitalization = yield FetchHospitalizationTask(date=self.date - timedelta(days=1), metrics_date=self.metrics_date, city_name=self.city_name, states_and_districts=self.states_and_districts)
        with (
            yesterday_hospitalization.open('r')
        ) as previous_hospitalization_file, (
            self.input().open('r')
        ) as metrics_file, (
            self.output().open('w')
        ) as output_file:
            calculate_city_states_hospitalizations(textio2stringio(previous_hospitalization_file), textio2stringio(metrics_file), output_file, self.city_name)        

class CalculateMetricsTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    states_and_districts = luigi.DictParameter()
    city_name = luigi.Parameter()

    def requires(self):
        return CalculateMetricsWithoutHospitalizationTask(date=self.date, states_and_districts=self.states_and_districts, city_name=self.city_name)

    def output(self):
        return dropbox_target(f"/data/metrics/{self.city_name}/{self.date}-metrics-with-hospitalization.csv")

    def run(self):
        hospitalization = yield CreateHospitalizationTask(
            date=self.date, 
            metrics_date=self.date, 
            city_name=self.city_name, 
            states_and_districts=self.states_and_districts)
        with (
            self.input().open('r')
        ) as metrics_without_hosptialization, (
            hospitalization.open('r')
        ) as hospitalization_data, (
            self.output().open('w')
        ) as output_file:
          calculate_city_stats_with_hospitalizations(
              textio2stringio(metrics_without_hosptialization), 
              textio2stringio(hospitalization_data), 
              output_file, 
              self.city_name)


if __name__ == '__main__':
    luigi.run()