import luigi
from datetime import date

import requests


class FetchCovid19IndiaDataTask(luigi.Task):
    """ Fetches all the data available trough the API without any filtering """
    url = luigi.Parameter(
        default="https://raw.githubusercontent.com/covid19india/api/gh-pages/v4/data-all.json"
    )

    def requires(self):
        return None

    def run(self):
        response = requests.get(self.url)
        with self.output().open("w") as output:
            output.write(response.text)

    def output(self):
        return luigi.LocalTarget(f'covid19india_{date.today()}.json')
