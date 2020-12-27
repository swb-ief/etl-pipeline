import luigi
from datetime import date

import requests


class FetchCovid19IndiaDataTask(luigi.Task):
    url = luigi.Parameter(
        default="https://raw.githubusercontent.com/covid19india/api/gh-pages/v4/data-all.json"
    )
    date = luigi.DateParameter(default=date.today())

    def requires(self):
        return None

    def run(self):
        response = requests.get(self.url)
        with self.output().open("w") as output:
            output.write(response.text)

    def output(self):
        return self.localTarget(f'{self.date}.json')  # dropbox_target(f"/data/covid19api/{self.date}.json")
