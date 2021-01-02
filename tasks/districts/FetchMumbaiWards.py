from datetime import datetime, date, timedelta

import pandas as pd

import luigi

from tasks.districts.DownloadFile import DownloadFile


class FetchMumbaiWards(luigi.Task):
    def requires(self):
        return DownloadFile(
            file_url='http://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf')

    def run(self):
        yesterday = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
        today = datetime.combine(date.today(), datetime.min.time())
        df = pd.DataFrame({
            'date': [yesterday, today],
            'some_metric': [0.1, 3.2]
        })

        # with self.output().open("wb") as output:
        #     # buf = TextIOWrapper(output)
        df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(f'mumbai_{date.today()}.csv')

    def complete(self):
        return self.output().exists()
