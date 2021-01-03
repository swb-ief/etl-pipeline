from datetime import datetime, date, timedelta

import luigi

from backend.data.extract_mumbai_wards_pdf import scrape_mumbai_pdf
from tasks.districts.DownloadFileTask import DownloadFileTask


class FetchMumbaiWardsTask(luigi.Task):
    def requires(self):
        return DownloadFileTask(
            file_url='http://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf')

    def run(self):
        df = scrape_mumbai_pdf(self.input().path)

        df.to_csv(self.output().path, index=False)


def output(self):
    return luigi.LocalTarget(f'mumbai_{date.today()}.csv')


def complete(self):
    return self.output().exists()
