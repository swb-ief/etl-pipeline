from datetime import date

import luigi

from backend.data.extract_mumbai_wards_pdf import scrape_mumbai_pdf
from tasks.districts.DownloadFileTask import DownloadFileTask


class FetchMumbaiWardsTask(luigi.Task):
    def requires(self):
        return DownloadFileTask(
            file_url='https://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf')

    def run(self):
        # We can also create a backup of the just downloaded PDF here

        df, df_2 = scrape_mumbai_pdf(self.input().path)

        df.to_csv(self.output()[0].path, index=False)
        df_2.to_csv(self.output()[1].path, index=False)

    def output(self):
        return luigi.LocalTarget(f'mumbai_{date.today()}.csv'), luigi.LocalTarget(f'mumbai_overall_{date.today()}.csv')  # TODO this can fail if the run is very close 23:59

    def complete(self):
        return self.output().exists()
