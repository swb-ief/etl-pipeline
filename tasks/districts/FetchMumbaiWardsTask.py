from datetime import date

import luigi

from backend.data.extract_mumbai_wards_pdf import scrape_mumbai_pdf
#from tasks.districts.DownloadFileTask import DownloadFileTask
from backend.repository import AWSFileRepository
from backend.config import get_config


class FetchMumbaiWardsTask(luigi.Task):
    def requires(self):
        self._temp_file = luigi.LocalTarget(is_tmp=True)
        dashboard_storage = 'mumbai_dashboard.pdf'
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        if repository.exists(dashboard_storage):
            repository._download_file(self._temp_file.path(), config['aws']['bucket production'], dashboard_storage)
        
        if self._temp_file.exists():
            return self._temp_file
        else:
            return False

    def run(self):
        # We can also create a backup of the just downloaded PDF here

        df = scrape_mumbai_pdf(self.input().path)

        df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(f'mumbai_{date.today()}.csv')  # TODO this can fail if the run is very close 23:59

    def complete(self):
        return self.output().exists()
