import luigi
import requests

from datetime import date

from backend.repository import AWSFileRepository
from backend.config import get_config


class DownloadFileTask(luigi.Task):
    file_url = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(DownloadFileTask, self).__init__(*args, **kwargs)

        # don't see  a better way to fix the name of the temp file.. if we do this in output it changes on each call
        self._temp_file = luigi.LocalTarget(is_tmp=True)

    def output(self):
        return self._temp_file

    def run(self):
        if 'stopcoronavirus' in self.file_url:
            dashboard_storage = 'mumbai_dashboard.pdf'
            config = get_config()
            repository = AWSFileRepository(config['aws']['bucket production'])

            if repository.exists(dashboard_storage):
                repository._download_file(self._temp_file.path, config['aws']['bucket production'], dashboard_storage)
            else:
                print("Mumbai file not found in S3 bucket")
                
        else:
            response = requests.get(self.file_url, verify='tasks/districts/mumbaiwards_consolidated.pem')
            with open(self.output().path, mode='wb') as output_file:
                output_file.write(response.content)

    def complete(self):
        return self.output().exists()
