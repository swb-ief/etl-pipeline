import luigi
import requests


class DownloadFileTask(luigi.Task):
    file_url = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(DownloadFileTask, self).__init__(*args, **kwargs)

        # don't see  a better way to fix the name of the temp file.. if we do this in output it changes on each call
        self._temp_file = luigi.LocalTarget(is_tmp=True)

    def output(self):
        return self._temp_file

    def run(self):
        response = requests.get(self.file_url, verify='/tasks/districts/mumbaiwards_consolidated.pem')
        with open(self.output().path, mode='wb') as output_file:
            output_file.write(response.content)

    def complete(self):
        return self.output().exists()
