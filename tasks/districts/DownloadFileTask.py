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
        headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'}
        response = requests.get("https://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf", headers=headers, verify=False)
        #response = requests.get(self.file_url, verify=False)
        with open(self.output().path, mode='wb') as output_file:
            output_file.write(response.content)

    def complete(self):
        return self.output().exists()
