import unittest

import luigi

from tasks.update_gsheet_task import FetchCovid19IndiaDataTask


class TestFetchCovid19IndiaDataTask(unittest.TestCase):

    def test_fetch_covid19inda_run(self):
        sut = FetchCovid19IndiaDataTask()
        worker = luigi.worker.Worker()
        worker.add(sut)
        self.assertTrue(worker.run())

        result = sut.output()
        self.assertTrue(result.exists())

        # cleanup
        result.remove()
