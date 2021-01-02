import unittest

import luigi

from tasks.districts import FetchMumbaiWards
from tasks.update_gsheet_task import FetchCovid19IndiaDataTask


class TestFetchMumbaiWards(unittest.TestCase):

    def test_fetch_mumbai_wards_run(self):
        sut = FetchMumbaiWards()

        worker = luigi.worker.Worker()
        worker.add(sut)
        self.assertTrue(worker.run())

        result = sut.output()
        self.assertTrue(result.exists())

        # cleanup
        result.remove()
