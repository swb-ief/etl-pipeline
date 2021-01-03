import unittest

import luigi

from tasks.districts import FetchMumbaiWardsTask


class TestFetchMumbaiWards(unittest.TestCase):

    def test_fetch_mumbai_wards_run(self):
        sut = FetchMumbaiWardsTask()

        worker = luigi.worker.Worker()
        worker.add(sut)
        self.assertTrue(worker.run())

        result = sut.output()
        self.assertTrue(result.exists())

        # cleanup
        result.remove()
