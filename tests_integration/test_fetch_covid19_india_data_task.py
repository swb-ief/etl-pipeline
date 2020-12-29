import unittest
from unittest.mock import patch

import luigi

from backend.gsheet_repository import GSheetRepository
from tasks.update_gsheet_task import FetchCovid19IndiaDataTask


class TestFetchCovid19IndiaDataTask(unittest.TestCase):

    def test_run(self):
        sut = FetchCovid19IndiaDataTask()
        worker = luigi.worker.Worker()
        worker.add(sut)
        self.assertTrue(worker.run())

        result = sut.output()
        self.assertTrue(result.exists())

        # cleanup
        result.remove()
