import os
import unittest
from unittest.mock import patch

import luigi
import pandas as pd

from backend import GSheetRepository
from tasks.districts.DownloadFileTask import DownloadFileTask
from tasks.fetch_ward_data import FetchWardDataTask

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestFetchWardData(unittest.TestCase):
    def test_fetch_ward_data_run(self):
        results = dict()

        def my_store_dataframe(self, df: pd.DataFrame, storage_name, allow_create):
            results[storage_name] = df
            df.to_csv(os.path.join(THIS_DIR, f'../test output/test_fetch_ward_data_run_{storage_name}.csv'))

        class DownloadOutputMock:
            @staticmethod
            def open(*args):
                return open(os.path.join(THIS_DIR, '../samples/mumbai_dashboard.pdf'))

            @staticmethod
            def exists():
                return True

            @staticmethod
            def remove():
                return

            @property
            def path(self):
                return os.path.join(THIS_DIR, '../samples/mumbai_dashboard.pdf')

        def my_output(self):
            return DownloadOutputMock()

        with patch.object(GSheetRepository, 'exists', return_value=False), \
                patch.object(GSheetRepository, 'store_dataframe', new=my_store_dataframe), \
                patch.object(DownloadFileTask, 'output', new=my_output):
            sut = FetchWardDataTask()
            worker = luigi.worker.Worker()
            worker.add(sut)
            worker.run()

            # cleanup
            sut.output().remove()

        self.assertIsNotNone(results['raw_ward_data'])
