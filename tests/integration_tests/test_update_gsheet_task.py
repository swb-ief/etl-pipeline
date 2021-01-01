import os
import unittest
from unittest.mock import patch
import pandas as pd

import luigi

from backend.repository.gsheet_repository import GSheetRepository
from tasks import FetchCovid19IndiaDataTask
from tasks.update_gsheet_task import UpdateGSheetTask

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestUpdateGSheetTask(unittest.TestCase):

    def test_update_gsheet_run(self):
        """ This will run local parts only using files from sample etc...
        """
        results = dict()
        expected_results = {
            UpdateGSheetTask.worksheet_hospitalizations: (307, 2),
            UpdateGSheetTask.worksheet_states: (9790, 16),
            UpdateGSheetTask.worksheet_districts: (156697, 17)  # +1 column for district
        }

        def my_get_dataframe(self, storage_name):
            if storage_name == UpdateGSheetTask.worksheet_hospitalizations:
                return pd.read_csv(
                    os.path.join(THIS_DIR, '../samples/Dashboard PDF Development - hospitalization.csv'))
            raise ValueError(f'Did not expect {storage_name=}')

        def my_store_dataframe(self, df: pd.DataFrame, storage_name, allow_create):
            results[storage_name] = df
            df.to_csv(os.path.join(THIS_DIR, f'../test output/test_update_run_{storage_name}.csv'))

        class OutputMock:
            @staticmethod
            def open(*args):
                return open(os.path.join(THIS_DIR, '../samples/covid19india_data-all.json'))

            @staticmethod
            def exists():
                return True

            @staticmethod
            def remove():
                return

        def my_output(self):
            return OutputMock()

        with patch.object(GSheetRepository, 'get_dataframe', new=my_get_dataframe), \
                patch.object(GSheetRepository, 'store_dataframe', new=my_store_dataframe), \
                patch.object(FetchCovid19IndiaDataTask, 'run', return_value=None), \
                patch.object(FetchCovid19IndiaDataTask, 'output', new=my_output):

            sut = UpdateGSheetTask()
            worker = luigi.worker.Worker()
            worker.add(sut)
            worker.run()

        for worksheet, expected in expected_results.items():
            expected_rows, result_rows = expected[0], results[worksheet].shape[0]
            expected_columns, result_columns = expected[1], results[worksheet].shape[1]

            # this is an integration test so rows will keep growing, columns should stay the same however
            self.assertEqual(result_rows, expected_rows, f'Expected more rows for worksheet {worksheet}')
            self.assertEqual(result_columns, expected_columns,
                             f'Number of columns does not match expectations for {worksheet}')
