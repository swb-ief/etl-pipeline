import os
import unittest
from unittest.mock import patch, Mock
import pandas as pd

import luigi

from backend.gsheet_repository import GSheetRepository
from tasks.update_gsheet_task import UpdateGSheetTask

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestUpdateGSheetTask(unittest.TestCase):

    def test_run(self):
        results = dict()
        expected_results = {
            UpdateGSheetTask.worksheet_hospitalizations: (308, 2),
            UpdateGSheetTask.worksheet_states: (9826, 16),
            UpdateGSheetTask.worksheet_districts: (157350, 17)  # +1 column for district
        }

        def my_get_dataframe(self, storage_name):
            if storage_name == UpdateGSheetTask.worksheet_hospitalizations:
                return pd.read_csv(
                    os.path.join(THIS_DIR, '../tests/samples/Dashboard PDF Development - hospitalization.csv'))
            raise ValueError(f'Did not expect {storage_name=}')

        def my_store_dataframe(self, df: pd.DataFrame, storage_name):
            results[storage_name] = df
            df.to_csv(os.path.join(THIS_DIR, f'../tests/test output/test_update_{storage_name}.csv'))

        with patch.object(GSheetRepository, 'get_dataframe', new=my_get_dataframe), \
                patch.object(GSheetRepository, 'store_dataframe', new=my_store_dataframe):
            sut = UpdateGSheetTask()
            worker = luigi.worker.Worker()
            worker.add(sut)
            worker.run()

        for worksheet, expected in expected_results.items():
            self.assertEqual(expected, results[worksheet].shape,
                             f'Expected {expected} for {worksheet} but got {results[worksheet].shape}')
