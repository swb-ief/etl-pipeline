import os
import unittest
from unittest.mock import patch
import pandas as pd

import luigi

from backend.repository import AWSFileRepository
from tasks.update_dashboard_task import UpdateDashboardTask

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestUpdateDashboardTask(unittest.TestCase):

    def test_update_dashboard_full_run(self):
        """ This test will run as much of the pipeline locally this includes getting the data from the API
        Once you identify the problem write a unit test that only executes that part and mock everything else
        We don't want to overburden the covid19india.org api to much :-)
        """
        results = dict()
        expected_results = {
            UpdateDashboardTask.storage_hospitalizations: (308, 2),
            UpdateDashboardTask.storage_states: (9826, 33),
            UpdateDashboardTask.storage_districts: (157350, 34),  # +1 column for district
            UpdateDashboardTask.storage_wards: (24, 35)  # +1 column for ward
        }

        def my_get_dataframe(self, storage_name):
            if storage_name == UpdateDashboardTask.storage_hospitalizations:
                return pd.read_csv(
                    os.path.join(THIS_DIR, '../tests/samples/Dashboard PDF Development - hospitalization.csv'))
            raise ValueError(f'Did not expect {storage_name=}')

        def my_store_dataframe(self, df: pd.DataFrame, storage_name, allow_create=True, store_index=False):
            results[storage_name] = df
            df.to_csv(os.path.join(THIS_DIR, f'../tests/test output/test_update_full_run_{storage_name}.csv'))

        def my_store_exists(self, url: str):
            return url == UpdateDashboardTask.storage_hospitalizations

        with patch.object(AWSFileRepository, 'get_dataframe', new=my_get_dataframe), \
                patch.object(AWSFileRepository, 'store_dataframe', new=my_store_dataframe), \
                patch.object(AWSFileRepository, 'exists', new=my_store_exists):

            sut = UpdateDashboardTask()
            worker = luigi.worker.Worker()
            worker.add(sut)
            worker.run()

        for worksheet, expected in expected_results.items():
            expected_rows, result_rows = expected[0], results[worksheet].shape[0]
            expected_columns, result_columns = expected[1], results[worksheet].shape[1]
            # this is an integration test so rows will keep growing, columns should stay the same however
            self.assertGreaterEqual(result_rows, expected_rows, f'Expected more rows for worksheet {worksheet}')
            self.assertEqual(expected_columns, result_columns,
                             f'Number of columns does not match expectations for {worksheet}')
