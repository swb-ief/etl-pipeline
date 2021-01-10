import os
import unittest
from unittest.mock import patch
import pandas as pd
import luigi

from backend.repository.gsheet_repository import GSheetRepository
from tasks import FetchCovid19IndiaDataTask
from tasks.districts import DownloadFileTask
from tasks.update_dashboard_task import UpdateDashboardTask

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestUpdateGSheetTask(unittest.TestCase):

    def test_update_dashboard_run(self):
        """ This will run local parts only using files from sample etc...
        """
        results = dict()
        expected_results = {
            UpdateDashboardTask.storage_hospitalizations: (307, 2),
            UpdateDashboardTask.storage_states: (9790, 16),
            UpdateDashboardTask.storage_districts: (156697, 17)  # +1 column for district
        }

        storage_prefix = 'test_update_run_'
        expected_output_files = [storage_prefix + storage for storage in [
            UpdateDashboardTask.storage_hospitalizations,

            UpdateDashboardTask.storage_states,
            UpdateDashboardTask.storage_districts,
            UpdateDashboardTask.storage_wards,

            UpdateDashboardTask.storage_states_static,
            UpdateDashboardTask.storage_districts_static,
        ]
                                 ]

        def mock_exists(self, storage_name):
            if storage_name == 'raw_ward_data':
                return False
            return True

        def mock_get_dataframe(self, storage_name):
            if storage_name == UpdateDashboardTask.storage_hospitalizations:
                return pd.read_csv(
                    os.path.join(THIS_DIR, '../samples/Dashboard PDF Development - hospitalization.csv'))
            raise ValueError(f'Did not expect {storage_name=}')

        def mock_store_dataframe(self, df: pd.DataFrame, storage_name, allow_create, store_index=True):
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

        def mock_output(self):
            return OutputMock()

        class DownloadOutputMock:
            @staticmethod
            def open(*args):
                return open(os.path.join(THIS_DIR, '../samples/mumbai_dashboard_2020_01_02.pdf'))

            @staticmethod
            def exists():
                return True

            @staticmethod
            def remove():
                return

            @property
            def path(self):
                return os.path.join(THIS_DIR, '../samples/mumbai_dashboard_2020_01_02.pdf')

        def mock_download_task_output(self):
            return DownloadOutputMock()

        with patch.object(GSheetRepository, 'get_dataframe', new=mock_get_dataframe), \
                patch.object(GSheetRepository, 'store_dataframe', new=mock_store_dataframe), \
                patch.object(GSheetRepository, 'exists', new=mock_exists), \
                patch.object(FetchCovid19IndiaDataTask, 'output', new=mock_output), \
                patch.object(DownloadFileTask, 'output', new=mock_download_task_output):

            sut = UpdateDashboardTask()
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

        for file in expected_output_files:
            path = os.path.join(THIS_DIR, f'../test output/{file}.csv')
            self.assertTrue(os.path.exists(path), f'Expected {path} to exist')
