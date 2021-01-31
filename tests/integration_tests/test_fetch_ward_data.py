import os
import unittest
from unittest.mock import patch

import luigi
import pandas as pd
import pytest

from backend import GSheetRepository
from tasks.districts.DownloadFileTask import DownloadFileTask
from tasks.fetch_ward_data import FetchWardDataTask

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.parametrize('pdf_path', ['../samples/mumbai_dashboard_2020_01_02.pdf',
                                      '../samples/mumbai_dashboard_2020_01_04.pdf'])
def test_fetch_ward_data_run(pdf_path):
    results = dict()

    def my_store_dataframe(self, df: pd.DataFrame, storage_name, allow_create, store_index):
        results[storage_name] = df
        df.to_csv(os.path.join(THIS_DIR, f'../test output/test_fetch_ward_data_run_{storage_name}.csv'))

    class DownloadOutputMock:
        @staticmethod
        def open(*args):
            return open(os.path.join(THIS_DIR, pdf_path))

        @staticmethod
        def exists():
            return True

        @staticmethod
        def remove():
            return

        @property
        def path(self):
            return os.path.join(THIS_DIR, pdf_path)

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

    assert results['raw_ward_data'] is not None


def test_fetch_ward_data_run_with_sheet_data():
    # contains data from 01-Jan while the existing data is from 03 Jan so should append
    pdf_path = '../samples/mumbai_dashboard_2020_01_02.pdf'
    expected = (48, 6)

    results = dict()

    def my_store_dataframe(self, df: pd.DataFrame, storage_name, allow_create, store_index):
        results[storage_name] = df
        df.to_csv(os.path.join(THIS_DIR, f'../test output/test_fetch_ward_data_run_{storage_name}.csv'))

    class DownloadOutputMock:
        @staticmethod
        def open(*args):
            return open(os.path.join(THIS_DIR, pdf_path))

        @staticmethod
        def exists():
            return True

        @staticmethod
        def remove():
            return

        @property
        def path(self):
            return os.path.join(THIS_DIR, pdf_path)

    def my_output(self):
        return DownloadOutputMock()

    existing_data = pd.read_csv(
        os.path.join(THIS_DIR, '../samples/Covid19 Dashboard - Development  - Phase 2 - Wards.csv'),
        parse_dates=['date'])

    with patch.object(GSheetRepository, 'exists', return_value=True), \
            patch.object(GSheetRepository, 'get_dataframe', return_value=existing_data), \
            patch.object(GSheetRepository, 'store_dataframe', new=my_store_dataframe), \
            patch.object(DownloadFileTask, 'output', new=my_output):
        sut = FetchWardDataTask()
        worker = luigi.worker.Worker()
        worker.add(sut)
        worker.run()

        # cleanup
        sut.output().remove()

    assert results['raw_ward_data'] is not None
    assert results['raw_ward_data'].shape == expected


def test_fetch_ward_data_run_with_old_sheet_data():
    # pdf and the csv both have data for Jan 3 this should not lead to duplicates
    pdf_path = '../samples/mumbai_dashboard_2020_01_04.pdf'
    expected = (24, 6)

    results = dict()

    def my_store_dataframe(self, df: pd.DataFrame, storage_name, allow_create, store_index):
        results[storage_name] = df
        df.to_csv(os.path.join(THIS_DIR, f'../test output/test_fetch_ward_data_run_{storage_name}.csv'))

    class DownloadOutputMock:
        @staticmethod
        def open(*args):
            return open(os.path.join(THIS_DIR, pdf_path))

        @staticmethod
        def exists():
            return True

        @staticmethod
        def remove():
            return

        @property
        def path(self):
            return os.path.join(THIS_DIR, pdf_path)

    def my_output(self):
        return DownloadOutputMock()

    existing_data = pd.read_csv(
        os.path.join(THIS_DIR, '../samples/Covid19 Dashboard - Development  - Phase 2 - Wards.csv'),
        parse_dates=['date'])

    with patch.object(GSheetRepository, 'exists', return_value=True), \
            patch.object(GSheetRepository, 'get_dataframe', return_value=existing_data), \
            patch.object(GSheetRepository, 'store_dataframe', new=my_store_dataframe), \
            patch.object(DownloadFileTask, 'output', new=my_output):
        sut = FetchWardDataTask()
        worker = luigi.worker.Worker()
        worker.add(sut)
        worker.run()

        # cleanup
        sut.output().remove()

    assert results['raw_ward_data'] is not None
    assert results['raw_ward_data'].shape == expected
